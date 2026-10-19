"""Volba připojení v config flow (A5) uvnitř skutečného Home Assistant core (bez UI) s falešným serverem wattery.
  <venv s balíkem homeassistant>/python tests/ha_core_flow.py
Zkouší: menu (Solarman / GX / entity jen když je podporovaná integrace), Victron GX přímo proti simulátoru GX
(tests/gx_sim.py: 8883 nedostupné → 1883, špatné heslo, portal ID, heslo nejde na server, caps bez modbus, čtení
mqtt do sync, nové spárování), jen čtení z entit integrace victron_gx (zástupná integrace v testu: identita
z unique_id, jen platné entity sensor/number, bez přepínače řízení, víc GX v jedné integraci → chyba).
Portal ID a hodnoty jsou smyšlené (veřejné repo)."""
import asyncio
import json
import os
import shutil
import socket
import sys
import tempfile
import time
from types import MappingProxyType

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, 'tests'))
import gx_sim  # noqa: E402

from homeassistant import loader  # noqa: E402
from homeassistant.config_entries import ConfigEntries, ConfigEntry  # noqa: E402
from homeassistant.core import HomeAssistant  # noqa: E402
from homeassistant.helpers import device_registry as dr, entity_registry as er, restore_state  # noqa: E402

FAILS = []
PORTAL2 = 'c0ffee000002'
STUB_INIT = '''async def async_setup_entry(hass, entry):
    return True


async def async_unload_entry(hass, entry):
    return True
'''


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


async def until(cond, timeout=10):
    for _ in range(int(timeout / 0.05)):
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


def free_port() -> int:
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    p = s.getsockname()[1]
    s.close()
    return p


def stub_integration(cfg, domain):
    d = os.path.join(cfg, 'custom_components', domain)
    os.makedirs(d)
    with open(os.path.join(d, 'manifest.json'), 'w') as f:
        json.dump({'domain': domain, 'name': domain, 'version': '0.0.1', 'config_flow': False,
                   'codeowners': [], 'documentation': 'https://example.invalid', 'iot_class': 'local_push'}, f)
    with open(os.path.join(d, '__init__.py'), 'w') as f:
        f.write(STUB_INIT)


async def gx_tls(cf):
    """Zabezpečený profil GX (TLS 8883, certifikát podepsaný sám sebou): otisk se uloží, jiný otisk → gx_cert bez
    zkoušení 1883."""
    import ssl
    import subprocess
    exe = shutil.which('openssl')
    if not exe:
        print('SKIP TLS (není openssl)')
        return
    d = tempfile.mkdtemp(prefix='gx-tls-')
    key, crt = os.path.join(d, 'k.pem'), os.path.join(d, 'c.pem')
    subprocess.run([exe, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', crt, '-days', '2',
                    '-subj', '/CN=venus.local'], check=True, capture_output=True)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(crt, key)
    bt = await gx_sim.GxBroker(password='tajne', ssl=ctx).start()
    bp = await gx_sim.GxBroker(password='tajne').start()
    cf.PORT_TLS, cf.PORT_PLAIN = bt.port, bp.port
    try:
        found = await cf.gx_probe('127.0.0.1', 'tajne')
        check('GX TLS: spojení přes 8883, otisk certifikátu uložen', found['tls'] is True and found['port'] == bt.port
              and len(found['cert_sha256'] or '') == 64 and found['sn'] == gx_sim.PORTAL, found)
        try:
            await cf.gx_probe('127.0.0.1', 'tajne', cert='0' * 64)
            check('GX TLS: jiný certifikát → odmítnuto, 1883 se nezkouší', False)
        except cf.GxError as e:
            check('GX TLS: jiný certifikát → odmítnuto, 1883 se nezkouší', 'certifikát' in str(e)
                  and bp.connects == 0, (e, bp.connects))
    finally:
        await bt.stop()
        await bp.stop()


async def main():
    cfg = tempfile.mkdtemp(prefix='ha-flow-')
    shutil.copytree(os.path.join(REPO, 'custom_components'), os.path.join(cfg, 'custom_components'))
    stub_integration(cfg, 'victron_gx')
    hass = HomeAssistant(cfg)
    hass.config.skip_pip = True
    loader.async_setup(hass)
    await dr.async_load(hass)
    await er.async_load(hass)
    await restore_state.async_load(hass)
    hass.config_entries = ConfigEntries(hass, {})
    await hass.config_entries.async_initialize()
    await hass.async_start()

    import custom_components.wattery.api as api
    import custom_components.wattery.config_flow as cf
    import custom_components.wattery.edge as cedge
    cedge.POLL_MIN_S, cedge.RETRY_FAILED_READ_S = 0.2, 0.3          # rychlé kolo jen v testu
    srv = {'pairs': [], 'bodies': []}
    plans = {'mqtt': [{'type': 'mqtt', 'name': 'live', 'paths': ['system/0/Dc/Battery/Soc'], 'everyS': 1}],
             'ha_entity': [{'type': 'ha_entity', 'name': 'victron_gx', 'integration': 'victron_gx', 'everyS': 1}]}

    async def post(path, body, token=None):
        if path == cedge.PAIR_PATH:
            srv['pairs'].append(body)
            tr = body['caps']['transports']
            return 200, {'ok': True, 'token': f'TOK{len(srv["pairs"])}', 'clientId': len(srv['pairs']),
                         'stationId': 1001, 'readPlan': plans.get(tr[0], []), 'nextPollS': 10}
        srv['bodies'].append((token, body))
        return 200, {'ok': True, 'nextPollS': 0.3, 'accepted': len(body['reads']), 'serverTime': time.time()}
    api.make_post = lambda h, base_url=None: post

    flow = hass.config_entries.flow
    res = await flow.async_init('wattery', context={'source': 'user'})
    check('menu bez podporované integrace: Solarman, GX', res['type'] == 'menu'
          and res['menu_options'] == ['solarman', 'gx'], res)
    res = await flow.async_configure(res['flow_id'], {'next_step_id': 'solarman'})
    check('Deye bez ha-solarman → abort no_solarman', res['type'] == 'abort' and res['reason'] == 'no_solarman', res)

    # ── Victron GX přímo (simulátor bez TLS; 8883 nahrazen zavřeným portem) ──
    b = await gx_sim.GxBroker(password='tajne').start()
    cf.PORT_TLS, cf.PORT_PLAIN = free_port(), b.port
    res = await flow.async_init('wattery', context={'source': 'user'})
    res = await flow.async_configure(res['flow_id'], {'next_step_id': 'gx'})
    check('GX: formulář s výchozí adresou venus.local', res['type'] == 'form' and res['step_id'] == 'gx'
          and any(str(k) == 'host' and k.default() == 'venus.local' for k in res['data_schema'].schema), res)
    bad = await flow.async_configure(res['flow_id'], {'host': '127.0.0.1', 'password': 'spatne', 'code': 'abcd-efgh'})
    check('GX: špatné heslo → gx_auth, nic se nepárovalo', bad['type'] == 'form'
          and bad['errors'] == {'base': 'gx_auth'} and not srv['pairs'], bad)
    ok = await flow.async_configure(res['flow_id'], {'host': '127.0.0.1', 'password': 'tajne', 'code': 'abcd-efgh'})
    d = ok.get('data') or {}
    check('GX: spárováno (TLS nedostupné → 1883), identita = portal ID, otisk jen u TLS',
          ok['type'] == 'create_entry' and d.get('conn') == 'gx_mqtt' and d.get('sn') == gx_sim.PORTAL
          and d.get('port') == b.port and d.get('tls') is False and d.get('cert_sha256') is None
          and d.get('password') == 'tajne', ok)
    pb = srv['pairs'][-1] if srv['pairs'] else {}
    check('GX: párování kind ha, sn = portal, caps jen mqtt (bez modbus), heslo ani adresa na server nejdou',
          pb.get('kind') == 'ha' and pb.get('sn') == gx_sim.PORTAL and pb['caps']['transports'] == ['mqtt']
          and 'tajne' not in json.dumps(pb) and '127.0.0.1' not in json.dumps(pb), pb)
    await hass.async_block_till_done()
    gx_entry = next(e for e in hass.config_entries.async_entries('wattery') if e.data.get('conn') == 'gx_mqtt')
    ents = er.async_entries_for_config_entry(er.async_get(hass), gx_entry.entry_id)
    check('GX: senzor stavu + přepínač řízení', sorted(e.domain for e in ents) == ['sensor', 'switch'], ents)
    runner = gx_entry.runtime_data.runner

    def mqtt_reads(tok):
        return [r for t, body in srv['bodies'] if t == tok for r in body['reads'] if r.get('type') == 'mqtt']
    got = await until(lambda: mqtt_reads('TOK1'), 15)
    rd = (mqtt_reads('TOK1') or [{}])[-1]
    check('GX: klient online, čtení mqtt s portal a SOC ze simulátoru, caps bez modbus',
          got and runner.status == 'online' and rd.get('dev') == gx_sim.PORTAL
          and rd.get('values') == {'system/0/Dc/Battery/Soc': 64.5}
          and srv['bodies'][-1][1]['client']['caps']['transports'] == ['mqtt']
          and srv['bodies'][-1][1]['client']['backend'] == 'ha-gx-mqtt', (runner.status, runner.last_error, rd))
    dup = await flow.async_init('wattery', context={'source': 'user'})
    dup = await flow.async_configure(dup['flow_id'], {'next_step_id': 'gx'})
    dup = await flow.async_configure(dup['flow_id'], {'host': '127.0.0.1', 'password': 'tajne', 'code': 'abcd-efgh'})
    check('GX podruhé → already_configured dřív, než se kód pošle serveru',
          dup['type'] == 'abort' and dup['reason'] == 'already_configured' and len(srv['pairs']) == 1, dup)

    gx_entry.async_start_reauth(hass)
    await hass.async_block_till_done()
    rf = [f for f in flow.async_progress() if f['context'].get('source') == 'reauth']
    rr = await flow.async_configure(rf[0]['flow_id'], {'code': 'wxyz-2345'}) if rf else {}
    await hass.async_block_till_done()
    gx_entry = hass.config_entries.async_get_entry(gx_entry.entry_id)
    check('GX: nové spárování přes uložené spojení (stejný portal) → nový token, nastavení beze změny',
          rr.get('reason') == 'reauth_successful' and gx_entry.data['token'] == f'TOK{len(srv["pairs"])}' != 'TOK1'
          and gx_entry.data['conn'] == 'gx_mqtt' and gx_entry.data['port'] == b.port
          and gx_entry.data['password'] == 'tajne', (rr, gx_entry.data))
    check('GX: unload bez chyby', await hass.config_entries.async_unload(gx_entry.entry_id))
    await b.stop()
    await gx_tls(cf)

    # ── jen čtení z entit integrace victron_gx ──
    reg = er.async_get(hass)

    def source(title):
        return ConfigEntry(data={}, domain='victron_gx', title=title, source='user', version=1, minor_version=1,
                           options={}, unique_id=None, discovery_keys=MappingProxyType({}), subentries_data=None)
    src = source('Cerbo')
    await hass.config_entries.async_add(src)
    reg.async_get_or_create('sensor', 'victron_gx', f'{PORTAL2}_battery_512_battery_soc', config_entry=src,
                            suggested_object_id='victron_battery_soc')
    reg.async_get_or_create('sensor', 'victron_gx', f'{PORTAL2}_system_0_system_dc_battery_power', config_entry=src,
                            suggested_object_id='victron_battery_power')
    reg.async_get_or_create('sensor', 'victron_gx', f'{PORTAL2}_grid_30_grid_energy_forward', config_entry=src,
                            suggested_object_id='victron_grid_forward')
    reg.async_get_or_create('switch', 'victron_gx', f'{PORTAL2}_settings_0_ess_switch', config_entry=src,
                            suggested_object_id='victron_ess')
    hass.states.async_set('sensor.victron_battery_soc', '55.5', {'unit_of_measurement': '%'})
    hass.states.async_set('sensor.victron_battery_power', 'unavailable', {'unit_of_measurement': 'W'})
    hass.states.async_set('sensor.victron_grid_forward', '12.25', {'unit_of_measurement': 'kWh'})
    hass.states.async_set('switch.victron_ess', 'on', {})
    src2 = source('Dva GX')
    await hass.config_entries.async_add(src2)
    for p in ('c0ffee000003', 'c0ffee000004'):
        reg.async_get_or_create('sensor', 'victron_gx', f'{p}_battery_512_battery_soc', config_entry=src2)

    res = await flow.async_init('wattery', context={'source': 'user'})
    check('menu s integrací victron_gx nabízí i čtení z entit', res['type'] == 'menu'
          and res['menu_options'] == ['solarman', 'gx', 'entities'], res)
    res = await flow.async_configure(res['flow_id'], {'next_step_id': 'entities'})
    sel = next((k for k in res.get('data_schema', {}).schema if str(k) == 'source_entry'), None)
    opts = res['data_schema'].schema[sel].config['options'] if sel else []
    check('entity: výběr položky integrace (název + doména)', res['type'] == 'form'
          and {o['value'] for o in opts} == {src.entry_id, src2.entry_id}
          and any(o['label'] == 'Cerbo (victron_gx)' for o in opts), opts)
    bad = await flow.async_configure(res['flow_id'], {'source_entry': src2.entry_id, 'code': 'abcd-efgh'})
    check('entity: dva GX v jedné integraci → no_identity, nic se nepárovalo', bad['type'] == 'form'
          and bad['errors'] == {'base': 'no_identity'} and len(srv['pairs']) == 2, bad)
    ok = await flow.async_configure(res['flow_id'], {'source_entry': src.entry_id, 'code': 'abcd-efgh'})
    d = ok.get('data') or {}
    pb = srv['pairs'][-1]
    check('entity: spárováno, identita = portal z unique_id, caps ha_entity + victron_gx (bez modbus)',
          ok['type'] == 'create_entry' and d.get('conn') == 'ha_entities' and d.get('sn') == PORTAL2
          and d.get('source_entry') == src.entry_id and pb['sn'] == PORTAL2
          and pb['caps']['transports'] == ['ha_entity'] and pb['caps'].get('integrations') == ['victron_gx'], (ok, pb))
    await hass.async_block_till_done()
    ent_entry = next(e for e in hass.config_entries.async_entries('wattery') if e.data.get('conn') == 'ha_entities')
    ents = er.async_entries_for_config_entry(reg, ent_entry.entry_id)
    check('entity: jen senzor stavu, bez přepínače řízení', [e.domain for e in ents] == ['sensor'], ents)
    tok = ent_entry.data['token']

    def ha_reads():
        return [r for t, body in srv['bodies'] if t == tok for r in body['reads'] if r.get('type') == 'ha_entity']
    await until(ha_reads, 10)
    rd = (ha_reads() or [{}])[-1]
    check('entity: do sync jen platné entity sensor (unavailable a switch ne) se state, unit, uid',
          rd.get('name') == 'victron_gx' and rd.get('values') == {
              'sensor.victron_battery_soc': {'state': '55.5', 'unit': '%', 'uid': f'{PORTAL2}_battery_512_battery_soc'},
              'sensor.victron_grid_forward': {'state': '12.25', 'unit': 'kWh',
                                              'uid': f'{PORTAL2}_grid_30_grid_energy_forward'}}, rd)
    check('entity: unload bez chyby', await hass.config_entries.async_unload(ent_entry.entry_id))
    await hass.async_stop()
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
