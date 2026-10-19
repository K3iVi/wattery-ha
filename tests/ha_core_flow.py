"""Volba připojení v config flow (A5) uvnitř skutečného Home Assistant core (bez UI) s falešným serverem wattery.
  <venv s balíkem homeassistant>/python tests/ha_core_flow.py
Zkouší: menu (Solarman / GX / entity jen když je podporovaná integrace), Victron GX přímo proti simulátoru GX
(tests/gx_sim.py: 8883 nedostupné → 1883, špatné heslo, portal ID, heslo nejde na server, caps bez modbus, čtení
mqtt do sync, nové spárování), jen čtení z entit integrace victron_gx (zástupná integrace v testu: identita
z unique_id, jen platné entity sensor/number, bez přepínače řízení, víc GX v jedné integraci → chyba), jen čtení
z entit solax_modbus (identitu vrátí server, jen entity s klíči z plánu, prefix hubu, opakování → konec před serverem),
Solax přes LAN dongle Modbus TCP (tests/solax_dongle_sim.py: obsazeno, jiná jednotka, SN, čtení, souběh se solax_modbus).
Portal ID a hodnoty jsou smyšlené (veřejné repo)."""
import asyncio
import json
import os
import shutil
import socket
import sys
import tempfile
import time
from types import MappingProxyType, SimpleNamespace

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, 'tests'))
import gx_sim  # noqa: E402
import solax_dongle_sim  # noqa: E402

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


async def modbus_tcp(hass, flow, srv, cf):
    """Solax přes LAN dongle (Modbus TCP) proti simulátoru donglu: výchozí hodnoty formuláře, obsazený dongle,
    špatná jednotka (mlčí → časový limit), spárování s SN ze střídače, čtení bloků přes dongle do sync, druhé
    přidání téže adresy, souběh se solax_modbus na stejné adrese (nepřipojit se), unload zavře spojení."""
    sim = await solax_dongle_sim.DongleSim().start()
    n0 = len(srv['pairs'])

    async def form():
        r = await flow.async_init('wattery', context={'source': 'user'})
        return await flow.async_configure(r['flow_id'], {'next_step_id': 'modbus_tcp'})

    def inp(**kw):
        return dict({'host': '127.0.0.1', 'port': sim.port, 'unit': 1, 'framing': 'tcp', 'code': 'abcd-efgh'}, **kw)

    res = await form()
    dflt = {str(k): k.default() for k in res['data_schema'].schema if callable(getattr(k, 'default', None))}
    check('Modbus TCP: formulář, výchozí port 502, jednotka 1, rámování tcp', res['type'] == 'form'
          and res['step_id'] == 'modbus_tcp' and dflt.get('port') == 502 and dflt.get('unit') == 1
          and dflt.get('framing') == 'tcp', (res, dflt))

    _r, w = await asyncio.open_connection('127.0.0.1', sim.port)     # dongle drží „aplikace výrobce“
    await until(lambda: sim._active == 1, 2)
    bad = await flow.async_configure(res['flow_id'], inp())
    check('Modbus TCP: dongle obsazený jiným klientem → mb_busy, nic se nepárovalo', bad['type'] == 'form'
          and bad['errors'] == {'base': 'mb_busy'} and len(srv['pairs']) == n0, bad)
    w.close()
    await until(lambda: sim._active == 0, 2)

    bad = await flow.async_configure(res['flow_id'], inp(unit=7))
    check('Modbus TCP: jiná jednotka (sběrnice mlčí) → mb_timeout', bad['type'] == 'form'
          and bad['errors'] == {'base': 'mb_timeout'} and len(srv['pairs']) == n0, bad)
    bad = await flow.async_configure(res['flow_id'], inp(port=free_port()))
    check('Modbus TCP: nic neposlouchá → mb_connect', bad['type'] == 'form'
          and bad['errors'] == {'base': 'mb_connect'}, bad)

    ok = await flow.async_configure(res['flow_id'], inp())
    d = ok.get('data') or {}
    pb = srv['pairs'][-1] if len(srv['pairs']) > n0 else {}
    check('Modbus TCP: spárováno, SN ze střídače (holding 0), data host/port/unit/framing',
          ok['type'] == 'create_entry' and d.get('conn') == 'modbus_tcp' and d.get('sn') == solax_dongle_sim.SN
          and ok['title'] == f'wattery – střídač {solax_dongle_sim.SN}' and d.get('host') == '127.0.0.1'
          and d.get('port') == sim.port and d.get('unit') == 1 and d.get('framing') == 'tcp', ok)
    check('Modbus TCP: párování sn ze střídače, caps modbus bez integrací, adresa na server nejde',
          pb.get('sn') == solax_dongle_sim.SN and pb.get('caps', {}).get('transports') == ['modbus']
          and 'integrations' not in pb.get('caps', {}) and '127.0.0.1' not in json.dumps(pb), pb)
    await hass.async_block_till_done()
    entry = next(e for e in hass.config_entries.async_entries('wattery') if e.data.get('conn') == 'modbus_tcp')
    tok = entry.data['token']

    def mb_reads():
        return [r for t, body in srv['bodies'] if t == tok for r in body['reads'] if r.get('fc') == 4]
    await until(mb_reads, 10)
    rd = (mb_reads() or [{}])[-1]
    check('Modbus TCP: čtení bloku input 0×16 přes dongle do sync (napětí 0x02 = 2400), backend ha-modbus-tcp',
          rd.get('addr') == 0 and len(rd.get('values') or []) == 16 and rd['values'][2] == 2400
          and any(b['client']['backend'] == 'ha-modbus-tcp' for t, b in srv['bodies'] if t == tok), rd)

    n1, c1 = len(srv['pairs']), sim.connections
    dup = await form()
    dup = await flow.async_configure(dup['flow_id'], inp())
    check('Modbus TCP podruhé na stejný dongle → already_configured bez spojení a bez serveru',
          dup.get('reason') == 'already_configured' and len(srv['pairs']) == n1 and sim.connections == c1, dup)

    ext = ConfigEntry(data={'name': 'Dongle', 'interface': 'tcp', 'host': '127.0.0.1', 'port': 1502},
                      domain='solax_modbus', title='Dongle', source='user', version=1, minor_version=1, options={},
                      unique_id=None, discovery_keys=MappingProxyType({}), subentries_data=None)
    await hass.config_entries.async_add(ext)
    bad = await form()
    bad = await flow.async_configure(bad['flow_id'], inp(port=1502))
    check('Modbus TCP: solax_modbus na stejné adrese → mb_conflict_same_address se jménem a adresou, bez spojení',
          bad['type'] == 'form' and bad['errors'] == {'base': 'mb_conflict_same_address'}
          and bad['description_placeholders'] == {'name': 'Dongle', 'host': '127.0.0.1', 'port': '1502'}
          and sim.connections == c1, bad)
    await hass.config_entries.async_remove(ext.entry_id)
    ser = ConfigEntry(data={'name': 'USB', 'interface': 'serial'}, domain='solax_modbus', title='USB', source='user',
                      version=1, minor_version=1, options={}, unique_id=None, discovery_keys=MappingProxyType({}),
                      subentries_data=None)
    await hass.config_entries.async_add(ser)
    bad = await form()
    bad = await flow.async_configure(bad['flow_id'], inp(port=1502))
    check('Modbus TCP: solax_modbus sériově → mb_conflict_serial, bez spojení',
          bad.get('errors') == {'base': 'mb_conflict_serial'} and sim.connections == c1, bad)
    await hass.config_entries.async_remove(ser.entry_id)
    tr = {lang: json.load(open(os.path.join(REPO, 'custom_components', 'wattery', 'translations', f'{lang}.json'),
                               encoding='utf-8'))['config']['error'] for lang in ('cs', 'en')}
    check('překlady: chyby konfliktu pro všechny kódy conflict_info v cs i en se {name}',
          all(f'mb_conflict_{c}' in tr[lang] and '{name}' in tr[lang][f'mb_conflict_{c}']
              for lang in tr for c in ('same_address', 'core_modbus', 'serial')), tr)

    check('Modbus TCP: unload bez chyby', await hass.config_entries.async_unload(entry.entry_id))
    check('Modbus TCP: po unload spojení na dongle zavřené', await until(lambda: sim._active == 0, 3), sim._active)
    await sim.stop()


async def main():
    cfg = tempfile.mkdtemp(prefix='ha-flow-')
    shutil.copytree(os.path.join(REPO, 'custom_components'), os.path.join(cfg, 'custom_components'))
    stub_integration(cfg, 'victron_gx')
    stub_integration(cfg, 'solax_modbus')
    hass = HomeAssistant(cfg)
    hass.config.skip_pip = True
    hass.config.components.add('http')    # kalendář plánu závisí na http (ve skutečném HA běží vždy) — jen atrapa
    hass.http = SimpleNamespace(register_view=lambda *a, **k: None)
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
             'ha_entity': [{'type': 'ha_entity', 'name': 'victron_gx', 'integration': 'victron_gx', 'everyS': 1}],
             'solax_modbus': [{'type': 'ha_entity', 'name': 'solax_modbus', 'integration': 'solax_modbus',
                               'keys': ['battery_capacity', 'pv_power_1', 'run_mode'], 'everyS': 1}],
             'modbus': [{'type': 'modbus', 'name': 'live', 'fc': 4, 'addr': 0, 'count': 16, 'everyS': 5}]}

    async def post(path, body, token=None):
        if path == cedge.PAIR_PATH:
            srv['pairs'].append(body)
            tr, integ = body['caps']['transports'], body['caps'].get('integrations') or []
            plan = plans['solax_modbus'] if 'solax_modbus' in integ else plans.get(tr[0], [])
            return 200, {'ok': True, 'token': f'TOK{len(srv["pairs"])}', 'clientId': len(srv['pairs']),
                         'stationId': 1001, 'readPlan': plan, 'nextPollS': 10, 'sn': body['sn'] or 'H3TEST0000001'}
        srv['bodies'].append((token, body))
        return 200, {'ok': True, 'nextPollS': 0.3, 'accepted': len(body['reads']), 'serverTime': time.time()}
    api.make_post = lambda h, base_url=None: post

    flow = hass.config_entries.flow
    res = await flow.async_init('wattery', context={'source': 'user'})
    check('menu bez podporované integrace: Solarman, GX, Modbus TCP', res['type'] == 'menu'
          and res['menu_options'] == ['solarman', 'gx', 'modbus_tcp'], res)
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
    check('GX: senzor stavu + plánu, kalendář plánu, přepínač řízení',
          sorted(e.domain for e in ents) == ['calendar', 'sensor', 'sensor', 'switch'], ents)
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
          and res['menu_options'] == ['solarman', 'gx', 'entities', 'modbus_tcp'], res)
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
    check('entity: senzory stavu a plánu + kalendář plánu, bez přepínače řízení',
          sorted(e.domain for e in ents) == ['calendar', 'sensor', 'sensor'], ents)
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

    # ── jen čtení z entit solax_modbus (G4: SN v entitách není → identitu vrátí server) ──
    sx = ConfigEntry(data={'name': 'SolaX'}, domain='solax_modbus', title='SolaX', source='user', version=1,
                     minor_version=1, options={}, unique_id=None, discovery_keys=MappingProxyType({}),
                     subentries_data=None)
    await hass.config_entries.async_add(sx)
    for key, val, unit in (('battery_capacity', '81', '%'), ('pv_power_1', '1200', 'W'),
                           ('inverter_temperature', '40', '°C'), ('run_mode', 'Normal Mode', None)):
        reg.async_get_or_create('sensor', 'solax_modbus', f'SolaX_{key}', config_entry=sx,
                                suggested_object_id=f'solax_{key}')
        hass.states.async_set(f'sensor.solax_{key}', val, {'unit_of_measurement': unit} if unit else {})
    n0 = len(srv['pairs'])
    res = await flow.async_init('wattery', context={'source': 'user'})
    res = await flow.async_configure(res['flow_id'], {'next_step_id': 'entities'})
    ok = await flow.async_configure(res['flow_id'], {'source_entry': sx.entry_id, 'code': 'abcd-efgh'})
    pb = srv['pairs'][-1]
    check('Solax z entit: párování bez identity (sn ""), SN ze serveru, caps ha_entity + solax_modbus',
          ok['type'] == 'create_entry' and ok['data']['sn'] == 'H3TEST0000001' and pb['sn'] == ''
          and pb['caps'].get('integrations') == ['solax_modbus'] and len(srv['pairs']) == n0 + 1, (ok, pb))
    await hass.async_block_till_done()
    sx_entry = next(e for e in hass.config_entries.async_entries('wattery') if e.data.get('source_entry') == sx.entry_id)
    tok = sx_entry.data['token']
    await until(ha_reads, 10)
    rd = (ha_reads() or [{}])[-1]
    check('Solax z entit: jen entity s klíči z plánu (teplota ne), prefix hubu',
          set(rd.get('values') or {}) == {'sensor.solax_battery_capacity', 'sensor.solax_pv_power_1',
                                          'sensor.solax_run_mode'} and rd.get('prefix') == 'SolaX', rd)
    dup = await flow.async_init('wattery', context={'source': 'user'})
    dup = await flow.async_configure(dup['flow_id'], {'next_step_id': 'entities'})
    dup = await flow.async_configure(dup['flow_id'], {'source_entry': sx.entry_id, 'code': 'abcd-efgh'})
    check('Solax z entit podruhé → already_configured dřív, než se kód pošle serveru',
          dup.get('reason') == 'already_configured' and len(srv['pairs']) == n0 + 1, dup)
    check('Solax z entit: unload bez chyby', await hass.config_entries.async_unload(sx_entry.entry_id))
    await modbus_tcp(hass, flow, srv, cf)
    await hass.async_stop()
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
