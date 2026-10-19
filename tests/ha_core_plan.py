"""Plán baterie jako data (0.6.0, A9) uvnitř skutečného Home Assistant core (bez UI a bez serveru):
  <venv s balíkem homeassistant>/python tests/ha_core_plan.py
Falešný server posílá `planId` / `plan` jako deye/plan_neutral.py. Zkouší: senzor „Plán baterie“ (enum, akce teď,
atributy until / power_w / target_soc / ceny / next), kalendář (události bez samospotřeby, popis pro automatizace,
český text), služba wattery.get_plan (odpověď po čtvrthodinách, neznámé připojení → chyba), obnova plánu po reloadu
(HA Store), planId None → nedostupné, konec platnosti → nedostupné časovačem bez dalšího sync."""
import asyncio
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, 'tests'))

from homeassistant import loader  # noqa: E402
from homeassistant.config_entries import ConfigEntries  # noqa: E402
from homeassistant.core import HomeAssistant, SupportsResponse  # noqa: E402
from homeassistant.exceptions import ServiceValidationError  # noqa: E402
from homeassistant.helpers import device_registry as dr, entity_registry as er, restore_state  # noqa: E402

from test_edge import mk_battery_plan  # noqa: E402

FAILS = []
SN = '2400000001'
SN_REGS = [(ord(SN[2 * i]) << 8) | ord(SN[2 * i + 1]) for i in range(5)]
READ_PLAN = [{'name': 'sn', 'type': 'modbus', 'fc': 3, 'addr': 3, 'count': 5, 'everyS': 3600}]
CET = timezone(timedelta(hours=1))


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


async def main():
    cfg = tempfile.mkdtemp(prefix='ha-plan-')
    shutil.copytree(os.path.join(REPO, 'custom_components'), os.path.join(cfg, 'custom_components'))
    hass = HomeAssistant(cfg)
    hass.config.skip_pip = True
    hass.config.language = 'cs'
    # kalendář závisí na http (ve skutečném HA běží vždy) — tady bez auth, jen atrapa pro registraci view
    hass.config.components.add('http')
    hass.http = SimpleNamespace(register_view=lambda *a, **k: None)
    loader.async_setup(hass)
    await dr.async_load(hass)
    await er.async_load(hass)
    await restore_state.async_load(hass)
    hass.config_entries = ConfigEntries(hass, {})
    await hass.config_entries.async_initialize()
    await hass.async_start()

    regs = {3 + i: v for i, v in enumerate(SN_REGS)}

    async def fake_read(call):
        a, n = int(call.data['address']), int(call.data['count'])
        return {a + i: regs.get(a + i, 0) for i in range(n)}
    for svc in ('read_holding_registers', 'read_input_registers'):
        hass.services.async_register('solarman', svc, fake_read, supports_response=SupportsResponse.OPTIONAL)

    import custom_components.wattery.api as api
    import custom_components.wattery.edge as cedge
    cedge.POLL_MIN_S = 0.2
    now = time.time()
    t0 = int(now // 900 * 900)
    pat = ['charge_grid'] * 4 + ['hold'] * 4 + ['discharge_grid'] * 4 + ['self_use'] * 4 + ['no_export'] * 2
    plan = mk_battery_plan(t0, pat, pid='p#ha1', tz=CET)
    srv = {'plan': plan, 'id': 'p#ha1', 'poll': 0.3, 'bodies': []}

    async def post(path, body, token=None):
        if path == cedge.PAIR_PATH:
            return 200, {'ok': True, 'token': 'TOK', 'clientId': 7, 'stationId': 1001, 'readPlan': READ_PLAN,
                         'nextPollS': 10}
        srv['bodies'].append(body)
        out = {'ok': True, 'nextPollS': srv['poll'], 'control': 'on', 'commands': [], 'accepted': len(body['reads']),
               'serverTime': time.time()}
        if srv['id'] is not False:             # False = server bez vlastnosti plan (klíč vůbec nepošle)
            out['planId'] = srv['id']
        if srv['id'] and body['client'].get('planId') != srv['id']:
            out['plan'] = srv['plan']
        return 200, out
    api.make_post = lambda h, base_url=None: post

    res = await hass.config_entries.flow.async_init('wattery', context={'source': 'user'})
    res = await hass.config_entries.flow.async_configure(res['flow_id'], {'next_step_id': 'solarman'})
    ok = await hass.config_entries.flow.async_configure(res['flow_id'], {'code': 'abcd-efgh', 'device': 'dev1'})
    check('spárováno', ok['type'] == 'create_entry', ok)
    await hass.async_block_till_done()
    entry = hass.config_entries.async_entries('wattery')[0]
    ents = er.async_entries_for_config_entry(er.async_get(hass), entry.entry_id)
    sens = next((e for e in ents if e.unique_id == f'{SN}_plan'), None)
    cal = next((e for e in ents if e.domain == 'calendar'), None)
    check('entity: senzor plánu a kalendář na zařízení wattery', sens and cal and sens.device_id == cal.device_id
          and cal.unique_id == f'{SN}_plan_calendar', [(e.entity_id, e.unique_id) for e in ents])
    runner = entry.runtime_data.runner
    check('caps hlásí vlastnost plan, plán přijat', await until(lambda: runner.battery_plan is not None)
          and 'plan' in srv['bodies'][0]['client']['caps']['features'], srv['bodies'][:1])
    await until(lambda: hass.states.get(sens.entity_id).state == 'charge_grid')
    st = hass.states.get(sens.entity_id)
    a = st.attributes
    check('senzor: akce teď = nabíjení ze sítě, enum, atributy', st.state == 'charge_grid'
          and a.get('options') == list(cedge.PLAN_ACTIONS) and a.get('power_w') == 5000 and a.get('target_soc') == 53.0
          and a.get('reserve_soc') == 15.0 and a.get('price_buy') == 3.1 and a.get('currency') == 'CZK'
          and a.get('until') == plan['windows'][0]['to'], dict(a))
    check('senzor: next = další okna (stát, prodej, samospotřeba, bez přetoku)',
          [x['action'] for x in a.get('next') or []] == ['hold', 'discharge_grid', 'self_use', 'no_export']
          and a['next'][1]['power_w'] == 3000, a.get('next'))
    cst = hass.states.get(cal.entity_id)
    check('kalendář: probíhá nabíjení (cs text s výkonem a SOC)', cst.state == 'on'
          and cst.attributes.get('message') == 'Nabíjení ze sítě 5 kW → 53 %', dict(cst.attributes))
    ev = await hass.services.async_call('calendar', 'get_events', {
        'entity_id': cal.entity_id, 'start_date_time': datetime.fromtimestamp(t0 - 3600, timezone.utc),
        'end_date_time': datetime.fromtimestamp(t0 + 6 * 3600, timezone.utc)}, blocking=True, return_response=True)
    evs = ev[cal.entity_id]['events']
    check('kalendář: 4 události bez samospotřeby, popis s action / power_w / target_soc',
          [e['summary'] for e in evs] == ['Nabíjení ze sítě 5 kW → 53 %', 'Podržet baterii (spotřeba ze sítě)',
                                          'Prodej z baterie 3 kW → 61 %', 'Bez přetoku do sítě (záporná cena)']
          and 'action: discharge_grid' in evs[2]['description'] and 'power_w: 3000' in evs[2]['description'],
          evs)
    resp = await hass.services.async_call('wattery', 'get_plan', {}, blocking=True, return_response=True)
    pl = (resp.get('plans') or [{}])[0]
    check('služba get_plan: plán po čtvrthodinách s ISO časy',
          pl.get('sn') == SN and pl['plan']['id'] == 'p#ha1' and len(pl['plan']['slots']) == len(pat)
          and pl['plan']['slots'][8]['action'] == 'discharge_grid' and pl['plan']['windows'][0]['power_w'] == 5000,
          resp)
    one = await hass.services.async_call('wattery', 'get_plan', {'config_entry_id': entry.entry_id}, blocking=True,
                                         return_response=True)
    try:
        await hass.services.async_call('wattery', 'get_plan', {'config_entry_id': 'neni'}, blocking=True,
                                       return_response=True)
        bad = None
    except ServiceValidationError as e:
        bad = e
    check('get_plan: jedno připojení podle config_entry_id, neznámé → ServiceValidationError',
          len(one['plans']) == 1 and bad is not None and bad.translation_key == 'entry_not_loaded', (one, bad))

    await asyncio.sleep(1.5)                   # Store ukládá se zpožděním 1 s
    srv['id'] = False                          # po reloadu server dočasně bez vlastnosti plan → plán jen z Store
    srv['plan'] = None
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    entry = hass.config_entries.async_entries('wattery')[0]
    runner = entry.runtime_data.runner
    check('po reloadu (jako restart HA) plán obnoven z HA Store', runner.battery_plan
          and runner.battery_plan['id'] == 'p#ha1' and hass.states.get(sens.entity_id).state == 'charge_grid',
          (runner.battery_plan and runner.battery_plan['id'], hass.states.get(sens.entity_id)))
    srv['id'] = None
    await until(lambda: runner.battery_plan is None)
    await until(lambda: hass.states.get(sens.entity_id).state == 'unavailable')
    check('planId None → plán zahozen, senzor i kalendář nedostupné',
          hass.states.get(sens.entity_id).state == 'unavailable' and hass.states.get(cal.entity_id).state == 'unavailable',
          (hass.states.get(sens.entity_id), hass.states.get(cal.entity_id)))

    short = mk_battery_plan(t0, pat, pid='p#ha2', valid_s=int(time.time() - t0) + 4, tz=CET)
    srv.update(plan=short, id='p#ha2')
    await until(lambda: hass.states.get(sens.entity_id).state == 'charge_grid')
    srv['poll'] = 300                          # další sync až za 5 min → změna jen časovačem
    await asyncio.sleep(0.6)
    syncs = len(srv['bodies'])
    ok_before = hass.states.get(sens.entity_id).state == 'charge_grid'
    await until(lambda: hass.states.get(sens.entity_id).state == 'unavailable', timeout=10)
    check('konec platnosti → nedostupné časovačem, bez dalšího sync', ok_before
          and hass.states.get(sens.entity_id).state == 'unavailable' and len(srv['bodies']) == syncs,
          (hass.states.get(sens.entity_id), len(srv['bodies']) - syncs))

    assert await hass.config_entries.async_unload(entry.entry_id)
    check('unload bez chyby', entry.state.name == 'NOT_LOADED', entry.state)
    await hass.async_stop(force=True)
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    shutil.rmtree(cfg, ignore_errors=True)


asyncio.run(main())
sys.exit(1 if FAILS else 0)
