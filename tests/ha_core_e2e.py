"""E2E integrace wattery uvnitř skutečného Home Assistant core (bez UI) proti LOKÁLNÍMU serveru deye-monitor
(DEYE_EDGE=1, přihlášení vypnuté). Potřebuje venv s balíkem homeassistant a vedle repa checkout deye-monitor.
  <venv>/python tests/ha_core_e2e.py <url lokálního serveru> <párovací kód> <sn>
Služba solarman.read_holding_registers je falešná (registry z deye-monitor/tools/edge_sim.py).
Zkouší: párování (špatný/dobrý kód), čtení SN, senzor online, přijetí bloků serverem, odpojení → reauth."""
import asyncio
import os
import shutil
import sys
import tempfile
import time

URL, CODE, SN = sys.argv[1], sys.argv[2], sys.argv[3]
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, '..', 'deye-monitor', 'tools'))
import edge_sim  # noqa: E402

from homeassistant import loader  # noqa: E402
from homeassistant.config_entries import ConfigEntries  # noqa: E402
from homeassistant.core import HomeAssistant, SupportsResponse  # noqa: E402
from homeassistant.helpers import device_registry as dr, entity_registry as er  # noqa: E402
from homeassistant.helpers import translation  # noqa: E402,F401
from homeassistant.setup import async_setup_component  # noqa: E402

FAILS = []


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


async def main():
    cfg = tempfile.mkdtemp(prefix='ha-e2e-')
    shutil.copytree(os.path.join(REPO, 'custom_components'), os.path.join(cfg, 'custom_components'))
    hass = HomeAssistant(cfg)
    hass.config.skip_pip = True
    loader.async_setup(hass)
    await dr.async_load(hass)
    await er.async_load(hass)
    hass.config_entries = ConfigEntries(hass, {})
    await hass.config_entries.async_initialize()
    await hass.async_start()

    calls = []
    regs = edge_sim.fake_regs(SN, time.time())

    async def fake_read(call):
        a, n = int(call.data['address']), int(call.data['count'])
        calls.append((call.data['device'], a, n))
        return {a + i: regs.get(a + i, 0) for i in range(n)}
    hass.services.async_register('solarman', 'read_holding_registers', fake_read,
                                 supports_response=SupportsResponse.OPTIONAL)
    hass.services.async_register('solarman', 'read_input_registers', fake_read,
                                 supports_response=SupportsResponse.OPTIONAL)

    # adresa serveru → lokální (jen v testu)
    # (sdílená HTTP session HA potřebuje komponentu network — v holém testu ji nahradí obyčejná aiohttp)
    import aiohttp
    import custom_components.wattery.api as api
    sess = aiohttp.ClientSession()

    async def post(path, body, token=None):
        headers = {'Authorization': f'Bearer {token}'} if token else {}
        async with sess.post(URL + path, json=body, headers=headers, timeout=api.HTTP_TIMEOUT) as r:
            return r.status, await r.json(content_type=None)
    api.make_post = lambda h, base_url=URL: post

    res = await hass.config_entries.flow.async_init('wattery', context={'source': 'user'})
    check('formulář párování', res['type'] == 'form' and res['step_id'] == 'user', res)
    bad = await hass.config_entries.flow.async_configure(res['flow_id'], {'code': 'XXXX-XXXX', 'device': 'dev1'})
    check('špatný kód → chyba bad_code', bad['type'] == 'form' and bad['errors'] == {'base': 'bad_code'}, bad)
    ok = await hass.config_entries.flow.async_configure(res['flow_id'], {'code': CODE.lower(), 'device': 'dev1'})
    check('správný kód → záznam vytvořen', ok['type'] == 'create_entry' and ok['data']['sn'] == SN, ok)
    check('SN přečteno ze střídače (3/5)', ('dev1', 3, 5) in calls, calls)
    await hass.async_block_till_done()
    entry = hass.config_entries.async_entries('wattery')[0]
    check('záznam načten', entry.state.name == 'LOADED', entry.state)
    for _ in range(40):
        await asyncio.sleep(0.25)
        st = hass.states.get('sensor.wattery_2203084752_lokalni_spojeni') or next(
            (s for s in hass.states.async_all('sensor') if s.entity_id.startswith('sensor.wattery')), None)
        if st and st.state == 'online':
            break
    check('senzor stavu online', st is not None and st.state == 'online', st)
    if st:
        print('   ', st.entity_id, dict(st.attributes))
    check('živý blok přečten přes solarman', ('dev1', 586, 94) in calls, calls)
    runner = entry.runtime_data.runner
    check('server přijal všechny bloky', runner.last_accepted == len(runner.plan), (runner.last_accepted, runner.plan))

    dup = await hass.config_entries.flow.async_init('wattery', context={'source': 'user'})
    dup2 = await hass.config_entries.flow.async_configure(dup['flow_id'], {'code': CODE, 'device': 'dev1'})
    check('druhé párování téhož střídače → chyba (kód už použitý)', dup2['type'] in ('form', 'abort'), dup2)

    # odpojení ve wattery → HA nabídne spárovat znovu (reauth) → nový kód → zase online
    st_code, _ = await post('/api/deye/edge/revoke', {'stationId': entry.data['station_id'],
                                                      'clientId': entry.data['client_id']})
    check('odpojení na serveru', st_code == 200, st_code)
    runner._last_sync = 0
    flows = []
    for _ in range(60):
        await asyncio.sleep(0.5)
        flows = [f for f in hass.config_entries.flow.async_progress() if f['context'].get('source') == 'reauth']
        if flows:
            break
    check('401 → HA nabízí nové spárování (reauth)', bool(flows) and runner.status == 'revoked', (flows, runner.status))
    if flows:
        _, new = await post('/api/deye/edge/pair-code', {'stationId': entry.data['station_id']})
        rr = await hass.config_entries.flow.async_configure(flows[0]['flow_id'], {'code': new['code']})
        check('reauth novým kódem → hotovo', rr['type'] == 'abort' and rr['reason'] == 'reauth_successful', rr)
        await hass.async_block_till_done()
        entry = hass.config_entries.async_entries('wattery')[0]
        for _ in range(40):
            await asyncio.sleep(0.25)
            if entry.state.name == 'LOADED' and entry.runtime_data.runner.status == 'online':
                break
        check('po reauth znovu online', entry.runtime_data.runner.status == 'online', entry.runtime_data.runner.status)

    assert await hass.config_entries.async_unload(entry.entry_id)
    check('odebrání/unload bez chyby', entry.state.name == 'NOT_LOADED', entry.state)
    await sess.close()
    await hass.async_stop(force=True)
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    shutil.rmtree(cfg, ignore_errors=True)


asyncio.run(main())
