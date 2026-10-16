"""Řízení uvnitř skutečného Home Assistant core (bez UI a bez serveru): integrace wattery s falešnými službami
ha-solarman (čtení i write_multiple_registers) a falešným serverem wattery, který posílá příkazy.
  <venv s balíkem homeassistant>/python tests/ha_core_control.py
Zkouší: přepínač „Řízení z wattery“ (výchozí vypnuto, kategorie Konfigurace), vypnutý → zápis odmítnut „local-off“
a do ha-solarman nic; zapnutí službou switch.turn_on → stav hlášený serveru (ctl), zápis přes
solarman.write_multiple_registers {device, address, values} + zpětné čtení; offline pojistka (uložení v HA Store,
obnova po reloadu, zápis neutrálu při výpadku serveru, hlášení); obnova přepínače po reloadu; vypnutí."""
import asyncio
import json
import os
import shutil
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from homeassistant import loader  # noqa: E402
from homeassistant.config_entries import ConfigEntries  # noqa: E402
from homeassistant.const import EntityCategory  # noqa: E402
from homeassistant.core import HomeAssistant, SupportsResponse  # noqa: E402
from homeassistant.helpers import device_registry as dr, entity_registry as er, restore_state  # noqa: E402

FAILS = []
SN = '2400000001'
SN_REGS = [(ord(SN[2 * i]) << 8) | ord(SN[2 * i + 1]) for i in range(5)]
PLAN = [{'name': 'live', 'type': 'modbus', 'fc': 3, 'addr': 586, 'count': 4, 'everyS': 30, 'sn': SN},
        {'name': 'sn', 'type': 'modbus', 'fc': 3, 'addr': 3, 'count': 5, 'everyS': 3600}]


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
    cfg = tempfile.mkdtemp(prefix='ha-ctl-')
    shutil.copytree(os.path.join(REPO, 'custom_components'), os.path.join(cfg, 'custom_components'))
    hass = HomeAssistant(cfg)
    hass.config.skip_pip = True
    loader.async_setup(hass)
    await dr.async_load(hass)
    await er.async_load(hass)
    await restore_state.async_load(hass)
    hass.config_entries = ConfigEntries(hass, {})
    await hass.config_entries.async_initialize()
    await hass.async_start()

    regs = {3 + i: v for i, v in enumerate(SN_REGS)}
    regs.update({586: 1, 587: 2, 588: 57, 589: 4, 143: 15500})
    writes = []

    async def fake_read(call):
        a, n = int(call.data['address']), int(call.data['count'])
        return {a + i: regs.get(a + i, 0) for i in range(n)}

    async def fake_write(call):
        writes.append(dict(call.data))
        a = int(call.data['address'])
        regs.update({a + i: int(v) for i, v in enumerate(call.data['values'])})
    for svc in ('read_holding_registers', 'read_input_registers'):
        hass.services.async_register('solarman', svc, fake_read, supports_response=SupportsResponse.OPTIONAL)
    hass.services.async_register('solarman', 'write_multiple_registers', fake_write)

    # Falešný server wattery: párování + sync, který vrací příkazy z fronty a sbírá výsledky.
    import custom_components.wattery.api as api
    import custom_components.wattery.edge as cedge
    cedge.POLL_MIN_S, cedge.FAST_POLL_S = 0.2, 0.2           # rychlé kolo jen v testu
    srv = {'cmds': [], 'results': [], 'bodies': [], 'fb': None, 'nofb': False, 'down': False}

    async def post(path, body, token=None):
        if path == cedge.PAIR_PATH:
            return 200, {'ok': True, 'token': 'TOK', 'clientId': 7, 'stationId': 1001, 'readPlan': PLAN,
                         'nextPollS': 10}
        if token != 'TOK':
            return 401, {'ok': False}
        srv['bodies'].append(body)
        if srv['down']:
            raise OSError('síť')
        srv['results'] += body.get('results') or []
        cmds, srv['cmds'] = srv['cmds'], []
        out = {'ok': True, 'nextPollS': 0.3, 'control': 'on', 'commands': cmds, 'accepted': len(body['reads']),
               'serverTime': time.time()}
        if not srv['nofb']:
            out['fallback'] = srv['fb']
        if body.get('fallbackReport'):
            out['fallbackAck'] = body['fallbackReport']['id']
        return 200, out
    api.make_post = lambda h, base_url=None: post

    res = await hass.config_entries.flow.async_init('wattery', context={'source': 'user'})
    ok = await hass.config_entries.flow.async_configure(res['flow_id'], {'code': 'abcd-efgh', 'device': 'dev1'})
    check('spárováno', ok['type'] == 'create_entry' and ok['data']['sn'] == SN, ok)
    await hass.async_block_till_done()
    entry = hass.config_entries.async_entries('wattery')[0]
    ents = er.async_entries_for_config_entry(er.async_get(hass), entry.entry_id)
    sw = next((e for e in ents if e.domain == 'switch'), None)
    sens = next((e for e in ents if e.domain == 'sensor'), None)
    check('entity: senzor stavu + přepínač řízení (kategorie Konfigurace) na jednom zařízení',
          sw and sens and sw.entity_category == EntityCategory.CONFIG and sw.device_id == sens.device_id
          and sw.unique_id == f'{SN}_control', [(e.entity_id, e.entity_category, e.device_id) for e in ents])
    st = hass.states.get(sw.entity_id) if sw else None
    check('přepínač výchozí vypnuto', st is not None and st.state == 'off', st)
    print('   ', sw.entity_id if sw else None, st.attributes if st else None)
    runner = entry.runtime_data.runner
    check('klient online, hlásí ctl off', await until(lambda: runner.status == 'online')
          and srv['bodies'] and srv['bodies'][-1]['client']['ctl'] == 'off', (runner.status, srv['bodies'][-1:]))

    def cmd(cid, addr=143, values=(15400,)):
        return {'id': cid, 'type': 'modbus', 'sn': SN, 'fc': 16, 'addr': addr, 'values': list(values), 'expiresInS': 10}

    def result(cid):
        return next((r for r in srv['results'] if r['id'] == cid), None)

    srv['cmds'] = [cmd('c-1')]
    await until(lambda: result('c-1'))
    check('přepínač vypnutý: zápis odmítnut local-off, do ha-solarman nic',
          (result('c-1') or {}).get('err') == 'local-off' and not writes and regs[143] == 15500, result('c-1'))
    srv['cmds'] = [{'id': 'c-2', 'type': 'modbus', 'sn': SN, 'fc': 3, 'addr': 586, 'count': 4, 'expiresInS': 10}]
    await until(lambda: result('c-2'))
    check('přepínač vypnutý: čtení přes solarman.read_holding_registers jde',
          (result('c-2') or {}).get('values') == [1, 2, 57, 4], result('c-2'))

    await hass.services.async_call('switch', 'turn_on', {'entity_id': sw.entity_id}, blocking=True)
    check('switch.turn_on → přepínač zapnutý, klient řídí', hass.states.get(sw.entity_id).state == 'on'
          and runner.control_enabled is True)
    check('server se o zapnutí dozví (ctl on)', await until(lambda: srv['bodies'][-1]['client']['ctl'] == 'on'))
    srv['cmds'] = [cmd('c-3')]
    await until(lambda: result('c-3'))
    r3 = result('c-3') or {}
    check('zápis přes solarman.write_multiple_registers {device, address, values} + zpětné čtení',
          r3.get('ok') is True and r3.get('readBack') == [15400] and regs[143] == 15400
          and writes == [{'device': 'dev1', 'address': 143, 'values': [15400]}], (r3, writes))
    srv['cmds'] = [cmd('c-4', addr=160, values=(5200,))]
    await until(lambda: result('c-4'))
    check('zápis mimo profil → not-allowed, do ha-solarman nic', (result('c-4') or {}).get('err', '').startswith(
        'not-allowed') and len(writes) == 1, result('c-4'))
    st = hass.states.get(sens.entity_id)
    check('senzor: atributy řízení', st.attributes.get('control') is True and st.attributes.get('server_control') == 'on'
          and st.attributes.get('commands', 0) >= 4 and '160' in (st.attributes.get('last_command') or ''),
          dict(st.attributes))

    # Offline pojistka: uložení v HA Store, obnova po reloadu, zápis přes ha-solarman při výpadku, hlášení.
    neutral = [{'fc': 16, 'addr': 0x94, 'values': [0, 500, 900, 1300, 1700, 2100] + [5000] * 6},
               {'fc': 16, 'addr': 0xA6, 'values': [15] * 6 + [0] * 6}, {'fc': 16, 'addr': 0x92, 'values': [0xFF]}]
    srv['fb'] = {'id': 'fb-1', 'validUntil': time.time() + 5, 'writes': neutral}
    path = os.path.join(cfg, '.storage', f'wattery.{entry.entry_id}')

    def stored():
        try:
            with open(path, encoding='utf-8') as f:
                return json.load(f).get('data') or {}
        except (OSError, ValueError):
            return {}
    check('pojistka ze serveru přijata a uložena v HA Store', await until(lambda: runner.fallback is not None)
          and await until(lambda: (stored().get('fallback') or {}).get('id') == 'fb-1', 5), stored())
    check('senzor: atribut fallback_until', bool(hass.states.get(sens.entity_id).attributes.get('fallback_until')),
          dict(hass.states.get(sens.entity_id).attributes))
    srv['nofb'] = True                       # server pojistku dál neposílá → po reloadu musí být z HA Store
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    entry = hass.config_entries.async_entries('wattery')[0]
    runner = entry.runtime_data.runner
    check('po reloadu pojistka načtena z HA Store', runner.fallback and runner.fallback['id'] == 'fb-1', runner.state())
    n = len(writes)
    srv['down'] = True
    ok = await until(lambda: runner.fallback_applied_at is not None, 15)
    check('výpadek serveru po platnosti → neutrál přes solarman.write_multiple_registers (3 bloky)', ok
          and [(w['device'], w['address']) for w in writes[n:]] == [('dev1', 0x94), ('dev1', 0xA6), ('dev1', 0x92)]
          and regs[0xA6] == 15, writes[n:])
    check('hlášení a čas zápisu uloženy v HA Store (přežijí restart)', await until(
        lambda: (stored().get('report') or {}).get('id') == 'fb-1' and stored().get('appliedAt'), 5), stored())
    srv['down'] = False
    check('spojení zpět → hlášení odesláno a potvrzeno', await until(lambda: runner._fb_report is None, 10)
          and any((b.get('fallbackReport') or {}).get('id') == 'fb-1' for b in srv['bodies']))
    check('a smazáno i v HA Store', await until(lambda: stored().get('report') is None and stored().get('appliedAt'), 5),
          stored())
    check('senzor: atribut fallback_applied', bool(hass.states.get(sens.entity_id).attributes.get('fallback_applied')))
    srv['fb'], srv['nofb'] = None, False

    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    entry = hass.config_entries.async_entries('wattery')[0]
    runner = entry.runtime_data.runner
    check('po reloadu (jako restart HA) přepínač obnoven zapnutý', entry.state.name == 'LOADED'
          and hass.states.get(sw.entity_id).state == 'on' and runner.control_enabled is True,
          (entry.state, hass.states.get(sw.entity_id)))
    await hass.services.async_call('switch', 'turn_off', {'entity_id': sw.entity_id}, blocking=True)
    nw = len(writes)
    srv['cmds'] = [cmd('c-5', values=(15500,))]
    await until(lambda: result('c-5'))
    check('switch.turn_off → zápis zase odmítnut local-off', (result('c-5') or {}).get('err') == 'local-off'
          and len(writes) == nw and srv['bodies'][-1]['client']['ctl'] == 'off', result('c-5'))

    hass.services.async_remove('solarman', 'write_multiple_registers')
    await hass.services.async_call('switch', 'turn_on', {'entity_id': sw.entity_id}, blocking=True)
    srv['cmds'] = [cmd('c-6')]
    await until(lambda: result('c-6'))
    check('ha-solarman bez služby zápisu → exec (server pošle přes cloud)',
          (result('c-6') or {}).get('err', '').startswith('exec'), result('c-6'))

    assert await hass.config_entries.async_unload(entry.entry_id)
    check('unload bez chyby', entry.state.name == 'NOT_LOADED', entry.state)
    await hass.async_stop(force=True)
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    shutil.rmtree(cfg, ignore_errors=True)


asyncio.run(main())
sys.exit(1 if FAILS else 0)
