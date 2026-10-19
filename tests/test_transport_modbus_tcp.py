"""Testy přenosu Modbus TCP (custom_components/wattery/transport_modbus_tcp.py) proti simulátoru donglu
(tests/solax_dongle_sim.py), bez Home Assistantu a bez sítě: python tests/test_transport_modbus_tcp.py

Rozhraní přenosu (read_block / write_block / identity) v obou rámováních, výjimky Modbus, zaneprázdněný dongle,
obsazený dongle (jedno spojení), špatná jednotka, zpožděná odpověď (stará tid se zahodí), souběh dotazů,
čekání před novým spojením, `probe` pro config flow a `conflict` proti položkám solax_modbus. Když je vedle repa
deye-monitor, jede i se simulátorem střídače SolaxSim a s plánem čtení adaptéru serveru.
"""
import asyncio
import importlib.util
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import solax_dongle_sim as ds  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    'wattery_mtcp', os.path.join(_HERE, '..', 'custom_components', 'wattery', 'transport_modbus_tcp.py'))
mt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mt)

DM = os.path.abspath(os.path.join(_HERE, '..', '..', 'deye-monitor'))
FAILS = []


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


async def _err(coro):
    try:
        await coro
    except mt.ModbusTcpError as e:
        return e
    return None


def client(sim, **kw):
    kw.setdefault('gap_s', 0)
    kw.setdefault('timeout_s', 0.5)
    return mt.ModbusTcp('127.0.0.1', sim.port, kw.pop('unit', 1), kw.pop('framing', sim.framing), **kw)


async def basics(framing):
    sim = await ds.DongleSim(framing=framing).start()
    c = client(sim)
    try:
        check(f'[{framing}] identity = SN z holding 0×7', await c.identity() == ds.SN)
        live = await c.read_block(4, 0, 0x54)
        check(f'[{framing}] read_block FC04 0×0x54: SOC 71, run mode 2', len(live) == 0x54 and live[0x1C] == 71
              and live[0x09] == 2)
        await c.write_block(0x7C, [4, 1] + [0] * 10 + [60] + ds._s32_lo_hi(-2000))
        check(f'[{framing}] write_block FC16 0x7C → zpětné čtení input 0x100 = 4, run mode 20',
              await c.read_block(4, 0x100, 1) == [4] and await c.read_block(4, 0x09, 1) == [20])
        e = await _err(c.read_block(3, 0x3000, 2))
        check(f'[{framing}] výjimka 2 → ModbusTcpError code 2, reason exception', e and e.code == 2
              and e.reason == 'exception', e)
        e = await _err(c.write_block(0x42, [1651]))
        check(f'[{framing}] zápis nad strop → výjimka 3', e and e.code == 3, e)
        check(f'[{framing}] po výjimce spojení dál jede (žádné nové připojení)',
              await c.read_block(3, 0xB6, 1) == [1650] and c.stats['connects'] == 1)
        bad = [None]
        try:
            await c.read_block(4, 0, 126)
        except ValueError as ex:
            bad[0] = ex
        check(f'[{framing}] čtení > 125 registrů odmítne klient sám (nic neodejde)', bad[0] is not None
              and not any(r[3] == 126 for r in sim.requests))
        res = await asyncio.gather(*(c.read_block(4, a, 4) for a in range(0, 40, 8)))
        check(f'[{framing}] 5 souběžných čtení za sebou, všechna správně', [len(r) for r in res] == [4] * 5
              and res[0] == live[0:4])
    finally:
        await c.close()
        await sim.stop()


async def quirks():
    sim = await ds.DongleSim(min_gap_s=0.2).start()
    c = client(sim)
    try:
        await c.read_block(4, 0, 4)
        r = await c.read_block(4, 0, 4)
        check('zaneprázdněn (výjimka 6) → jedno zopakování po pauze, pak data', len(r) == 4 and c.stats['busy'] == 1)
        c.gap_s = 0.25
        await c.read_block(4, 0, 4)
        await c.read_block(4, 0, 4)
        check('s mezerou gap_s už dongle nehlásí zaneprázdnění', c.stats['busy'] == 1)
        sim.min_gap_s = 0
        c.gap_s = 0
        await c.write_block(0x7C, [0] * 13)
        sim.min_gap_s = 10
        e = await _err(c.write_block(0x7C, [0] * 13))
        check('zápis se při zaneprázdnění neopakuje (výjimka 6 rovnou ven)', e and e.code == 6, e)
    finally:
        await c.close()
        await sim.stop()


async def failures():
    sim = await ds.DongleSim().start()
    try:
        c = client(sim, unit=7, reconnect_s=60)
        e = await _err(c.read_block(4, 0, 1))
        check('jiná jednotka → časový limit (reason timeout), spojení zavřeno', e and e.reason == 'timeout'
              and c._w is None, e)
        e2 = await _err(c.read_block(4, 0, 1))
        check('hned další dotaz se nepřipojuje (čeká reconnect_s, reason connect)', e2 and e2.reason == 'connect'
              and sim.connections == 1, e2)
        await c.close()

        sim.delay_s = 0.4
        c = client(sim, timeout_s=0.2, reconnect_s=0)
        e = await _err(c.read_block(3, 0, 7))
        sim.delay_s = 0
        check('zpožděná odpověď → časový limit', e and e.reason == 'timeout', e)
        await asyncio.sleep(0.5)     # dongle drží slot starého spojení, dokud nedopíše (proto reconnect_s)
        check('další dotaz nové spojení a správná odpověď (stará se nevezme)', await c.identity() == ds.SN
              and c.stats['connects'] == 2)
        await c.close()
        await asyncio.sleep(0.05)

        holder = client(sim)
        await holder.identity()
        other = client(sim)
        e = await _err(other.identity())
        check('druhý klient na obsazený dongle → DongleBusy (reason busy)', isinstance(e, mt.DongleBusy)
              and e.reason == 'busy', repr(e))
        p = await mt.probe('127.0.0.1', sim.port)
        check('probe na obsazený dongle → reason busy', p == {'ok': False, 'reason': 'busy', 'error': p.get('error')}
              and 'jiný klient' in p['error'], p)
        await holder.close()
        await asyncio.sleep(0.05)
        p = await mt.probe('127.0.0.1', sim.port)
        check('probe po uvolnění → ok + SN', p == {'ok': True, 'sn': ds.SN}, p)
        await other.close()
    finally:
        for x in [v for v in locals().values() if isinstance(v, mt.ModbusTcp)]:
            await x.close()          # jinak sim.stop() čeká na otevřené spojení (asyncio ≥ 3.12)
        await sim.stop()
    p = await mt.probe('127.0.0.1', sim.port, timeout_s=0.3)
    check('probe na zavřený port → reason connect', p['ok'] is False and p['reason'] == 'connect', p)
    rtu = await ds.DongleSim(framing='rtu').start()
    try:
        p = await mt.probe('127.0.0.1', rtu.port, framing='tcp', timeout_s=0.3)
        check('probe se špatným rámováním (tcp na rtu) → neprojde', p['ok'] is False
              and p['reason'] in ('timeout', 'frame', 'link'), p)
        p = await mt.probe('127.0.0.1', rtu.port, framing='rtu')
        check('probe s rtu → ok + SN', p == {'ok': True, 'sn': ds.SN}, p)
    finally:
        await rtu.stop()
    try:
        mt.ModbusTcp('h', framing='ascii')
        ok = False
    except ValueError:
        ok = True
    check('rámování ascii odmítnuto (umím tcp, rtu)', ok)


def conflicts():
    smb = [{'name': 'SolaX', 'interface': 'tcp', 'host': '192.168.1.50', 'port': 502}]
    check('conflict: stejný host:port jako solax_modbus → důvod', 'jedno spojení' in
          (mt.conflict(smb, '192.168.1.50', 502) or ''))
    check('conflict: jiný host → None', mt.conflict(smb, '192.168.1.51', 502) is None)
    check('conflict: jiný port na stejném převodníku → None', mt.conflict(smb, '192.168.1.50', 8899) is None)
    check('conflict: solax_modbus přes Modbus jádra HA → důvod (nevíme kam)',
          mt.conflict([{'interface': 'core_tcp'}], '10.0.0.2') is not None)
    check('conflict: solax_modbus sériově → důvod (střídač už čte jiná integrace)',
          'sériově' in (mt.conflict([{'name': 'Dům', 'interface': 'serial'}], '10.0.0.2') or ''))
    check('conflict: bez solax_modbus → None', mt.conflict([], '10.0.0.2') is None)
    check('conflict_info: kód + jméno + adresa (text skládá flow v jazyce UI)',
          mt.conflict_info(smb, '192.168.1.50', 502) == {'code': 'same_address', 'name': 'SolaX',
                                                         'host': '192.168.1.50', 'port': 502}
          and mt.conflict_info([{'interface': 'core_serial'}], 'x')['code'] == 'core_modbus'
          and mt.conflict_info([{'interface': 'serial'}], 'x')['code'] == 'serial'
          and mt.conflict_info(smb, '192.168.1.51') is None)
    check('decode_ascii: netisknutelné → None, nuly = výplň',
          mt.decode_ascii([0x0102]) is None and mt.decode_ascii([0x4834, 0x3300, 0]) == 'H43')


async def with_server_adapter():
    if not os.path.isdir(os.path.join(DM, 'deye')):
        print('—    deye-monitor vedle repa není → bez SolaxSim a plánu serveru')
        return
    sys.path.insert(0, DM)
    from deye import edge_solax as es
    from deye.edge_solax_sim import SolaxSim
    inv = SolaxSim()
    sim = await ds.DongleSim(inv).start()
    c = client(sim)
    try:
        check('SolaxSim: identity = SN simulátoru', await c.identity() == inv.sn)
        dev = {'deviceSn': inv.sn, 'model': '14', 'modbusProto': 100}
        state = {}
        for b in es.read_plan(dev, {'transports': ['modbus'], 'integrations': []}):
            vals = await c.read_block(b['fc'], b['addr'], b['count'])
            state.update({(b['fc'], b['addr'] + i): v for i, v in enumerate(vals)})
        snap = es.decode(state, dev)
        ref = es.decode({(fc, a): v for fc in (3, 4) for a in range(0x200)
                         for v in [_safe(inv, fc, a)] if v is not None}, dev)
        dl = {d['key']: d['value'] for d in snap['dataList']}
        check(f'plán serveru přes přenos → decode: {len(dl)} klíčů, identita a SOC jako simulátor',
              es.identity(state) == inv.sn and dl.get('SOC') == {d['key']: d['value'] for d in ref['dataList']}
              .get('SOC') and len(dl) >= 20, dl)
        cmds = es.lower({'kind': 'solax.export', 'kw': {'enable': True, 'limit_kw': 5.0}}, dev,
                        {'transports': ['modbus'], 'integrations': [], 'profiles': ['solax-g4@1']})
        cmd = cmds[0]
        await c.write_block(cmd['addr'], cmd['values'])
        rb = [{'fc': r['fc'], 'addr': r['addr'], 'count': r['count'],
               'values': await c.read_block(r['fc'], r['addr'], r['count'])} for r in cmd['readBack']]
        problem = es.verify(cmds[0].get('op') or {'kind': 'solax.export', 'kw': {'enable': True, 'limit_kw': 5.0}},
                            cmds, [{'ok': True, 'readBack': rb}])
        check('lower → write_block → readBack → verify sedí (limit přetoku 5 kW)', problem is None, problem)
    finally:
        await c.close()
        await sim.stop()


def _safe(inv, fc, a):
    try:
        return inv.read(fc, a, 1)[0]
    except Exception:  # noqa: BLE001
        return None


if __name__ == '__main__':
    asyncio.run(basics('tcp'))
    asyncio.run(basics('rtu'))
    asyncio.run(quirks())
    asyncio.run(failures())
    conflicts()
    asyncio.run(with_server_adapter())
    print(f'\n{"FAIL" if FAILS else "OK"} — {len(FAILS)} chyb')
    sys.exit(1 if FAILS else 0)
