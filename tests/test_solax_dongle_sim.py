"""Testy podkladů pro Solax v klientu HA (bez Home Assistantu, bez sítě): python tests/test_solax_dongle_sim.py

1. Simulátor donglu Modbus TCP (tests/solax_dongle_sim.py) — ověřuje sám sebe syrovými rámci, ať je na čem stavět
   přenos: FC03/FC04/FC06/FC16, výjimky, jediné spojení, jiná jednotka mlčí, mezera mezi dotazy, RTU přes TCP s CRC,
   oddělená mapa zápisu a čtení (VPP 0x7C → input 0x100, limit přetoku 0x42 → holding 0xB6).
2. Testovací data entit homeassistant-solax-modbus (tests/fixtures/solax_modbus_g4.json) — tvar jako reads ha_entity.
Když je vedle repa checkout deye-monitor: data projdou adaptérem serveru (deye/edge_solax.py) s očekávaným
výsledkem a simulátor jede i se simulátorem střídače deye.edge_solax_sim.SolaxSim (fyzika VPP).
"""
import asyncio
import json
import os
import struct
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import solax_dongle_sim as ds  # noqa: E402

DM = os.path.abspath(os.path.join(_HERE, '..', '..', 'deye-monitor'))
FIXTURE = os.path.join(_HERE, 'fixtures', 'solax_modbus_g4.json')
FAILS = []


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


class Raw:
    """Nejjednodušší klient pro test simulátoru (přenos wattery bude mít vlastní)."""

    def __init__(self, port, framing='tcp', unit=1):
        self.port, self.framing, self.unit, self.tid = port, framing, unit, 0

    async def open(self):
        self.r, self.w = await asyncio.open_connection('127.0.0.1', self.port)
        return self

    async def close(self):
        self.w.close()

    async def call(self, pdu, timeout=0.5, unit=None):
        unit = self.unit if unit is None else unit
        if self.framing == 'tcp':
            self.tid = (self.tid + 1) & 0xFFFF
            self.w.write(struct.pack('>HHHB', self.tid, 0, len(pdu) + 1, unit) + pdu)
            await self.w.drain()
            head = await asyncio.wait_for(self.r.readexactly(7), timeout)
            tid, _p, length, _u = struct.unpack('>HHHB', head)
            assert tid == self.tid
            return await self.r.readexactly(length - 1)
        frame = bytes([unit]) + pdu
        self.w.write(frame + struct.pack('<H', ds.crc16(frame)))
        await self.w.drain()
        head = await asyncio.wait_for(self.r.readexactly(3), timeout)
        if head[1] & 0x80:
            rest = await self.r.readexactly(2)
        elif head[1] in (3, 4):
            rest = await self.r.readexactly(head[2] + 2)
        else:
            rest = await self.r.readexactly(5)
        raw = head + rest
        assert ds.crc16(raw[:-2]) == struct.unpack('<H', raw[-2:])[0], 'CRC odpovědi'
        return raw[1:-2]

    async def read(self, fc, addr, count, **kw):
        out = await self.call(struct.pack('>BHH', fc, addr, count), **kw)
        if out[0] & 0x80:
            return ('exc', out[1])
        return list(struct.unpack(f'>{out[1] // 2}H', out[2:]))

    async def write(self, addr, values):
        out = await self.call(struct.pack('>BHHB', 16, addr, len(values), 2 * len(values))
                              + struct.pack(f'>{len(values)}H', *values))
        return ('exc', out[1]) if out[0] & 0x80 else 'ok'


def _ascii(regs):
    return b''.join(struct.pack('>H', r) for r in regs).rstrip(b'\0').decode('ascii')


async def dongle_tcp():
    sim = await ds.DongleSim().start()
    try:
        c = await Raw(sim.port).open()
        check('FC03 holding 0×7 = SN „H34…“', _ascii(await c.read(3, 0, 7)) == ds.SN)
        live = await c.read(4, 0, 0x54)
        check('FC04 živý blok 0×0x54 (SOC 71, run mode Normal)', len(live) == 0x54 and live[0x1C] == 71
              and live[0x09] == 2)
        check('neznámá adresa → výjimka 2', await c.read(3, 0x3000, 2) == ('exc', 2))
        check('neznámá funkce → výjimka 1', (await c.call(bytes([0x2B, 0x0E, 1, 0])))[:2] == bytes([0xAB, 1]))
        check('jiná jednotka → bez odpovědi (časový limit)',
              await _timeout(c.read(4, 0, 1, unit=7, timeout=0.3)))
        c2 = await Raw(sim.port).open()
        await asyncio.sleep(0.05)
        closed = await _closed(c2)
        check('druhé spojení dongle hned zavře (jen jedno Modbus TCP)', closed and sim.refused == 1)
        # jiná jednotka nechala v proudu nepřečtenou nic — spojení dál funguje
        c = await _reopen(c, sim)
        vpp = [4, 1] + [0] * 10 + [60] + ds._s32_lo_hi(2000)
        check('FC16 VPP 0x7C × 15 → ok', await c.write(0x7C, vpp) == 'ok')
        check('zápis se čte JINDE: input 0x100 = režim 4, run mode = Normal (R)',
              (await c.read(4, 0x100, 1)) == [4] and (await c.read(4, 0x09, 1)) == [20])
        check('na zapsané adrese 0x7C se čte něco jiného (holding ≠ zapsaný blok)',
              (await c.read(3, 0x7C, 2)) != vpp[:2])
        check('limit přetoku 0x42 = 500 (5 kW) → holding 0xB6 = 500',
              await c.write(0x42, [500]) == 'ok' and (await c.read(3, 0xB6, 1)) == [500])
        check('limit přetoku nad strop modelu → výjimka 3', await c.write(0x42, [1651]) == ('exc', 3))
        check('zápis mimo mapu → výjimka 2', await c.write(0x8F, [1]) == ('exc', 2))
        check('záznam dotazů (jednotka, fc, adresa, počet)', (1, 16, 0x7C, 15) in sim.requests
              and (7, 4, 0, 1) not in sim.requests)
        await c.close()
    finally:
        await sim.stop()


async def dongle_quirks():
    sim = await ds.DongleSim(min_gap_s=0.2, max_read=40).start()
    try:
        c = await Raw(sim.port).open()
        await c.read(4, 0, 10)
        check('dotaz hned po odpovědi → výjimka 6 (zaneprázdněn)', await c.read(4, 0, 10) == ('exc', 6))
        await asyncio.sleep(0.25)
        check('po mezeře zase odpovídá', len(await c.read(4, 0, 10)) == 10)
        await asyncio.sleep(0.25)
        check('čtení nad strop donglu (40) → výjimka 2', await c.read(4, 0, 0x54) == ('exc', 2))
        await c.close()
    finally:
        await sim.stop()


async def dongle_rtu():
    sim = await ds.DongleSim(framing='rtu').start()
    try:
        c = await Raw(sim.port, framing='rtu').open()
        check('RTU přes TCP: FC03 SN s CRC', _ascii(await c.read(3, 0, 7)) == ds.SN)
        check('RTU přes TCP: FC16 + zpětné čtení', await c.write(0x7C, [0] * 13) == 'ok'
              and (await c.read(4, 0x100, 1)) == [0])
        c.w.write(bytes([1, 3, 0, 0, 0, 7, 0, 0]))       # špatné CRC → sběrnice mlčí
        await c.w.drain()
        check('RTU: špatné CRC → bez odpovědi', await _timeout(c.r.readexactly(3), 0.3))
        await c.close()
    finally:
        await sim.stop()


def fixture():
    f = json.load(open(FIXTURE, encoding='utf-8'))
    ents = f['entities']
    shape = all(isinstance(k, str) and '.' in k and set(v) == {'state', 'unit', 'uid'} for k, v in ents.items())
    check(f'testovací entity: {len(ents)} × {{state, unit, uid}} (tvar reads ha_entity)', shape)
    check('uid = „<hub>_<klíč>“ u všech entit', all(v['uid'].split('_', 1)[0] in ('SolaX', 'Garaz')
                                                  for v in ents.values()))
    if not os.path.isdir(os.path.join(DM, 'deye')):
        print('—    deye-monitor vedle repa není → bez kontroly proti adaptéru serveru')
        return
    sys.path.insert(0, DM)
    from deye import edge_solax as es
    st = {'type': 'ha_entity', 'entities': ents, 'prefix': f['prefix'], 'at': 1}
    d = es.decode(st)
    got = {x['key']: x['value'] for x in d['dataList']}
    check(f'adaptér serveru: dataList = očekávaný ({len(f["expect"]["dataList"])} klíčů)',
          got == f['expect']['dataList'], {k: (f['expect']['dataList'].get(k), got.get(k))
                                            for k in set(got) | set(f['expect']['dataList'])
                                            if got.get(k) != f['expect']['dataList'].get(k)})
    check('adaptér serveru: run mode „Normal Mode“ → runState 2',
          (d['runState'], d['runMode']) == (f['expect']['runState'], f['expect']['runMode']))
    amb = es.decode_ha_entities(ents).get('ambiguous')
    check('bez prefixu se dva huby nesmíchají (nic, ambiguous)', amb == f['expect']['ambiguousWithoutPrefix'], amb)


async def with_solax_sim():
    if not os.path.isdir(os.path.join(DM, 'deye')):
        return
    sys.path.insert(0, DM)
    from deye.edge_solax_sim import SolaxSim
    inv = SolaxSim()
    sim = await ds.DongleSim(inv).start()
    try:
        c = await Raw(sim.port).open()
        check('dongle + SolaxSim: SN z holdingu', _ascii(await c.read(3, 0, 7)) == inv.sn)
        vpp = [4, 1] + [0] * 10 + [60] + ds._s32_lo_hi(-3000)
        ok = await c.write(0x7C, vpp) == 'ok'
        inv.step(5)
        check('dongle + SolaxSim: VPP nabíjení 3 kW → input 0x100 = 4', ok and (await c.read(4, 0x100, 1)) == [4])
        check('dongle + SolaxSim: blok 0x7C × 14 → výjimka 3', await c.write(0x7C, [4] + [0] * 13) == ('exc', 3))
        await c.close()
    finally:
        await sim.stop()


async def _timeout(coro, t=None):
    try:
        await (asyncio.wait_for(coro, t) if t else coro)
    except asyncio.TimeoutError:
        return True
    return False


async def _closed(c):
    try:
        return await asyncio.wait_for(c.r.read(1), 0.5) == b''
    except (asyncio.TimeoutError, ConnectionError):
        return False


async def _reopen(c, sim):
    await c.close()
    await asyncio.sleep(0.05)
    return await Raw(sim.port).open()


if __name__ == '__main__':
    asyncio.run(dongle_tcp())
    asyncio.run(dongle_quirks())
    asyncio.run(dongle_rtu())
    fixture()
    asyncio.run(with_solax_sim())
    print(f'\n{"FAIL" if FAILS else "OK"} — {len(FAILS)} chyb')
    sys.exit(1 if FAILS else 0)
