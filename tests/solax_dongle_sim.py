"""Simulátor dongle / převodníku Modbus TCP ke střídači Solax X3-Hybrid G4 — pro testy přenosu bez sítě a bez HW.

Mluví na 127.0.0.1 buď Modbus TCP (MBAP, `framing='tcp'`, Pocket WiFi 3.0 / LAN dongle), nebo RTU přes TCP
(`framing='rtu'`, CRC16 — levné převodníky RS485→Ethernet v režimu „transparent“). Umí FC03, FC04, FC06 a FC16,
výjimky Modbus (1 funkce, 2 adresa, 3 hodnota, 6 zaneprázdněn) a chování skutečného donglu, na které musí přenos
pamatovat:
  • jen JEDNO spojení (`max_clients=1`) — další spojení dongle hned zavře (`refused` je počítá); proto se přenos
    wattery nepřipojí tam, kde už je homeassistant-solax-modbus
  • jiná adresa jednotky = žádná odpověď (sběrnice RS485 mlčí), přenos musí mít časový limit
  • `min_gap_s` — dotaz dřív než po mezeře od předchozí odpovědi → výjimka 6 (zaneprázdněn)
  • `max_read` — delší čtení → výjimka 2 (některé dongly pustí méně než 125 registrů)
Zařízení za donglem je objekt s `read(fc, addr, count) -> [u16]` a `write(addr, values)`; chyba = výjimka s `code`.
Výchozí `SolaxRegs` je malá mapa G4 (oddělená mapa zápisu a čtení jako skutečný střídač: zápis VPP 0x7C se čte na
input 0x100, limit přetoku 0x42 na holding 0xB6). V E2E s deye-monitor lze dát `deye.edge_solax_sim.SolaxSim`.
SN a hodnoty jsou smyšlené (veřejné repo).
"""
import asyncio
import struct
import time

SN = 'H34A15SIM00001'


class ModbusError(Exception):
    def __init__(self, code, msg=''):
        super().__init__(f'Modbus výjimka {code}{": " + msg if msg else ""}')
        self.code = code


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def _s32_lo_hi(v):
    v &= 0xFFFFFFFF
    return [v & 0xFFFF, v >> 16]


class SolaxRegs:
    """Minimální G4: input data (FC04), identita a nastavení (FC03), zápisy VPP a limitu přetoku."""

    def __init__(self, sn=SN):
        self.writes = []
        self.input = {a: 0 for a in range(0x00, 0x54)}
        self.input.update({a: 0 for a in range(0x6A, 0x9C)})
        self.input.update({0x02: 2400, 0x09: 2, 0x0A: 2100, 0x0B: 1500, 0x14: 2051, 0x16: 1200, 0x1C: 71,
                           0x100: 0})
        lo, hi = _s32_lo_hi(-850)                       # measured_power: + přetok / − odběr
        self.input.update({0x46: lo, 0x47: hi})
        b = sn.encode('ascii').ljust(14, b'\0')
        self.holding = {i: (b[2 * i] << 8) | b[2 * i + 1] for i in range(7)}
        self.holding.update({0x7D: 1, 0x82: 100, 0xB5: 1650, 0xB6: 1650})

    def read(self, fc, addr, count):
        bank = self.input if fc == 4 else self.holding if fc == 3 else None
        if bank is None:
            raise ModbusError(1)
        if any(a not in bank for a in range(addr, addr + count)):
            raise ModbusError(2, f'FC{fc} 0x{addr:04X} × {count}')
        return [bank[a] & 0xFFFF for a in range(addr, addr + count)]

    def write(self, addr, values):
        v = [int(x) & 0xFFFF for x in values]
        self.writes.append((addr, list(v)))
        if addr == 0x7C and len(v) in (13, 15):
            self.input[0x100] = v[0]
            self.input[0x09] = 20 if v[0] else 2       # Normal (R) při VPP, jinak Normal
        elif addr == 0x42 and len(v) == 1:
            if v[0] > self.holding[0xB5]:
                raise ModbusError(3, 'nad limitem modelu')
            self.holding[0xB6] = v[0]
        else:
            raise ModbusError(2, f'zápis 0x{addr:04X} × {len(v)}')


class DongleSim:
    def __init__(self, device=None, *, framing='tcp', unit=1, max_clients=1, min_gap_s=0.0, max_read=125,
                 delay_s=0.0):
        assert framing in ('tcp', 'rtu')
        self.device = device or SolaxRegs()
        self.framing, self.unit, self.max_clients = framing, unit, max_clients
        self.min_gap_s, self.max_read, self.delay_s = min_gap_s, max_read, delay_s
        self.requests = []          # (unit, fc, addr, count) každého přijatého dotazu
        self.refused = 0            # spojení zavřená kvůli max_clients
        self.connections = 0
        self.port = None
        self._server = None
        self._active = 0
        self._last_reply = 0.0

    async def start(self):
        self._server = await asyncio.start_server(self._client, '127.0.0.1', 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # ── zpracování PDU (společné pro obě rámování) ──
    def _pdu(self, pdu: bytes) -> bytes:
        fc = pdu[0]
        try:
            if self.min_gap_s and time.monotonic() - self._last_reply < self.min_gap_s:
                raise ModbusError(6)
            if fc in (3, 4):
                addr, count = struct.unpack('>HH', pdu[1:5])
                self.requests.append((self.unit, fc, addr, count))
                if not 1 <= count <= 125:
                    raise ModbusError(3)
                if count > self.max_read:
                    raise ModbusError(2, f'dongle čte nejvýš {self.max_read}')
                vals = self.device.read(fc, addr, count)
                return bytes([fc, 2 * count]) + struct.pack(f'>{count}H', *vals)
            if fc == 6:
                addr, val = struct.unpack('>HH', pdu[1:5])
                self.requests.append((self.unit, fc, addr, 1))
                self.device.write(addr, [val])
                return pdu[:5]
            if fc == 16:
                addr, count, nbytes = struct.unpack('>HHB', pdu[1:6])
                self.requests.append((self.unit, fc, addr, count))
                if not 1 <= count <= 123 or nbytes != 2 * count or len(pdu) != 6 + nbytes:
                    raise ModbusError(3)
                self.device.write(addr, list(struct.unpack(f'>{count}H', pdu[6:6 + nbytes])))
                return bytes([fc]) + struct.pack('>HH', addr, count)
            self.requests.append((self.unit, fc, None, None))
            raise ModbusError(1)
        except Exception as e:  # noqa: BLE001 — výjimka zařízení → výjimka Modbus (kód z ní, jinak 4)
            code = getattr(e, 'code', 4)
            return bytes([fc | 0x80, code if isinstance(code, int) and 1 <= code <= 11 else 4])

    async def _client(self, reader, writer):
        if self._active >= self.max_clients:
            self.refused += 1
            writer.close()
            return
        self._active += 1
        self.connections += 1
        try:
            while True:
                if self.framing == 'tcp':
                    head = await reader.readexactly(7)
                    tid, proto, length, unit = struct.unpack('>HHHB', head)
                    pdu = await reader.readexactly(length - 1)
                    if proto != 0 or unit != self.unit:
                        continue                          # jiná jednotka: sběrnice mlčí
                    out = self._pdu(pdu)
                    frame = struct.pack('>HHHB', tid, 0, len(out) + 1, unit) + out
                else:
                    frame = await self._rtu_request(reader)
                    if frame is None:
                        continue
                if self.delay_s:
                    await asyncio.sleep(self.delay_s)
                writer.write(frame)
                await writer.drain()
                self._last_reply = time.monotonic()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self._active -= 1
            writer.close()

    async def _rtu_request(self, reader):
        """RTU přes TCP: [unit, fc, …, crc_lo, crc_hi]. Délka podle funkce; špatné CRC / jiná jednotka = mlčí."""
        head = await reader.readexactly(2)
        unit, fc = head
        if fc in (3, 4, 6):
            rest = await reader.readexactly(6)
        elif fc == 16:
            mid = await reader.readexactly(5)
            rest = mid + await reader.readexactly(mid[4] + 2)
        else:
            rest = await reader.readexactly(2)
        raw = head + rest
        if crc16(raw[:-2]) != struct.unpack('<H', raw[-2:])[0] or unit != self.unit:
            return None
        out = bytes([unit]) + self._pdu(raw[1:-2])
        return out + struct.pack('<H', crc16(out))
