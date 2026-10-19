"""Přenos Modbus TCP přímo na dongle / převodník u střídače (Solax X3-Hybrid G4: LAN dongle, Pocket WiFi 3.0,
převodník RS485→Ethernet na COM portu). Rozhraní přenosu Modbus jako `transport_solarman.SolarmanModbus`:
`read_block(fc, addr, count)` → u16, `write_block(addr, values)` (FC16, chyba = výjimka), `identity()` → SN / None.

Vlastní asyncio klient bez závislostí (pymodbus by se pral s verzí, kterou si drží jiné integrace v HA):
  • rámování `tcp` (MBAP) nebo `rtu` (RTU přes TCP s CRC — převodníky v režimu „transparent“)
  • jedno trvalé spojení, dotazy za sebou (zámek) s mezerou `gap_s` — dongly nesnesou souběh ani rychlou palbu
  • po chybě nebo časovém limitu spojení zavře (v proudu by mohla viset stará odpověď) a znovu se připojí nejdřív
    po `reconnect_s` — žádné bušení do donglu
  • výjimka 6 (zaneprázdněn) → jednou zopakovat po krátké pauze
  • dongle pustí JEDNO spojení: když spojení hned po připojení zavře, je obsazený (homeassistant-solax-modbus,
    aplikace výrobce…) → `DongleBusy`; proto se tam, kde už je solax_modbus, nepřipojujeme (`conflict_info`)
Zápis jde jen tak, jak ho pošle server a pustí profil zápisu v `edge.py` (tady se nic nekontroluje navíc).
"""
from __future__ import annotations

import asyncio
import struct
import time

FRAMINGS = ('tcp', 'rtu')
DEFAULT_PORT = 502
DEFAULT_UNIT = 1
SOLAX_IDENTITY = (3, 0x0000, 7)          # holding 0x0000–0x0006 = SN „H34…“ (ASCII, 2 znaky na registr)
EXC_TEXT = {1: 'nepodporovaná funkce', 2: 'neplatná adresa', 3: 'neplatná hodnota', 4: 'chyba zařízení',
            6: 'zaneprázdněn', 10: 'brána: cesta nedostupná', 11: 'brána: zařízení neodpovídá'}


class ModbusTcpError(Exception):
    """Chyba spojení / odpovědi (bez kódu) nebo výjimka Modbus (`code`)."""

    def __init__(self, msg, code=None, reason=None):
        super().__init__(msg)
        self.code = code
        self.reason = reason or ('exception' if code else 'link')


class DongleBusy(ModbusTcpError):
    """Dongle spojení hned zavřel → drží ho někdo jiný (pustí jen jedno)."""

    def __init__(self, msg):
        super().__init__(msg, reason='busy')


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def decode_ascii(regs) -> str | None:
    """Registry → text (2 znaky na registr, vyšší bajt první); nula = konec/výplň; netisknutelný znak = None."""
    out = ''
    for r in regs or ():
        for c in ((int(r) >> 8) & 0xFF, int(r) & 0xFF):
            if c == 0:
                continue
            if c < 0x20 or c > 0x7E:
                return None
            out += chr(c)
    return out.strip() or None


def conflict_info(solax_modbus_options: list, host: str, port: int = DEFAULT_PORT) -> dict | None:
    """Překáží spojení na host:port položka integrace homeassistant-solax-modbus? `solax_modbus_options` = options
    (sloučené s data) jejích config entries. Vrací {code, name, host, port} (text hlášky skládá flow v jazyce UI),
    nebo None. code:
      'same_address' — interface 'tcp' se stejným host:port (dongle / převodník pustí jedno spojení)
      'core_modbus'  — 'core_tcp' / 'core_serial' / 'core' (Modbus jádra HA, adresa je v YAML) → nevíme kam
      'serial'       — solax_modbus jede přes vlastní USB převodník; dongle je volný, ale střídač už čte někdo jiný
    Pravidlo §5 (Petr 16. 10.): kdo má solax_modbus, toho čteme z entit a druhé spojení nezakládáme — flow má
    nabídnout „Jen čtení z jiné integrace“; tahle funkce jen hlídá, ať se omylem nepřipojíme na totéž."""
    h = str(host or '').strip().lower()
    for o in solax_modbus_options or ():
        o = o or {}
        iface = str(o.get('interface') or 'tcp')
        info = {'name': str(o.get('name') or 'SolaX'), 'host': host, 'port': int(port or DEFAULT_PORT)}
        if iface == 'tcp' and str(o.get('host') or '').strip().lower() == h \
                and int(o.get('port') or DEFAULT_PORT) == int(port or DEFAULT_PORT):
            return {'code': 'same_address', **info}
        if iface.startswith('core'):
            return {'code': 'core_modbus', **info}
        if iface == 'serial':
            return {'code': 'serial', **info}
    return None


class ModbusTcp:
    """Jedno zařízení (unit) za donglem / převodníkem na host:port."""

    def __init__(self, host: str, port: int = DEFAULT_PORT, unit: int = DEFAULT_UNIT, framing: str = 'tcp', *,
                 timeout_s: float = 3.0, connect_timeout_s: float = 5.0, gap_s: float = 0.1,
                 reconnect_s: float = 10.0, identity_spec=SOLAX_IDENTITY, clock=time.monotonic):
        if framing not in FRAMINGS:
            raise ValueError(f'rámování {framing!r} (umím {", ".join(FRAMINGS)})')
        if not 0 < int(unit) < 248:
            raise ValueError('adresa jednotky 1–247')
        self.host, self.port, self.unit, self.framing = host, int(port), int(unit), framing
        self.timeout_s, self.connect_timeout_s = timeout_s, connect_timeout_s
        self.gap_s, self.reconnect_s, self.identity_spec = gap_s, reconnect_s, identity_spec
        self._clock = clock
        self._lock = asyncio.Lock()
        self._r = self._w = None
        self._tid = 0
        self._last = 0.0            # konec posledního dotazu (mezera gap_s)
        self._retry_at = 0.0        # dřív se znovu nepřipojovat
        self.last_error = None
        self.stats = {'requests': 0, 'errors': 0, 'connects': 0, 'busy': 0}

    def __repr__(self):
        return f'<ModbusTcp {self.framing} {self.host}:{self.port} unit {self.unit}>'

    # ── rozhraní přenosu ──
    async def read_block(self, fc: int, addr: int, count: int) -> list:
        if fc not in (3, 4) or not 1 <= count <= 125 or not 0 <= addr <= 0xFFFF - count + 1:
            raise ValueError(f'čtení FC{fc} {addr}×{count} mimo rozsah')
        pdu = await self._call(struct.pack('>BHH', fc, addr, count))
        if len(pdu) != 2 + 2 * count or pdu[1] != 2 * count:
            raise self._broken(f'odpověď FC{fc}: čekáno {count} registrů, přišlo {len(pdu)} B')
        return list(struct.unpack(f'>{count}H', pdu[2:]))

    async def write_block(self, addr: int, values: list) -> None:
        vals = [int(v) for v in values]
        if not 1 <= len(vals) <= 123 or not all(0 <= v <= 0xFFFF for v in vals) or not 0 <= addr <= 0xFFFF:
            raise ValueError(f'zápis {addr}×{len(vals)} mimo rozsah')
        pdu = await self._call(struct.pack('>BHHB', 16, addr, len(vals), 2 * len(vals))
                               + struct.pack(f'>{len(vals)}H', *vals), retry_busy=False)
        if pdu[:5] != struct.pack('>BHH', 16, addr, len(vals)):
            raise self._broken('potvrzení FC16 nesedí na zápis')

    async def identity(self) -> str | None:
        fc, addr, count = self.identity_spec
        return decode_ascii(await self.read_block(fc, addr, count))

    async def close(self) -> None:
        async with self._lock:
            await self._drop()

    # ── vnitřek ──
    async def _call(self, pdu: bytes, retry_busy=True) -> bytes:
        async with self._lock:
            for attempt in (0, 1):
                out = await self._exchange(pdu)
                if out[0] & 0x80:
                    code = out[1] if len(out) > 1 else None
                    if code == 6 and retry_busy and attempt == 0:
                        self.stats['busy'] += 1
                        await asyncio.sleep(max(self.gap_s * 5, 0.3))
                        continue
                    self.stats['errors'] += 1
                    self.last_error = f'výjimka Modbus {code} ({EXC_TEXT.get(code, "?")})'
                    raise ModbusTcpError(f'FC{pdu[0]}: {self.last_error}', code)
                self.last_error = None
                return out
        raise AssertionError('nedosažitelné')

    async def _exchange(self, pdu: bytes) -> bytes:
        await self._connect()
        wait = self._last + self.gap_s - self._clock()
        if wait > 0:
            await asyncio.sleep(wait)
        self.stats['requests'] += 1
        try:
            if self.framing == 'tcp':
                self._tid = (self._tid + 1) & 0xFFFF
                self._w.write(struct.pack('>HHHB', self._tid, 0, len(pdu) + 1, self.unit) + pdu)
                await self._w.drain()
                async with asyncio.timeout(self.timeout_s):
                    while True:
                        head = await self._r.readexactly(7)
                        tid, proto, length, unit = struct.unpack('>HHHB', head)
                        if not 2 <= length <= 254:
                            raise self._broken(f'neplatná délka MBAP {length}')
                        body = await self._r.readexactly(length - 1)
                        if tid == self._tid and proto == 0 and unit == self.unit:
                            break                     # stará odpověď (jiné tid) se zahodí
            else:
                frame = bytes([self.unit]) + pdu
                self._w.write(frame + struct.pack('<H', crc16(frame)))
                await self._w.drain()
                async with asyncio.timeout(self.timeout_s):
                    body = await self._read_rtu(pdu[0])
        except asyncio.TimeoutError:
            await self._fail('zařízení neodpovídá (časový limit) — špatná adresa jednotky, nebo dongle zaneprázdněn')
            raise ModbusTcpError(self.last_error, reason='timeout') from None
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as e:
            busy = self.stats['requests'] == 1 and (isinstance(e, ConnectionResetError) or (
                isinstance(e, asyncio.IncompleteReadError) and not e.partial))
            await self._fail('dongle zavřel spojení — drží ho jiný klient (pustí jen jedno)' if busy
                             else f'spojení přerušeno ({type(e).__name__})')
            raise (DongleBusy if busy else ModbusTcpError)(self.last_error) from None
        finally:
            self._last = self._clock()
        if not body or (body[0] & 0x7F) != pdu[0]:
            raise self._broken(f'odpověď na jinou funkci ({body[:1].hex()} ≠ {pdu[0]:02x})')
        return body

    async def _read_rtu(self, fc: int) -> bytes:
        head = await self._r.readexactly(3)          # unit, fc, (počet bajtů | kód výjimky | horní bajt adresy)
        if head[1] & 0x80:
            rest = await self._r.readexactly(2)
        elif head[1] in (3, 4):
            rest = await self._r.readexactly(head[2] + 2)
        else:
            rest = await self._r.readexactly(5)
        raw = head + rest
        if crc16(raw[:-2]) != struct.unpack('<H', raw[-2:])[0]:
            raise self._broken('CRC odpovědi RTU nesedí')
        if raw[0] != self.unit:
            raise self._broken(f'odpověď od jednotky {raw[0]}, čekána {self.unit}')
        return raw[1:-2]

    async def _connect(self):
        if self._w is not None and not self._w.is_closing():
            return
        now = self._clock()
        if now < self._retry_at:
            raise ModbusTcpError(f'čekám na nové spojení ({self._retry_at - now:.0f} s): {self.last_error}',
                                 reason='connect')
        try:
            async with asyncio.timeout(self.connect_timeout_s):
                self._r, self._w = await asyncio.open_connection(self.host, self.port)
        except (asyncio.TimeoutError, OSError) as e:
            self._r = self._w = None
            self._retry_at = self._clock() + self.reconnect_s
            self.stats['errors'] += 1
            self.last_error = f'nelze se připojit na {self.host}:{self.port} ({type(e).__name__})'
            raise ModbusTcpError(self.last_error, reason='connect') from None
        self.stats['connects'] += 1
        self.stats['requests'] = 0                     # počítadlo dotazů v tomhle spojení (pozná obsazený dongle)

    def _broken(self, msg) -> ModbusTcpError:
        """Odpověď nedává smysl → proud je rozhozený: spojení zahodit (synchronně, zavře se při dalším dotazu)."""
        self.stats['errors'] += 1
        self.last_error = msg
        if self._w is not None:
            self._w.close()
        self._r = self._w = None
        return ModbusTcpError(msg, reason='frame')

    async def _fail(self, msg):
        self.stats['errors'] += 1
        self.last_error = msg
        self._retry_at = self._clock() + self.reconnect_s
        await self._drop()

    async def _drop(self):
        w, self._r, self._w = self._w, None, None
        if w is not None:
            w.close()
            try:
                await w.wait_closed()
            except (ConnectionError, OSError):
                pass


async def probe(host: str, port: int = DEFAULT_PORT, unit: int = DEFAULT_UNIT, framing: str = 'tcp',
                identity_spec=SOLAX_IDENTITY, timeout_s: float = 3.0) -> dict:
    """Pro krok config flow: připojit, přečíst identitu, odpojit. {ok, sn} nebo {ok: False, error, reason}.
    reason: 'connect' (nejde se připojit), 'busy' (dongle drží jiný klient), 'timeout' (nic neodpovídá —
    špatná jednotka / rámování), 'exception' (výjimka Modbus — špatná mapa / jiný střídač), 'frame' (nesmyslná
    odpověď — špatné rámování), 'link' (spojení přerušeno), 'identity' (odpověď není text SN)."""
    c = ModbusTcp(host, port, unit, framing, timeout_s=timeout_s, reconnect_s=0, identity_spec=identity_spec)
    try:
        sn = await c.identity()
        if not sn:
            return {'ok': False, 'reason': 'identity', 'error': 'odpověď není sériové číslo (jiné zařízení?)'}
        return {'ok': True, 'sn': sn}
    except ModbusTcpError as e:
        return {'ok': False, 'reason': e.reason, 'error': str(e)}
    finally:
        await c.close()
