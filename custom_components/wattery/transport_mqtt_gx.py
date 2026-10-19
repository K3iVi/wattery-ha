"""Přenos MQTT na Victron GX (Cerbo, Venus OS 3.x) v místní síti — JEN ČTENÍ.

Minimalistický klient MQTT 3.1.1 nad asyncio (bez závislostí). Integrace `mqtt` v HA povolí jen jeden broker, proto
se k brokeru GX (dbus-flashmq) připojujeme přímo — stejně jako core integrace `victron_gx`.

- Zabezpečení podle profilu „Local Network Security“ na GX: Secured / Weak = TLS 8883 + heslo (uživatel prázdný),
  Unsecured = 1883 bez hesla. GX má certifikát podepsaný sám sebou → ověřujeme otiskem (při prvním spojení se
  uloží `cert_sha256`, při další změně se klient odpojí). Heslo zůstává v HA, na server wattery nikdy nejde.
- Portal ID (identita GX): z `N/+/system/0/Serial` (GX ho posílá jako retained), nebo zadané.
- Odběr `N/<portal>/<filtr>` pro filtry z plánu čtení serveru (`readPlan` položky `type: 'mqtt'`, `paths` s '+').
- Keepalive: `R/<portal>/keepalive` s prázdným tělem = GX pošle všechny hodnoty; pak každých 30 s
  `{"keepalive-options": ["suppress-republish"]}` (GX posílá jen změny, bez keepalive do 60 s přestane).
  Klient drží poslední hodnoty (`snapshot`) a serveru je posílá podle `everyS`.

ŽÁDNÝ ZÁPIS: klient umí publikovat jen `R/<portal>/keepalive` (`_publish` cokoli jiného odmítne). Řízení Victronu
přes lokální spojení (`W/…`) přijde samostatně a jen s výslovným souhlasem pro konkrétní instalaci.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import ssl
import struct
import time
import uuid

_LOGGER = logging.getLogger(__name__)

PORT_TLS, PORT_PLAIN = 8883, 1883
MQTT_KEEPALIVE_S = 60          # keepalive spojení MQTT (PINGREQ á polovina)
GX_KEEPALIVE_S = 30            # R/<portal>/keepalive — GX bez něj do 60 s přestane posílat
DISCOVER_S = 10                # čekání na N/+/system/0/Serial
CONNECT_TIMEOUT_S = 10
MAX_PACKET = 256 * 1024        # větší paket od GX = chyba (ochrana paměti)
SERIAL_TOPIC = 'system/0/Serial'
SUPPRESS = {'keepalive-options': ['suppress-republish']}
_PORTAL_RE = re.compile(r'^[0-9a-f]{12}$')
_LEVEL_RE = re.compile(r'^[A-Za-z0-9_+\-.]+$')


class GxError(Exception):
    pass


class GxAuthError(GxError):
    """GX odmítl přihlášení MQTT (heslo / přístup) — jiný port nepomůže."""


class GxCertError(GxError):
    """Certifikát GX se liší od uloženého otisku, nebo při TLS žádný nepřišel — spojení odmítnuto."""


def _vlen(n: int) -> bytes:
    out = bytearray()
    while True:
        b, n = n % 128, n // 128
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _s(v) -> bytes:
    b = v.encode('utf-8') if isinstance(v, str) else v
    return struct.pack('!H', len(b)) + b


def match_filter(flt: str, path: str) -> bool:
    """MQTT filtr ('+' jedna úroveň, '#' zbytek) × cesta (stejně jako deye-monitor edge_victron.match_filter)."""
    f, p = flt.split('/'), path.split('/')
    for k, part in enumerate(f):
        if part == '#':
            return True
        if k >= len(p) or (part != '+' and part != p[k]):
            return False
    return len(f) == len(p)


def valid_filter(flt) -> bool:
    """Filtr z plánu serveru: relativní cesta, '+' jen jako celá úroveň, '#' jen na konci, žádné '..'."""
    if not isinstance(flt, str) or not flt or len(flt) > 200:
        return False
    parts = flt.split('/')
    for k, p in enumerate(parts):
        if p == '#':
            return 0 < k == len(parts) - 1
        if p in ('.', '..') or ('+' in p and p != '+') or not _LEVEL_RE.match(p):
            return False
    return True


def cert_fingerprint(der: bytes) -> str:
    return hashlib.sha256(der or b'').hexdigest()


class GxMqtt:
    """Jedno spojení na broker GX. `start()` připojí, zjistí portal ID a přihlásí odběr; `snapshot(filtry)` vrací
    poslední hodnoty. Spojení drží úloha `run()` (čtení, keepalive); po chybě ji volající spustí znovu."""

    def __init__(self, host: str, port: int = PORT_TLS, *, tls: bool = True, username: str = '',
                 password: str = '', portal: str | None = None, cert_sha256: str | None = None,
                 open_connection=None, clock=time.time, gx_keepalive_s: float = GX_KEEPALIVE_S):
        self.host, self.port, self.tls = host, int(port), bool(tls)
        self.username, self.password = username or '', password or ''
        self.portal = (portal or '').strip().lower() or None
        self.cert_sha256 = (cert_sha256 or '').lower() or None
        self._open = open_connection or asyncio.open_connection
        self._clock = clock
        self._gx_keepalive_s = gx_keepalive_s
        self._r = self._w = None
        self._buf = b''
        self._pid = 0
        self.filters: list[str] = []
        self.values: dict = {}
        self.ts: dict = {}
        self.connected = False
        self.published: list[str] = []        # témata, která klient poslal (test: nikdy W/)
        self._serials: set = set()

    # ── spojení ──
    def _ssl_context(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE          # GX = certifikát podepsaný sám sebou → ověření otiskem
        return ctx

    def _check_pin(self, der: bytes):
        if not der:                                  # bez certifikátu by se uložil otisk prázdna
            raise GxCertError('GX při TLS neposlal certifikát — spojení odmítnuto')
        fp = cert_fingerprint(der)
        if self.cert_sha256 and fp != self.cert_sha256:
            raise GxCertError('certifikát GX se změnil (jiné zařízení v síti?) — spojení odmítnuto')
        self.cert_sha256 = fp

    async def start(self):
        kw = {'ssl': self._ssl_context(), 'server_hostname': self.host} if self.tls else {}
        try:
            self._r, self._w = await asyncio.wait_for(self._open(self.host, self.port, **kw), CONNECT_TIMEOUT_S)
        except (OSError, asyncio.TimeoutError) as e:
            raise GxError(f'GX {self.host}:{self.port} nedostupné: {e}') from e
        if self.tls:
            so = self._w.get_extra_info('ssl_object')
            self._check_pin(so.getpeercert(binary_form=True) if so else b'')
        flags = 0b00000010                                    # clean session
        payload = _s(f'wattery-{uuid.uuid4().hex[:10]}')
        if self.username or self.password:
            flags |= 0b10000000                               # GX: uživatel prázdný, heslo z profilu GX
            payload += _s(self.username)
        if self.password:
            flags |= 0b01000000
            payload += _s(self.password)
        self._send(0x10, _s('MQTT') + bytes([4, flags]) + struct.pack('!H', MQTT_KEEPALIVE_S) + payload)
        t, body = await self._packet(CONNECT_TIMEOUT_S)
        if t != 0x20 or len(body) < 2:
            raise GxError('GX neodpověděl na přihlášení MQTT')
        if body[1] != 0:
            if body[1] in (4, 5):
                raise GxAuthError({4: 'GX odmítl heslo', 5: 'GX: přístup nepovolen'}[body[1]])
            raise GxError(f'GX odmítl spojení ({body[1]})')
        self.connected = True
        if not self.portal:
            await self._discover()
        await self._subscribe(['N/' + self.portal + '/' + SERIAL_TOPIC] +
                              ['N/' + self.portal + '/' + f for f in self.filters])
        self._publish(f'R/{self.portal}/keepalive', b'')     # plný přehled hodnot
        self._last_keepalive = self._last_ping = self._clock()

    async def _discover(self):
        await self._subscribe(['N/+/' + SERIAL_TOPIC])
        end = self._clock() + DISCOVER_S
        while not self._serials and self._clock() < end:
            try:
                await self._handle(await self._packet(max(0.05, end - self._clock())))
            except asyncio.TimeoutError:
                break
        if len(self._serials) != 1:
            raise GxError('portal ID GX se nepodařilo zjistit' if not self._serials
                          else 'na brokeru je víc GX — zadejte portal ID')
        self.portal = next(iter(self._serials))

    async def close(self):
        self.connected = False
        if self._w:
            try:
                self._send(0xE0, b'')
                self._w.close()
                await self._w.wait_closed()
            except (OSError, ssl.SSLError, ConnectionError):
                pass
        self._w = self._r = None

    # ── odběr a stav ──
    async def set_filters(self, filters):
        """Filtry z plánu čtení (relativní k N/<portal>/). Nové se hned přihlásí a GX pošle plný přehled."""
        new = [f for f in dict.fromkeys(filters) if valid_filter(f) and f not in self.filters]
        bad = [f for f in filters if not valid_filter(f)]
        if bad:
            _LOGGER.warning('wattery: neplatné filtry MQTT od serveru přeskočeny: %s', bad[:3])
        self.filters += new
        if new and self.connected:
            await self._subscribe(['N/' + self.portal + '/' + f for f in new])
            self._publish(f'R/{self.portal}/keepalive', b'')

    def snapshot(self, filters) -> dict:
        # Po ztrátě spojení jsou hodnoty staré — neposílat je jako čerstvé (čte se až po znovupřipojení).
        if not self.connected:
            raise GxError('GX není připojené')
        vals ={p: v for p, v in self.values.items() if any(match_filter(f, p) for f in filters)}
        return {'values': vals, 'ts': {p: self.ts.get(p) for p in vals}}

    def read_item(self, item: dict) -> dict:
        """Položka plánu `{type: 'mqtt', name, paths, base?, everyS}` → čtení pro sync (§2.4 v todo-edge-multibrand).
        S `base` (společný prefix) jsou filtry i posílané cesty bez něj — hub ho doplní."""
        base = str(item.get('base') or '')
        snap = self.snapshot([base + f for f in item.get('paths') or []])
        cut = len(base)
        return {'type': 'mqtt', 'name': item.get('name'), 'dev': self.portal,
                'values': {p[cut:]: v for p, v in snap['values'].items()},
                'tsBy': {p[cut:]: t for p, t in snap['ts'].items()}, 'ts': int(self._clock())}

    @staticmethod
    def plan_filters(plan) -> list:
        """Všechny filtry odběru z plánu čtení (položky mqtt, s base)."""
        return [str(it.get('base') or '') + f for it in plan or [] if it.get('type') == 'mqtt'
                for f in it.get('paths') or []]

    # ── smyčka ──
    async def run(self):
        """Čte zprávy a drží keepalive, dokud spojení žije (chyba → GxError / ConnectionError ven)."""
        while self.connected:
            await self.poll(1.0)

    async def poll(self, timeout: float):
        now = self._clock()
        if now - self._last_keepalive >= self._gx_keepalive_s:
            self._publish(f'R/{self.portal}/keepalive', SUPPRESS)
            self._last_keepalive = now
        if now - self._last_ping >= MQTT_KEEPALIVE_S / 2:
            self._send(0xC0, b'')
            self._last_ping = now
        try:
            pkt = await self._packet(timeout)
        except asyncio.TimeoutError:
            return
        await self._handle(pkt)

    async def _handle(self, pkt):
        t, body = pkt
        if t != 0x30 or len(body) < 2:
            return                                         # SUBACK, PINGRESP …
        tl = struct.unpack('!H', body[:2])[0]
        topic = body[2:2 + tl].decode('utf-8', 'replace')
        raw = body[2 + tl:]
        p = topic.split('/', 2)
        if len(p) < 3 or p[0] != 'N':
            return
        if p[2] == SERIAL_TOPIC and _PORTAL_RE.match(p[1]):
            self._serials.add(p[1])
        if self.portal is None or p[1] != self.portal:
            return
        if not raw:
            v = None                                       # prázdné tělo = hodnota zmizela (dbus invalid)
        else:
            try:
                v = json.loads(raw)
                v = v.get('value') if isinstance(v, dict) else v
            except ValueError:
                return
        self.values[p[2]] = v
        self.ts[p[2]] = int(self._clock())

    # ── drát ──
    def _send(self, head: int, body: bytes):
        if not self._w:
            raise GxError('spojení s GX není otevřené')
        self._w.write(bytes([head]) + _vlen(len(body)) + body)

    def _publish(self, topic: str, payload):
        # Jediné, co klient smí poslat: požadavek na hodnoty (keepalive). Žádné W/ — do Victronu se nezapisuje.
        if not self.portal or topic != f'R/{self.portal}/keepalive':
            raise GxError(f'publikování {topic!r} není povolené (klient GX jen čte)')
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode('utf-8')
        self.published.append(topic)
        self._send(0x30, _s(topic) + data)

    async def _subscribe(self, topics):
        for i in range(0, len(topics), 32):
            chunk = topics[i:i + 32]
            self._pid = self._pid % 65535 + 1
            self._send(0x82, struct.pack('!H', self._pid) + b''.join(_s(t) + b'\x00' for t in chunk))
        await self._w.drain()

    def _take(self):
        b = self._buf
        mult, n, i = 1, 0, 1
        while True:
            if i >= len(b):
                return None
            x = b[i]
            n += (x & 0x7F) * mult
            mult *= 128
            i += 1
            if not x & 0x80:
                break
            if i > 4:
                raise GxError('vadný paket MQTT')
        if n > MAX_PACKET:
            raise GxError('příliš velký paket MQTT')
        if len(b) < i + n:
            return None
        self._buf = b[i + n:]
        return b[0] & 0xF0, b[i:i + n]

    async def _packet(self, timeout: float):
        deadline = self._clock() + timeout
        while True:
            pkt = self._take()
            if pkt:
                return pkt
            left = deadline - self._clock()
            if left <= 0:
                raise asyncio.TimeoutError
            chunk = await asyncio.wait_for(self._r.read(65536), left)
            if not chunk:
                self.connected = False
                raise ConnectionError('GX ukončil spojení')
            self._buf += chunk
