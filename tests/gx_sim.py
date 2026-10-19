"""Simulátor brokeru Victron GX (dbus-flashmq) pro testy bez sítě a bez Cerba.

Mluví MQTT 3.1.1 na 127.0.0.1 (bez TLS): CONNECT s heslem, SUBSCRIBE s '+'/'#', retained `N/<portal>/system/0/Serial`,
`R/<portal>/keepalive` (prázdné tělo = pošle vše, `suppress-republish` = jen prodlouží), změny hodnot posílá jen
během platného keepalive (60 s), PINGREQ. Každé téma, které klient publikuje, si zapíše do `client_topics`
(test hlídá, že klient bez řízení nikdy nepošle W/). Zápis jako na GX: `W/<portal>/<cesta>` {"value": v} změní
existující cestu (neznámou ignoruje, zapíše do `writes`) a pošle `N/`; `R/<portal>/<cesta>` hodnotu znovu publikuje.
Portal ID a hodnoty jsou smyšlené (veřejné repo).
"""
import asyncio
import json
import struct

PORTAL = 'c0ffee000001'
KEEPALIVE_S = 60


def _vlen(n):
    out = bytearray()
    while True:
        b, n = n % 128, n // 128
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _s(v):
    b = v.encode('utf-8')
    return struct.pack('!H', len(b)) + b


def match(flt, topic):
    f, p = flt.split('/'), topic.split('/')
    for k, part in enumerate(f):
        if part == '#':
            return True
        if k >= len(p) or (part != '+' and part != p[k]):
            return False
    return len(f) == len(p)


def default_tree():
    """Malý souvislý strom: systém, baterie, elektroměr, MPPT se 2 trackery, nastavení ESS/DESS, nabíječka."""
    t = {'system/0/Serial': PORTAL, 'platform/0/Firmware/Installed/Version': 'v3.80',
         'system/0/Dc/Battery/Soc': 64.5, 'system/0/Dc/Battery/Power': -1200.0, 'system/0/Dc/Battery/Voltage': 53.1,
         'system/0/Dc/Battery/Current': -22.6, 'system/0/Dc/Pv/Power': 2100.0,
         'system/0/ActiveBatteryService': 'com.victronenergy.battery/512', 'system/0/Control/ActiveSocLimit': 10,
         'system/0/SystemState/State': 9}
    for k in (1, 2, 3):
        t[f'system/0/Ac/Grid/L{k}/Power'] = 40.0 * k
        t[f'system/0/Ac/Consumption/L{k}/Power'] = 300.0 + k
        t[f'system/0/Ac/PvOnOutput/L{k}/Power'] = 0.0
    t.update({'grid/30/Ac/Energy/Forward': 1000.5, 'grid/30/Ac/Energy/Reverse': 500.25,
              'battery/512/History/ChargedEnergy': 300.0, 'battery/512/History/DischargedEnergy': 280.0,
              'solarcharger/280/Yield/Power': 2100.0, 'solarcharger/280/Pv/0/P': 1200.0,
              'solarcharger/280/Pv/1/P': 900.0, 'solarcharger/280/Yield/User': 900.0,
              'settings/0/Settings/CGwacs/BatteryLife/MinimumSocLimit': 10,
              'settings/0/Settings/CGwacs/Hub4Mode': 2, 'settings/0/Settings/DynamicEss/Mode': 0,
              'evcharger/40/StartStop': 0, 'evcharger/40/SetCurrent': 10})
    for i in range(48):
        for f in ('Start', 'Duration', 'Soc', 'TargetSoc', 'Strategy', 'Restrictions', 'AllowGridFeedIn', 'Flags'):
            t[f'settings/0/Settings/DynamicEss/Schedule/{i}/{f}'] = 0
    return t


class GxBroker:
    def __init__(self, password='heslo', portal=PORTAL, tree=None, clock=None, ssl=None):
        self.ssl = ssl                                      # ssl.SSLContext → TLS jako 8883 na GX
        self.password, self.portal = password, portal
        self.tree = dict(default_tree() if tree is None else tree)
        if tree is None and portal != PORTAL:
            self.tree['system/0/Serial'] = portal
        self.clock = clock or (lambda: asyncio.get_event_loop().time())
        self.clients = []
        self.client_topics = []
        self.writes = []                                    # [(cesta, hodnota)] z W/ (i neznámé cesty)
        self.connects = 0
        self.server = None
        self.port = None

    async def start(self):
        self.server = await asyncio.start_server(self._client, '127.0.0.1', 0, ssl=self.ssl)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        for c in list(self.clients):
            c['w'].close()
        self.server.close()
        await self.server.wait_closed()

    def set(self, path, value):
        """Změna hodnoty na GX → odběratelům s platným keepalive (None = téma zmizí, prázdné tělo)."""
        self.tree[path] = value
        for c in self.clients:
            if self.clock() - c['alive'] < KEEPALIVE_S:
                self._pub(c, path)

    def _pub(self, c, path):
        topic = f'N/{self.portal}/{path}'
        if not any(match(f, topic) for f in c['subs']):
            return
        v = self.tree.get(path)
        body = b'' if v is None else json.dumps({'value': v}).encode()
        c['w'].write(bytes([0x30]) + _vlen(len(_s(topic)) + len(body)) + _s(topic) + body)

    async def _read(self, r):
        h = await r.readexactly(1)
        mult, n = 1, 0
        while True:
            x = (await r.readexactly(1))[0]
            n += (x & 0x7F) * mult
            mult *= 128
            if not x & 0x80:
                break
        return h[0], (await r.readexactly(n) if n else b'')

    async def _client(self, r, w):
        c = {'w': w, 'subs': [], 'alive': -1e9}
        try:
            t, body = await self._read(r)
            if t != 0x10:
                return
            flags = body[7]
            i = 10
            cid_len = struct.unpack('!H', body[i:i + 2])[0]
            i += 2 + cid_len
            user = pw = None
            if flags & 0x80:
                ul = struct.unpack('!H', body[i:i + 2])[0]
                user = body[i + 2:i + 2 + ul].decode()
                i += 2 + ul
            if flags & 0x40:
                pl = struct.unpack('!H', body[i:i + 2])[0]
                pw = body[i + 2:i + 2 + pl].decode()
            ok = (self.password is None) or (pw == self.password)
            w.write(bytes([0x20, 2, 0, 0 if ok else 4]))
            if not ok:
                await w.drain()
                return
            self.connects += 1
            self.clients.append(c)
            while True:
                t, body = await self._read(r)
                kind = t & 0xF0
                if kind == 0x80:                                   # SUBSCRIBE
                    pid = body[:2]
                    i, n = 2, 0
                    new = []
                    while i < len(body):
                        tl = struct.unpack('!H', body[i:i + 2])[0]
                        new.append(body[i + 2:i + 2 + tl].decode())
                        i += 3 + tl
                        n += 1
                    c['subs'] += new
                    w.write(bytes([0x90]) + _vlen(2 + n) + pid + b'\x00' * n)
                    serial = f'N/{self.portal}/system/0/Serial'
                    if any(match(f, serial) for f in new):              # retained
                        self._pub(c, 'system/0/Serial')
                elif kind == 0x30:                                 # PUBLISH od klienta
                    tl = struct.unpack('!H', body[:2])[0]
                    topic = body[2:2 + tl].decode()
                    payload = body[2 + tl:]
                    self.client_topics.append(topic)
                    if topic == f'R/{self.portal}/keepalive':
                        suppress = False
                        if payload:
                            try:
                                suppress = 'suppress-republish' in (json.loads(payload).get('keepalive-options') or [])
                            except ValueError:
                                pass
                        c['alive'] = self.clock()
                        if not suppress:
                            for p in list(self.tree):
                                self._pub(c, p)
                    elif topic.startswith(f'R/{self.portal}/'):          # požadavek na jednu hodnotu
                        path = topic[len(self.portal) + 3:]
                        if path in self.tree:
                            self._pub(c, path)
                    elif topic.startswith(f'W/{self.portal}/'):          # zápis (jen existující cesta)
                        path = topic[len(self.portal) + 3:]
                        try:
                            v = json.loads(payload)['value']
                        except (ValueError, KeyError, TypeError):
                            continue
                        self.writes.append((path, v))
                        if path in self.tree:
                            self.set(path, v)
                elif kind == 0xC0:                                 # PINGREQ
                    w.write(b'\xd0\x00')
                elif kind == 0xE0:
                    return
                await w.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            if c in self.clients:
                self.clients.remove(c)
            w.close()
