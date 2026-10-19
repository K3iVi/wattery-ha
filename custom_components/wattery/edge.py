"""Jádro klienta lokálního spojení wattery — bez závislosti na Home Assistantu (testovatelné samostatně).

Protokol (server monitoring.wattery.cz, `deye/edge.py` + `deye/edge_cmd.py`):
  POST /api/edge/v1/pair  {code, kind, sn, clientVer, label, caps}  → {token, readPlan, nextPollS, profile?}
  POST /api/edge/v1/sync  (Bearer token) {client: {ver, backend, ctl, caps}, reads, results, fallbackReport?,
                          leaseReport?} → {readPlan, nextPollS, commands, control, accepted, ignored, profile?,
                          fallback | leases, fallbackAck?, leaseAck?}

Co a jak často číst, určuje server (`readPlan`) — nový registr = změna jen na serveru.

Řízení (od 0.2.0): server pošle v `commands` Modbus příkazy platformy (místo přes Deye cloud):
  {id, type: 'modbus', sn, fc: 3|4|16, addr, count | values, expiresInS}
Čtení (FC03/04) klient provede vždy. Zápis (FC16) jen se zapnutým přepínačem „Řízení z wattery“ (`control_enabled`,
výchozí vypnuto), jen do registrů z profilu zápisu (stejný profil má server — `edge_deye.EDGE_WRITE_PROFILE`, shodu
hlídá test), nejvýš RATE_PER_MIN / RATE_PER_H zápisů s odstupem WRITE_GAP_S a hned po zápisu zpětné čtení. Výsledek
{id, ok, err | values | readBack, rbErr, dur} jde serveru hned dalším sync. Přepínač klient hlásí v každém sync
(`client.ctl`) — vypnutý = server řídí přes Deye cloud jako dřív.

Offline pojistka (od 0.3.0): klientovi s řízením posílá server v každé odpovědi `fallback` =
  {id, validUntil, writes: [{fc: 16, addr, values}]} — neutrální rozvrh (bez prodeje, baterie na rezervě) a do kdy
platí plán, který je ve střídači (TOU se opakuje každý den). Klient ho uloží (přežije restart HA). Když se po
validUntil k serveru nedostane (síť, výpadek serveru — ne odmítnutí 4xx), zapíše neutrál JEDNOU stejnými pojistkami
jako příkazy (přepínač, profil, limity, zpětné čtení) a po obnovení spojení ho nahlásí v `fallbackReport`.

Protokol 2 (od 0.4.0, FVE/tasks/todo-edge-multibrand.md §8): klient v pair i každém sync posílá `caps` (přenosy,
vestavěné profily zápisu, vlastnosti `lease` a `readBack`). Starší server je ignoruje → všechno jako 0.3.0.
- Plán a čtení podle `type`: Modbus přes ha-solarman; položky `mqtt` / `ha_entity` přes zapojený přenos (objekt
  s `read_item(položka)` → {type, name, dev?, values, tsBy?, ts}; cesty bez `base` vrací přenos, hub ho doplní).
  Čtení se dělí do více sync pod strop těla serveru (READS_BUDGET).
- Příkazy podle `type`: Modbus jako dřív + `readBack: [{fc, addr, count}]` (co přečíst po zápisu → výsledek
  `readBack: [{fc, addr, count, values}]`); `mqtt` {dev, op: 'write', values: [[cesta, hodnota], …]} → `echo`,
  {op: 'read', paths} → `values`. Zápis prochází profilem, který určí server (`profile`) — ale jen vestavěným a jen
  když identita zařízení podle profilu sedí na spárované (jinak by chybný server mohl poslat zápis jiné značky na
  stejné adresy). Vestavěný Deye profil platí bez ověření jako v 0.3.0.
- `leases` místo `fallback`: úplný seznam platných pojistek. Pojistka stanice v tvaru v1 (zápisy FC16 Deye) jde dál
  cestou `fallback` (stejná logika, hlášení, uložení i senzor). Pojistka příkazu (Solax VPP): do platnosti klient
  opakuje `refresh.cmds` á `everyS` (i bez spojení se serverem); když platnost uplyne a server se neozývá, provede
  jednou `onExpire` a po obnovení spojení pošle `leaseReport`. Pojistku, která v seznamu chybí, zahodí bez onExpire.

Přenosy (od 0.5.0, A5): jedna položka integrace = jedno připojení — Deye přes ha-solarman (Modbus,
`transport_solarman`), Victron GX přímo přes MQTT (`transport_mqtt_gx`), nebo jen čtení z entit integrace, kterou
zákazník už má (`transport_ha_entities`). Položka bez Modbusu (`read_block=None`) ho v `caps` nehlásí a bloky ani
příkazy modbus neprovádí; `caps.integrations` = domény HA, ze kterých položka čte (žádné identifikátory).
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import re
import time
from typing import Awaitable, Callable

_LOGGER = logging.getLogger(__name__)

PAIR_PATH = '/api/edge/v1/pair'
SYNC_PATH = '/api/edge/v1/sync'
RETRY_FAILED_READ_S = 30      # nepovedené čtení bloku zkusit znovu za 30 s (ne až za everyS)
HEARTBEAT_S = 30              # bez nových dat se ozvat aspoň jednou za 30 s
POLL_MIN_S, POLL_MAX_S = 5, 300
BACKOFF_S = 300               # server odmítá (např. střídač už k elektrárně nepatří) → zkusit za 5 min
MAX_BLOCK = 125               # FC03/FC04 čte nejvýš 125 registrů
# Řízení
FAST_POLL_S, FAST_WINDOW_S = 1.5, 20   # po příkazu se chvíli ptát rychle (další blok TOU čeká na výsledek předchozího)
WRITE_GAP_S = 1.0             # mezi dvěma zápisy do střídače aspoň 1 s
RATE_PER_MIN, RATE_PER_H = 15, 60      # pojistka proti zahlcení střídače zápisy (profil může mít přísnější)
MAX_COMMANDS = 8              # příkazů z jedné odpovědi
MAX_RESULTS = 16              # výsledků v jednom sync (stejně jako server)
KEEP_RESULTS = 32             # neodeslané výsledky (server nedostupný) — starší by server stejně zahodil
SEEN_IDS = 64                 # pamatovat si posledních 64 provedených příkazů (žádný se neprovede dvakrát)
FLUSH_ROUNDS = 3              # výsledky hned dalším sync; odpověď může nést další příkaz
MAX_READBACK = 4              # bloků zpětného čtení po jednom zápisu (`readBack` od serveru)
# Offline pojistka
FALLBACK_MAX_WRITES = 6
FALLBACK_TTL_S = 120          # zápisy pojistky jdou po sobě s odstupem WRITE_GAP_S — žádný z nich „neprošlý“
FALLBACK_TRIES, FALLBACK_RETRY_S = 3, 300   # ha-solarman neodpověděl → zkusit znovu za 5 min, nejvýš 3×
FALLBACK_SAVE_S = 300         # platnost se posouvá každým sync; ukládat ji nejvýš jednou za 5 min
# Protokol 2
PROTO = 2
FEATURES = ('lease', 'readBack')
NAMED_TYPES = ('mqtt', 'ha_entity')    # přenosy mimo ha-solarman (zapojuje __init__, jen když jsou nastavené)
DOMAIN_RE = re.compile(r'^[a-z0-9_]{1,40}$')   # doména integrace HA v caps.integrations (stejně jako server)
MAX_READS = 16                # položek čtení v jednom sync (strop serveru protokolu 1)
READS_BUDGET = 48 * 1024      # čtení v jednom sync — strop těla serveru je 64 kB (zbytek na výsledky a hlášení)
MQTT_MAX_PATHS = 1000         # filtrů v jedné položce plánu / cest v jednom příkazu
LEASE_MAX = 4                 # pojistek najednou (stejně jako server)
LEASE_MAX_CMDS = 6            # příkazů v onExpire / refresh jedné pojistky
REFRESH_MIN_S = 30            # obnova příkazu nejčastěji á 30 s, ať server pošle cokoli

# Stav pro senzor v HA.
STARTING, ONLINE, ERROR, REVOKED = 'starting', 'online', 'error', 'revoked'

ReadBlock = Callable[[int, int, int], Awaitable[list]]          # (fc, addr, count) → hodnoty u16
WriteBlock = Callable[[int, list], Awaitable[None]]             # (addr, hodnoty) → zápis FC16
Post = Callable[[str, dict, 'str | None'], Awaitable[tuple]]     # (cesta, tělo, token) → (http kód, json)


class Revoked(Exception):
    """Server token nezná (klient odpojen ve wattery) — je potřeba spárovat znovu."""


def decode_sn(values) -> str | None:
    """Sériové číslo Deye z registrů 3–7 (ASCII, 2 znaky na registr, vyšší bajt první)."""
    if not values:
        return None
    s = ''.join(chr(v >> 8) + chr(v & 0xFF) for v in values)
    return s.replace('\x00', '').strip() or None


def clean_plan(plan) -> list:
    """Z plánu serveru jen bloky, které umíme (modbus FC03/FC04, rozumný rozsah)."""
    out = []
    for b in plan if isinstance(plan, list) else []:
        if not isinstance(b, dict) or b.get('type', 'modbus') != 'modbus':
            continue
        try:
            fc, addr, count = int(b.get('fc', 3)), int(b['addr']), int(b['count'])
            every = max(5, int(b.get('everyS', 60)))
        except (KeyError, TypeError, ValueError):
            continue
        if fc not in (3, 4) or not (0 <= addr <= 0xFFFF) or not (1 <= count <= MAX_BLOCK):
            continue
        out.append({'fc': fc, 'addr': addr, 'count': count, 'everyS': every,
                    'sn': str(b['sn']) if b.get('sn') else None, 'name': str(b.get('name') or addr)})
    return out


def _short_str(v, n) -> bool:
    return isinstance(v, str) and 0 < len(v) <= n


def clean_items(plan, transports=()) -> list:
    """Z plánu serveru položky přenosů mqtt / ha_entity, které má klient zapojené (protokol 2)."""
    out = []
    for b in plan if isinstance(plan, list) else []:
        if not isinstance(b, dict) or b.get('type') not in NAMED_TYPES or b['type'] not in transports:
            continue
        try:
            every = max(5, int(b.get('everyS', 60)))
        except (TypeError, ValueError):
            continue
        if not _short_str(b.get('name'), 40):
            continue
        it = {'type': b['type'], 'name': b['name'], 'everyS': every}
        if b['type'] == 'mqtt':
            paths = b.get('paths')
            if (not isinstance(paths, list) or not 1 <= len(paths) <= MQTT_MAX_PATHS
                    or not all(_short_str(p, 200) for p in paths)):
                continue
            it['paths'] = list(paths)
            it.update({k: b[k] for k in ('dev', 'base') if _short_str(b.get(k), 120)})
        else:
            it.update({k: b[k] for k in ('integration', 'prefix') if _short_str(b.get(k), 60)})
        out.append(it)
    return out


def split_reads(reads, budget=READS_BUDGET, max_n=MAX_READS) -> list:
    """Čtení → dávky pod strop těla sync (nejvýš max_n položek). Položka větší než strop se zahodí — server by jinak
    odmítl celé tělo a nedostal by ani ostatní data."""
    out, cur, size = [], [], 0
    for rd in reads:
        n = len(json.dumps(rd))
        if n > budget:
            _LOGGER.warning('wattery: čtení %s má %s B (víc než %s) — neposílám', rd.get('name') or rd.get('addr'),
                            n, budget)
            continue
        if cur and (size + n > budget or len(cur) >= max_n):
            out.append(cur)
            cur, size = [], 0
        cur.append(rd)
        size += n
    if cur:
        out.append(cur)
    return out


async def _maybe_await(v):
    return await v if inspect.isawaitable(v) else v


def _addr_ok(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= 0xFFFF


def _readback_spec(spec) -> list | None:
    """`readBack` od serveru (co přečíst po zápisu) → [{fc, addr, count}]; [] = bez zadání; None = neplatné."""
    if spec is None:
        return []
    if not isinstance(spec, list) or len(spec) > MAX_READBACK:
        return None
    out = []
    for r in spec:
        if not isinstance(r, dict):
            return None
        fc, addr, count = r.get('fc', 3), r.get('addr'), r.get('count')
        if (fc not in (3, 4) or not _addr_ok(addr) or not isinstance(count, int) or isinstance(count, bool)
                or not 1 <= count <= MAX_BLOCK):
            return None
        out.append({'fc': fc, 'addr': addr, 'count': count})
    return out


def _ttl(cmd) -> float:
    try:
        return float(cmd.get('expiresInS') or 10)
    except (TypeError, ValueError):
        return 10.0


# ── Co smí wattery do zařízení zapsat ───────────────────────────────────────
# Profil zápisu jsou data: profiles/<id>.json + pojmenované kontroly (write_profile.py, bajtově stejný soubor má server).
try:
    from . import write_profile as _wp
except ImportError:                  # edge.py načtený samostatně ze souboru (testy bez Home Assistantu)
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location('wattery_write_profile', os.path.join(os.path.dirname(__file__),
                                                                                'write_profile.py'))
    _wp = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_wp)

PROFILES_DIR = os.path.join(os.path.dirname(__file__), 'profiles')
PROFILE_ID = 'deye-lv@1'
# Deye SG04LP3 / SG05LP3: vše, co platforma wattery zapisuje (spot — okna TOU, proudy, max. prodej, Low Batt, hodiny).
# Blok FC16 projde jen celý; jiný registr klient nezapíše, ať server pošle cokoli.
PROFILE = _wp.compile_profile(_wp.load(PROFILE_ID, PROFILES_DIR))
DEYE_PROFILE = PROFILE['regs']       # adresa → kontrola (registry, které smí klient zapsat)


def _load_profiles(directory) -> tuple:
    """Všechny vestavěné profily (profiles/*.json) → ({id: zkompilovaný}, {id: identita}). Neplatný se vynechá."""
    comp, ident = {}, {}
    for fn in sorted(os.listdir(directory)) if os.path.isdir(directory) else []:
        if not fn.endswith('.json'):
            continue
        pid = fn[:-5]
        try:
            raw = _wp.load(pid, directory)
            comp[pid] = _wp.compile_profile(raw)
            ident[pid] = dict(raw.get('identity') or {})
        except (OSError, ValueError, KeyError, TypeError) as e:
            _LOGGER.warning('wattery: profil zápisu %s je neplatný — nepoužiji ho: %s', pid, e)
    return comp, ident


PROFILES, PROFILE_IDENTITY = _load_profiles(PROFILES_DIR)
PROFILES[PROFILE_ID] = PROFILE


def write_problem(addr, values) -> str | None:
    """None = blok FC16 smí klient zapsat do Deye (vestavěný profil); jinak důvod."""
    return _wp.write_problem(PROFILE, addr, values)


def client_caps(transports=(), modbus=True, integrations=()) -> dict:
    """`caps` pro pair i každý sync: Modbus (ha-solarman; bez něj jen GX / entity) + zapojené přenosy, vestavěné profily
    zápisu, vlastnosti a domény integrací HA, ze kterých položka čte (`integrations`, jen názvy domén)."""
    caps = {'proto': PROTO, 'transports': (['modbus'] if modbus else []) + [t for t in NAMED_TYPES if t in transports],
            'profiles': sorted(PROFILES), 'features': list(FEATURES)}
    doms = sorted({d for d in integrations or () if isinstance(d, str) and DOMAIN_RE.match(d)})
    if doms:
        caps['integrations'] = doms[:16]
    return caps


def _u16_list(v, n) -> bool:
    return (isinstance(v, list) and len(v) == n
            and all(isinstance(x, int) and not isinstance(x, bool) and 0 <= x <= 0xFFFF for x in v))


def clean_fallback(fb) -> dict | None:
    """Offline pojistka ze serveru / z uloženého stavu → {id, writes} nebo None. Projde jen celá: každý zápis FC16
    v profilu (jinak by se při výpadku zapsala jen část neutrálu)."""
    if not isinstance(fb, dict) or not isinstance(fb.get('id'), str) or not 1 <= len(fb['id']) <= 40:
        return None
    writes = fb.get('writes')
    if not isinstance(writes, list) or not 1 <= len(writes) <= FALLBACK_MAX_WRITES:
        return None
    out = []
    for w in writes:
        if not isinstance(w, dict) or w.get('fc', 16) != 16:
            return None
        addr, values = w.get('addr'), w.get('values')
        if not isinstance(addr, int) or isinstance(addr, bool) or write_problem(addr, values):
            return None
        out.append({'addr': addr, 'values': list(values)})
    return {'id': fb['id'], 'writes': out}


def station_fallback(lease, sn) -> dict | None:
    """Pojistka stanice z `leases` → v1 `fallback` {id, validUntil, writes}, když má jeho tvar (jen zápisy FC16 tohoto
    střídače v profilu Deye) — jinak None (pak ji klient drží jako obecnou pojistku)."""
    cmds = lease.get('onExpire') if isinstance(lease, dict) else None
    if not isinstance(cmds, list) or not cmds:
        return None
    writes = []
    for c in cmds:
        if (not isinstance(c, dict) or c.get('type', 'modbus') != 'modbus' or c.get('fc', 16) != 16
                or c.get('readBack') or (sn and str(c.get('sn') or sn) != str(sn))):
            return None
        writes.append({'fc': 16, 'addr': c.get('addr'), 'values': c.get('values')})
    fb = {'id': lease.get('id'), 'validUntil': lease.get('validUntil'), 'writes': writes}
    return fb if clean_fallback(fb) else None


class EdgeRunner:
    """Smyčka klienta: přečte bloky, které jsou na řadě, pošle je serveru a provede jeho příkazy."""

    def __init__(self, *, read_block: ReadBlock | None, post: Post, token: str, read_plan, version: str,
                 backend: str, next_poll_s=10, on_status: Callable[['EdgeRunner'], None] | None = None,
                 clock: Callable[[], float] = time.time, write_block: WriteBlock | None = None,
                 sn: str | None = None, sleep: Callable[[float], Awaitable] = asyncio.sleep,
                 state: dict | None = None, on_persist: Callable[[dict], None] | None = None,
                 transports: dict | None = None, integrations=()):
        # read_block None = položka bez Modbusu (GX přes MQTT, entity integrace) → žádné bloky ani příkazy modbus
        self._read_block, self._post, self._token = read_block, post, token
        self._version, self._backend, self._on_status, self._clock = version, backend, on_status, clock
        self._write_block, self._sn, self._sleep = write_block, (str(sn) if sn else None), sleep
        self._on_persist = on_persist
        self._transports = {k: v for k, v in (transports or {}).items() if k in NAMED_TYPES and v is not None}
        self.caps = client_caps(self._transports, modbus=read_block is not None, integrations=integrations)
        self.plan = clean_plan(read_plan) if read_block else []
        self.items = clean_items(read_plan, self._transports)   # položky mqtt / ha_entity
        self._filters_dirty = bool(self.items)
        self.next_poll_s = self._clamp_poll(next_poll_s)
        self._last_read: dict = {}           # (fc, addr, count) / (typ, jméno) → čas posledního (pokusu o) čtení
        self._last_sync = 0.0
        self._backoff_until = 0.0
        self.status = STARTING
        self.last_error: str | None = None
        self.last_sync_at: float | None = None
        self.last_accepted: int | None = None
        self.read_errors = 0
        # řízení
        self.control_enabled = False         # přepínač „Řízení z wattery“ (HA, výchozí vypnuto)
        self.server_control: str | None = None   # režim řízení na serveru: None / 'test' / 'on'
        self.commands_done = 0
        self.last_command: str | None = None
        self._pending: list = []             # výsledky příkazů k odeslání
        self._done: dict = {}                # id → výsledek posledních SEEN_IDS příkazů
        self._writes: list = []              # časy zápisů (limit za minutu / hodinu)
        self._persist_writes: list = []      # časy zápisů trvalých nastavení (limits.persistPerDay)
        self._last_write = 0.0
        self._fast_until = 0.0
        self.profile_id = PROFILE_ID         # profil zápisu, který určil server (`profile`); bez něj Deye, null = None
                                             # (jen čtení — všechny zápisy odmítnuty)
        self._verified: dict = {}            # profil → identita zařízení sedí (True / False)
        # offline pojistka (stav se ukládá přes on_persist — HA Store, přežije restart)
        self.fallback: dict | None = None    # {id, deadline (čas tohoto klienta), writes}
        self.fallback_applied_at: float | None = None   # kdy naposled zapsána; pojistka s platností do té doby
                                                         # už proběhla (jednou za výpadek, i přes restart)
        self._fb_report: dict | None = None     # hlášení serveru po obnovení spojení
        self._fb_tries, self._fb_retry_at, self._fb_saved = 0, 0.0, 0.0
        self._sync_failed = False            # poslední sync nedošel (síť / 5xx) — jen tehdy pojistka
        # pojistky příkazů (protokol 2): id → {id, source, onExpire, refresh, deadline, appliedAt, …}
        self.leases: dict = {}
        self._lease_reports: list = []
        self._lease_saved: dict = {}
        self.refreshes = 0
        self._load_state(state or {})

    # ── offline pojistka: stav ───────────────────────────────────────────────
    def _load_state(self, st: dict) -> None:
        fb = clean_fallback(st.get('fallback'))
        try:
            if fb:
                fb['deadline'] = float(st['fallback']['deadline'])
                self.fallback = fb
            self.fallback_applied_at = float(st['appliedAt']) if st.get('appliedAt') is not None else None
        except (KeyError, TypeError, ValueError):
            self.fallback = None
        rep = st.get('report')
        self._fb_report = rep if isinstance(rep, dict) and isinstance(rep.get('id'), str) else None
        pid = st.get('profile')
        if isinstance(pid, str) and _wp.ID_RE.match(pid):
            self.profile_id = pid
        elif 'profile' in st and pid is None:
            self.profile_id = None               # uložené „jen čtení“ od serveru (stav 0.3.0 klíč nemá → Deye)
        # Pojistky příkazů: profil se ověří až při provedení (identita zařízení se po restartu teprve čte).
        for x in st.get('leases') if isinstance(st.get('leases'), list) else []:
            lease = self._clean_lease(x, check=False)
            try:
                if lease:
                    applied = x.get('appliedAt')
                    lease.update(deadline=float(x['deadline']), appliedAt=float(applied) if applied is not None else None,
                                 tries=0, retryAt=0.0, nextRefresh=0.0)
                    self.leases[lease['id']] = lease
            except (KeyError, TypeError, ValueError):
                continue
        reps = st.get('leaseReports')
        self._lease_reports = [r for r in reps if isinstance(r, dict) and isinstance(r.get('id'), str)][:LEASE_MAX] \
            if isinstance(reps, list) else []
        self._lease_saved = {i: x['deadline'] for i, x in self.leases.items()}

    def state(self) -> dict:
        return {'fallback': self.fallback, 'report': self._fb_report, 'appliedAt': self.fallback_applied_at,
                'profile': self.profile_id, 'leaseReports': list(self._lease_reports),
                'leases': [{k: x.get(k) for k in ('id', 'source', 'onExpire', 'refresh', 'deadline', 'appliedAt')}
                           for x in self.leases.values()]}

    def _persist(self) -> None:
        if self.fallback:
            self._fb_saved = self.fallback['deadline']
        self._lease_saved = {i: x['deadline'] for i, x in self.leases.items()}
        if self._on_persist:
            try:
                self._on_persist(self.state())
            except Exception:  # noqa: BLE001 — uložení nesmí shodit smyčku
                _LOGGER.exception('wattery: uložení stavu offline pojistky selhalo')

    def _accept_fallback(self, res: dict, now: float) -> None:
        """Pojistka z odpovědi sync. validUntil je čas serveru → převést na hodiny klienta (serverTime)."""
        if 'fallback' not in res:
            return
        fb = clean_fallback(res['fallback'])
        if fb is None:
            if res['fallback'] is not None:
                _LOGGER.warning('wattery: offline pojistka ze serveru neprošla kontrolou (mimo profil) — neukládám')
            if self.fallback is not None:
                self.fallback = None
                self._persist()
            return
        try:
            st = res.get('serverTime')
            offset = now - float(st) if isinstance(st, (int, float)) and not isinstance(st, bool) else 0.0
            fb['deadline'] = float(res['fallback']['validUntil']) + offset
        except (KeyError, TypeError, ValueError):
            return
        changed = self.fallback is None or self.fallback['id'] != fb['id']
        self.fallback = fb
        if changed or abs(fb['deadline'] - self._fb_saved) >= FALLBACK_SAVE_S:
            self._persist()

    async def _maybe_fallback(self, now: float) -> None:
        """Bez spojení se serverem po platnosti plánu → zapsat neutrál (jednou za výpadek)."""
        fb = self.fallback
        if (not fb or not self._sync_failed or now < fb['deadline'] or not self.control_enabled
                or (self.fallback_applied_at is not None and fb['deadline'] <= self.fallback_applied_at)
                or now < self._fb_retry_at):
            return
        _LOGGER.warning('wattery: bez spojení se serverem po platnosti plánu — zapisuji neutrální rozvrh (pojistka)')
        steps = []
        for w in fb['writes']:
            out = await self._execute({'type': 'modbus', 'sn': self._sn, 'fc': 16, 'addr': w['addr'],
                                       'values': w['values'], 'expiresInS': FALLBACK_TTL_S}, self._clock())
            steps.append({'addr': w['addr'], 'values': w['values'],
                          **{k: out[k] for k in ('ok', 'err', 'readBack', 'rbErr') if k in out}})
        self._fb_tries += 1
        retry = any(not s['ok'] and str(s.get('err', '')).startswith(('exec', 'rate-limit')) for s in steps)
        if retry and self._fb_tries < FALLBACK_TRIES:
            self._fb_retry_at = now + FALLBACK_RETRY_S
            _LOGGER.warning('wattery: pojistka se nezapsala celá (%s) — zkusím znovu za %s s',
                            [s.get('err') for s in steps if not s['ok']], FALLBACK_RETRY_S)
            return
        ok = all(s['ok'] for s in steps)
        self._fb_tries, self._fb_retry_at = 0, 0.0
        self.fallback_applied_at = self._clock()
        self._fb_report = {'id': fb['id'], 'at': int(self.fallback_applied_at), 'ok': ok, 'steps': steps}
        self.last_command = 'offline pojistka: ' + ('neutrál zapsán' if ok else 'zapsáno jen částečně')
        self._persist()
        self._set(self.status, self.last_error)

    # ── pojistky příkazů (protokol 2) ────────────────────────────────────────
    def _clean_write(self, c, check=True) -> dict | None:
        """Zápis z pojistky → příkaz bez id / expiresInS, nebo None (jiný tvar, jiné zařízení, mimo profil)."""
        if not isinstance(c, dict):
            return None
        typ = c.get('type', 'modbus')
        if typ == 'modbus':
            spec = _readback_spec(c.get('readBack'))
            if (c.get('fc', 16) != 16 or spec is None or not _addr_ok(c.get('addr'))
                    or not isinstance(c.get('values'), list) or (self._sn and str(c.get('sn') or self._sn) != self._sn)):
                return None
            out = {'type': 'modbus', 'sn': self._sn or c.get('sn'), 'fc': 16, 'addr': c['addr'],
                   'values': list(c['values'])}
            if spec:
                out['readBack'] = spec
        elif typ == 'mqtt':
            vals = c.get('values')
            if c.get('op') != 'write' or not isinstance(vals, list) or not _short_str(c.get('dev'), 120):
                return None
            out = {'type': 'mqtt', 'dev': c['dev'], 'op': 'write',
                   'values': [list(x) if isinstance(x, (list, tuple)) else x for x in vals]}
        else:
            return None
        return out if not check or self._write_check(out) is None else None

    def _clean_lease(self, raw, check=True) -> dict | None:
        """Pojistka z `leases` → {id, source, onExpire, refresh} nebo None. Projde jen celá: každý příkaz je zápis
        tohoto zařízení v profilu; obnova bez trvalých nastavení (flash), nejčastěji á REFRESH_MIN_S."""
        if not isinstance(raw, dict) or not _short_str(raw.get('id'), 40):
            return None

        def cmds(lst):
            if not isinstance(lst, list) or not 1 <= len(lst) <= LEASE_MAX_CMDS:
                return None
            out = [self._clean_write(c, check) for c in lst]
            return None if any(c is None for c in out) else out
        on_expire = cmds(raw.get('onExpire'))
        if on_expire is None:
            return None
        refresh = None
        if raw.get('refresh') is not None:
            r = raw['refresh']
            rc = cmds(r.get('cmds')) if isinstance(r, dict) else None
            every = r.get('everyS') if isinstance(r, dict) else None
            if rc is None or isinstance(every, bool) or not isinstance(every, (int, float)):
                return None
            comp = PROFILES.get(self.profile_id)
            if any(c['type'] == 'mqtt' and comp and comp['transport'] == 'mqtt'
                   and any(_wp.persistent(comp, p) for p, _v in c['values']) for c in rc):
                return None
            refresh = {'cmds': rc, 'everyS': max(REFRESH_MIN_S, float(every))}
        return {'id': raw['id'], 'source': str(raw.get('source') or 'command')[:20], 'onExpire': on_expire,
                'refresh': refresh}

    def _accept_leases(self, res: dict, now: float) -> None:
        """Blok `leases` (server protokolu 2) = úplný seznam platných pojistek. Pojistka stanice v tvaru v1 jde cestou
        `fallback`; ostatní drží `self.leases`. Co v seznamu chybí, klient zahodí bez onExpire."""
        if 'leases' not in res:
            return
        st = res.get('serverTime')
        offset = now - float(st) if isinstance(st, (int, float)) and not isinstance(st, bool) else 0.0
        station, keep = None, {}
        for x in (res['leases'] if isinstance(res['leases'], list) else [])[:LEASE_MAX]:
            if not isinstance(x, dict):
                continue
            if x.get('source') == 'station' and station is None:
                station = station_fallback(x, self._sn)
                if station is not None:
                    continue
            lease = self._clean_lease(x)
            try:
                deadline = float(x['validUntil']) + offset
                if x.get('hardUntil') is not None:
                    deadline = min(deadline, float(x['hardUntil']) + offset)
            except (KeyError, TypeError, ValueError):
                lease = None
            if lease is None:
                _LOGGER.warning('wattery: pojistka %s ze serveru neprošla kontrolou — neukládám', str(x.get('id'))[:40])
                continue
            old = self.leases.get(lease['id']) or {}
            first = now + lease['refresh']['everyS'] if lease['refresh'] else 0.0
            lease.update(deadline=deadline, appliedAt=old.get('appliedAt'), tries=old.get('tries', 0),
                         retryAt=old.get('retryAt', 0.0), nextRefresh=old.get('nextRefresh', first))
            keep[lease['id']] = lease
        changed = set(keep) != set(self.leases) or any(
            abs(x['deadline'] - self._lease_saved.get(i, 0.0)) >= FALLBACK_SAVE_S for i, x in keep.items())
        self.leases = keep
        if changed:
            self._persist()
        self._accept_fallback({'fallback': station, 'serverTime': st}, now)

    async def _maybe_refresh(self, now: float) -> None:
        """Obnova příkazu (Solax VPP): do platnosti pojistky opakovat zápis á everyS — i bez spojení se serverem.
        Do limitu zápisů za minutu / hodinu se nepočítá (strop dává REFRESH_MIN_S a LEASE_MAX)."""
        for lease in list(self.leases.values()):
            ref = lease.get('refresh')
            if not ref or now >= lease['deadline'] or now < lease['nextRefresh'] or not self.control_enabled:
                continue
            lease['nextRefresh'] = now + ref['everyS']
            outs = [await self._execute(dict(c, expiresInS=FALLBACK_TTL_S), self._clock(), refresh=True)
                    for c in ref['cmds']]
            self.refreshes += 1
            bad = [str(o.get('err')) for o in outs if not o['ok']]
            if bad:
                self.last_command = f'obnova {lease["id"]}: ' + '; '.join(bad)[:150]
                _LOGGER.warning('wattery: %s', self.last_command)

    async def _maybe_leases(self, now: float) -> None:
        """Bez spojení se serverem po platnosti pojistky příkazu → jednou onExpire (Solax: konec VPP), po obnovení
        spojení `leaseReport`. Stejné podmínky a opakování jako offline pojistka stanice."""
        for lease in list(self.leases.values()):
            if (not self._sync_failed or now < lease['deadline'] or not self.control_enabled
                    or (lease['appliedAt'] is not None and lease['deadline'] <= lease['appliedAt'])
                    or now < lease['retryAt']):
                continue
            _LOGGER.warning('wattery: bez spojení se serverem po platnosti pojistky %s — provádím ukončení', lease['id'])
            steps = []
            for c in lease['onExpire']:
                out = await self._execute(dict(c, expiresInS=FALLBACK_TTL_S), self._clock())
                steps.append({**{k: c[k] for k in ('type', 'fc', 'addr', 'dev', 'values') if k in c},
                              **{k: out[k] for k in ('ok', 'err', 'readBack', 'rbErr', 'echo') if k in out}})
            lease['tries'] += 1
            retry = any(not s['ok'] and str(s.get('err', '')).startswith(('exec', 'rate-limit')) for s in steps)
            if retry and lease['tries'] < FALLBACK_TRIES:
                lease['retryAt'] = now + FALLBACK_RETRY_S
                continue
            ok = all(s['ok'] for s in steps)
            lease['tries'], lease['retryAt'], lease['appliedAt'] = 0, 0.0, self._clock()
            self._lease_reports.append({'id': lease['id'], 'source': lease['source'], 'at': int(lease['appliedAt']),
                                        'ok': ok, 'steps': steps})
            del self._lease_reports[:-LEASE_MAX]
            self.last_command = f'pojistka {lease["id"]}: ' + ('ukončení provedeno' if ok else 'provedeno jen částečně')
            self._persist()
            self._set(self.status, self.last_error)

    # ── profil zápisu ────────────────────────────────────────────────────────
    async def _ensure_profile(self) -> None:
        """Profil od serveru platí, jen když identita zařízení podle profilu sedí na spárované (SN / portal ID)."""
        pid = self.profile_id
        if pid == PROFILE_ID or pid not in PROFILES or self._verified.get(pid) is not None:
            return
        ident = PROFILE_IDENTITY.get(pid) or {}
        try:
            if PROFILES[pid]['transport'] == 'mqtt':
                tr = self._transports.get('mqtt')
                ok = bool(tr is not None and self._sn and str(getattr(tr, 'portal', '') or '') == self._sn)
            elif ident.get('decode') == 'ascii' and self._sn and self._read_block:
                vals = await self._read_block(int(ident.get('fc', 3)), int(ident['addr']), int(ident['count']))
                ok = decode_sn([int(v) for v in vals]) == self._sn
            else:
                ok = False
        except Exception as e:  # noqa: BLE001 — čtení identity se zkusí znovu v dalším kole
            _LOGGER.debug('wattery: identitu pro profil %s se nepodařilo přečíst: %s', pid, e)
            return
        self._verified[pid] = ok
        if not ok:
            _LOGGER.warning('wattery: profil zápisu %s nesedí na zařízení %s — zápisy odmítám', pid, self._sn)

    def _comp(self, typ) -> tuple:
        """(zkompilovaný profil pro přenos, důvod když není)."""
        pid = self.profile_id
        if pid is None:
            return None, 'zařízení jen ke čtení (server: profile null)'
        comp = PROFILES.get(pid)
        if comp is None:
            return None, f'profil {pid} v klientu není'
        if comp['transport'] != typ:
            return None, f'profil {pid} není pro přenos {typ}'
        if pid != PROFILE_ID and not self._verified.get(pid):
            return None, f'profil {pid}: identita zařízení neověřená'
        return comp, None

    def _write_check(self, cmd) -> str | None:
        """Zápis (Modbus FC16 / dávka mqtt) projde profilem zařízení? None = ano, jinak důvod."""
        typ = cmd.get('type', 'modbus')
        comp, why = self._comp(typ)
        if comp is None:
            return why
        if typ == 'modbus':
            return _wp.write_problem(comp, cmd.get('addr'), cmd.get('values'))
        return _wp.batch_problem(comp, cmd.get('values'))

    def _rate_ok(self, comp, now) -> bool:
        def lim(key, default):
            try:
                return min(default, int((comp.get('limits') or {}).get(key) or default))
            except (TypeError, ValueError):
                return default
        self._writes = [t for t in self._writes if now - t < 3600]
        return (sum(1 for t in self._writes if now - t < 60) < lim('perMin', RATE_PER_MIN)
                and len(self._writes) < lim('perHour', RATE_PER_H))

    @staticmethod
    def _clamp_poll(v) -> float:
        try:
            return float(min(POLL_MAX_S, max(POLL_MIN_S, float(v))))
        except (TypeError, ValueError):
            return 10.0

    def _set(self, status, error=None):
        self.status, self.last_error = status, error
        if self._on_status:
            try:
                self._on_status(self)
            except Exception:  # noqa: BLE001 — senzor nesmí shodit smyčku
                _LOGGER.exception('wattery: chyba při aktualizaci stavu')

    def set_control(self, enabled: bool) -> None:
        """Přepínač „Řízení z wattery“. Změnu nahlásit serveru hned v příštím kole (i bez dat), ať podle ní
        směruje příkazy (vypnuto → Deye cloud)."""
        if bool(enabled) != self.control_enabled:
            self.control_enabled = bool(enabled)
            self._last_sync = 0.0

    def poll_s(self) -> float:
        """Za jak dlouho další kolo: po příkazu rychle, jinak podle serveru."""
        return FAST_POLL_S if self._clock() < self._fast_until else self.next_poll_s

    async def _apply_filters(self) -> None:
        """Nové položky plánu → odběr u přenosu (GX: N/<portal>/<filtr>)."""
        if not self._filters_dirty:
            return
        self._filters_dirty = False
        for typ, tr in self._transports.items():
            fn = getattr(tr, 'set_filters', None)
            if fn is None:
                continue
            paths = [str(it.get('base') or '') + p for it in self.items if it['type'] == typ for p in it.get('paths', [])]
            try:
                await _maybe_await(fn(paths))
            except Exception as e:  # noqa: BLE001 — přenos zkusí odběr znovu v dalším kole
                self._filters_dirty = True
                _LOGGER.debug('wattery: odběr %s selhal: %s', typ, e)

    async def _read_item(self, it) -> dict | None:
        """Položka plánu mqtt / ha_entity → čtení pro sync, nebo None (přenos zatím nic nemá)."""
        rd = await _maybe_await(self._transports[it['type']].read_item(it))
        if not isinstance(rd, dict) or not isinstance(rd.get('values'), dict) or not rd['values']:
            return None
        out = {'type': it['type'], 'name': it['name'], 'values': rd['values'], 'ts': int(self._clock())}
        if it['type'] == 'mqtt':
            tsb = rd.get('tsBy') if isinstance(rd.get('tsBy'), dict) else {}
            out['tsBy'] = {k: int(v) for k, v in tsb.items() if isinstance(v, (int, float)) and not isinstance(v, bool)}
            if rd.get('dev') or it.get('dev'):
                out['dev'] = str(rd.get('dev') or it.get('dev'))
        elif _short_str(rd.get('prefix'), 60):
            out['prefix'] = rd['prefix']
        return out

    async def tick(self) -> None:
        """Jedno kolo: bloky na řadě → přečíst → poslat (+ výsledky příkazů) → provést příkazy z odpovědi.
        Vyhodí Revoked, když server token nezná."""
        now = self._clock()
        if now < self._backoff_until:
            return
        await self._apply_filters()
        reads = []
        for b in self.plan:
            key = (b['fc'], b['addr'], b['count'])
            if now - self._last_read.get(key, 0) < b['everyS']:
                continue
            try:
                vals = [int(v) for v in await self._read_block(b['fc'], b['addr'], b['count'])]
                if len(vals) != b['count'] or not all(0 <= v <= 0xFFFF for v in vals):
                    raise ValueError(f'čekáno {b["count"]} registrů 0–65535, přišlo {len(vals)}')
            except Exception as e:  # noqa: BLE001 — výpadek čtení jednoho bloku nesmí zastavit ostatní
                self.read_errors += 1
                self._last_read[key] = now - b['everyS'] + RETRY_FAILED_READ_S
                _LOGGER.debug('wattery: čtení bloku %s (%s/%s) selhalo: %s', b['name'], b['addr'], b['count'], e)
                self.last_error = f'čtení {b["name"]}: {e}'
                continue
            self._last_read[key] = now
            rd = {'fc': b['fc'], 'addr': b['addr'], 'values': vals, 'ts': int(self._clock())}
            if b['sn']:
                rd['sn'] = b['sn']
            reads.append(rd)
        for it in self.items:
            key = (it['type'], it['name'])
            if now - self._last_read.get(key, 0) < it['everyS']:
                continue
            try:
                rd = await self._read_item(it)
            except Exception as e:  # noqa: BLE001
                rd, self.last_error = None, f'čtení {it["name"]}: {e}'
                self.read_errors += 1
            if rd is None:
                self._last_read[key] = now - it['everyS'] + RETRY_FAILED_READ_S
                continue
            self._last_read[key] = now
            reads.append(rd)
        # Bez dat se ozvat jednou za HEARTBEAT_S; se zapnutým řízením na serveru á nextPollS (čekají tam příkazy).
        idle_s = self.next_poll_s - 0.5 if self.server_control else HEARTBEAT_S
        if (not reads and not self._pending and not self._lease_reports and now >= self._fast_until
                and now - self._last_sync < idle_s):
            return
        chunks = split_reads(reads) or [[]]
        await self._sync(now, chunks[0])
        for more in chunks[1:]:              # čtení přes strop těla → další sync hned po něm
            if self.status != ONLINE:
                break
            await self._sync(self._clock(), more)
        for _ in range(FLUSH_ROUNDS):       # výsledky hned — volající na serveru na ně čeká
            if not self._pending or self.status != ONLINE:
                break
            await self._sync(self._clock(), [])
        await self._offline_duties(self._clock())

    async def _offline_duties(self, now) -> None:
        await self._ensure_profile()
        await self._maybe_refresh(now)
        await self._maybe_fallback(now)
        await self._maybe_leases(now)

    async def _sync(self, now, reads) -> None:
        body = {'client': {'ver': self._version, 'backend': self._backend,
                           'ctl': 'on' if self.control_enabled else 'off', 'caps': self.caps}, 'reads': reads}
        sent = self._pending[:MAX_RESULTS]
        if sent:
            body['results'] = sent
        report = self._fb_report
        if report:
            body['fallbackReport'] = report
        lrep = self._lease_reports[0] if self._lease_reports else None
        if lrep:
            body['leaseReport'] = lrep
        try:
            code, res = await self._post(SYNC_PATH, body, self._token)
        except Exception as e:  # noqa: BLE001 — síť: zkusit příště (čtení z tohoto kola propadnou, výsledky ne)
            self._last_sync = now
            self._sync_failed = True
            self._set(ERROR, f'server nedostupný: {e}')
            return
        self._last_sync = now
        self._sync_failed = code >= 500       # jen výpadek serveru se počítá jako „bez spojení“ (ne odmítnutí 4xx)
        res = res if isinstance(res, dict) else {}
        if report and code in (200, 400):    # přijato (fallbackAck), nebo server hlášení nechce — neposílat dokola
            if res.get('fallbackAck') != report['id']:
                _LOGGER.info('wattery: server hlášení offline pojistky nepotvrdil (HTTP %s)', code)
            self._fb_report = None
            self._persist()
        if lrep and code in (200, 400):
            if res.get('leaseAck') != lrep['id']:
                _LOGGER.info('wattery: server hlášení pojistky %s nepotvrdil (HTTP %s)', lrep['id'], code)
            if self._lease_reports and self._lease_reports[0] is lrep:
                self._lease_reports.pop(0)
            self._persist()
        if code == 200:
            self._fb_tries, self._fb_retry_at = 0, 0.0   # spojení je zpět — případný příští výpadek zkouší znovu 3×
            pid = res.get('profile')
            if 'profile' in res and pid is None and self.profile_id is not None:
                self.profile_id = None           # server: zařízení jen ke čtení (baterie JK, Solax mimo G4) → žádné zápisy
                self._persist()
            elif isinstance(pid, str) and pid != self.profile_id and _wp.ID_RE.match(pid):
                self.profile_id = pid
                self._persist()
            await self._ensure_profile()
            self._accept_leases(res, now)
            self._accept_fallback(res, now)
            del self._pending[:len(sent)]
            plan, items = clean_plan(res.get('readPlan')), clean_items(res.get('readPlan'), self._transports)
            plan = plan if self._read_block else []
            if plan or items:
                self._filters_dirty = self._filters_dirty or items != self.items
                self.plan, self.items = plan, items
            if 'nextPollS' in res:
                self.next_poll_s = self._clamp_poll(res['nextPollS'])
            self.server_control = res.get('control') if res.get('control') in ('test', 'on') else None
            self.last_sync_at, self.last_accepted = now, res.get('accepted')
            self._set(ONLINE, self.last_error if not reads else None)
            cmds = res.get('commands')
            if isinstance(cmds, list) and cmds:
                received = self._clock()
                self._fast_until = received + FAST_WINDOW_S
                for cmd in cmds[:MAX_COMMANDS]:
                    r = await self._command(cmd, received)
                    if r is not None:
                        self._pending.append(r)
                del self._pending[:-KEEP_RESULTS]
        elif code == 401:
            self._set(REVOKED, 'spojení bylo ve wattery odpojeno — spárujte znovu')
            raise Revoked()
        elif code == 429:
            self._backoff_until = now + 60
            self._set(ERROR, 'příliš častá komunikace — zpomaluji')
        else:
            if code in (409, 403):
                self._backoff_until = now + BACKOFF_S
            if code == 400:
                self._pending.clear()        # server tělo odmítl — výsledky by se odmítaly pořád dokola
            self._set(ERROR, res.get('message') or res.get('error') or f'HTTP {code}')

    async def _command(self, cmd, received) -> dict | None:
        """Jeden příkaz serveru → výsledek pro `results` (None = příkaz bez id, není komu odpovědět)."""
        cid = cmd.get('id') if isinstance(cmd, dict) else None
        if not isinstance(cid, str) or not cid or len(cid) > 40:
            _LOGGER.warning('wattery: příkaz bez platného id — ignoruji')
            return None
        if cid in self._done:                # už provedený → jen znovu výsledek, do střídače podruhé nic
            return self._done[cid]
        try:
            out = await self._execute(cmd, received)
        except Exception as e:  # noqa: BLE001
            _LOGGER.exception('wattery: chyba při provádění příkazu %s', cid)
            out = {'ok': False, 'err': f'exec: {e}'[:200]}
        out = {'id': cid, **out, 'dur': round(self._clock() - received, 2)}
        self._done[cid] = out
        while len(self._done) > SEEN_IDS:
            self._done.pop(next(iter(self._done)))
        self.commands_done += 1
        if cmd.get('type') == 'mqtt':
            what = f'MQTT {cmd.get("op")} ({len(cmd.get("values") or cmd.get("paths") or [])} cest)'
        else:
            what = f'FC{cmd.get("fc")} {cmd.get("addr")}' + (f' = {cmd["values"]}' if cmd.get('values') else '')
        self.last_command = f'{what}: ' + ('OK' if out['ok'] else out.get('err', 'chyba'))
        is_write = cmd.get('fc') == 16 or cmd.get('op') == 'write'
        (_LOGGER.info if is_write or not out['ok'] else _LOGGER.debug)('wattery: příkaz %s', self.last_command)
        return out

    async def _execute(self, cmd, received, refresh=False) -> dict:
        typ = cmd.get('type', 'modbus')
        if typ == 'mqtt':
            return await self._execute_mqtt(cmd, received, refresh)
        if typ != 'modbus':
            return {'ok': False, 'err': 'bad-command: neznámý typ'}
        if self._read_block is None:
            return {'ok': False, 'err': 'bad-command: přenos modbus není zapojený'}
        if self._sn and str(cmd.get('sn') or '') != self._sn:
            return {'ok': False, 'err': 'foreign-sn'}
        fc, addr = cmd.get('fc'), cmd.get('addr')
        if not isinstance(fc, int) or not isinstance(addr, int) or isinstance(addr, bool) or not 0 <= addr <= 0xFFFF:
            return {'ok': False, 'err': 'bad-command: fc / addr'}
        ttl = _ttl(cmd)
        if fc in (3, 4):                     # čtení: vždy (nic nemění)
            count = cmd.get('count')
            if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= MAX_BLOCK:
                return {'ok': False, 'err': 'bad-command: count'}
            if self._clock() - received > ttl:
                return {'ok': False, 'err': 'expired'}
            try:
                vals = [int(v) for v in await self._read_block(fc, addr, count)]
            except Exception as e:  # noqa: BLE001 — ha-solarman: timeout, spojení
                return {'ok': False, 'err': f'exec: {e}'[:200]}
            if not _u16_list(vals, count):
                return {'ok': False, 'err': 'exec: čtení vrátilo jiný počet registrů'}
            return {'ok': True, 'values': vals}
        if fc != 16:
            return {'ok': False, 'err': f'bad-command: FC{fc}'}
        values = cmd.get('values')
        spec = _readback_spec(cmd.get('readBack'))
        if spec is None:
            return {'ok': False, 'err': 'bad-command: readBack'}
        if not self.control_enabled:
            return {'ok': False, 'err': 'local-off'}
        prob = self._write_check(cmd)
        if prob:
            return {'ok': False, 'err': f'not-allowed: {prob}'}
        if self._write_block is None:
            return {'ok': False, 'err': 'exec: zápis není k dispozici'}
        now = self._clock()
        if not refresh and not self._rate_ok(self._comp('modbus')[0], now):
            return {'ok': False, 'err': 'rate-limit'}
        gap = self._last_write + WRITE_GAP_S - now
        if gap > 0:
            await self._sleep(gap)
        if self._clock() - received > ttl:
            return {'ok': False, 'err': 'expired'}
        if not refresh:
            self._writes.append(self._clock())
        try:
            await self._write_block(addr, list(values))
        except Exception as e:  # noqa: BLE001 — ha-solarman: timeout, spojení, odmítnutí
            return {'ok': False, 'err': f'exec: {e}'[:200]}
        finally:
            self._last_write = self._clock()
        out = {'ok': True}
        if refresh:                          # obnova: stav hlídá server ze čtení, zpětné čtení by jen zdržovalo
            return out
        try:                                 # zpětné čtení — posoudí server (hodiny se mezitím posunou)
            if spec:                         # co přečíst určil server (Solax: jiná mapa čtení než zápisu)
                out['readBack'] = []
                for r in spec:
                    rb = [int(v) for v in await self._read_block(r['fc'], r['addr'], r['count'])]
                    if not _u16_list(rb, r['count']):
                        raise ValueError('jiný počet registrů')
                    out['readBack'].append(dict(r, values=rb))
            else:
                rb = [int(v) for v in await self._read_block(3, addr, len(values))]
                if not _u16_list(rb, len(values)):
                    raise ValueError('jiný počet registrů')
                out['readBack'] = rb
        except Exception as e:  # noqa: BLE001
            out.pop('readBack', None)
            out['rbErr'] = str(e)[:120] or type(e).__name__
        return out

    async def _execute_mqtt(self, cmd, received, refresh=False) -> dict:
        """Příkaz mqtt (Victron GX): čtení cest vždy; dávka zápisu jen s přepínačem, celá v profilu, v limitech."""
        tr = self._transports.get('mqtt')
        if tr is None:
            return {'ok': False, 'err': 'bad-command: přenos mqtt není zapojený'}
        dev = str(getattr(tr, 'portal', '') or self._sn or '')
        if dev and str(cmd.get('dev') or '') != dev:
            return {'ok': False, 'err': 'foreign-sn'}
        ttl, op = _ttl(cmd), cmd.get('op')
        if op == 'read':
            paths = cmd.get('paths')
            if not isinstance(paths, list) or not 1 <= len(paths) <= MQTT_MAX_PATHS or not all(
                    _short_str(p, 200) for p in paths):
                return {'ok': False, 'err': 'bad-command: paths'}
            if self._clock() - received > ttl:
                return {'ok': False, 'err': 'expired'}
            try:
                if hasattr(tr, 'read_paths'):
                    vals = await _maybe_await(tr.read_paths(list(paths)))
                else:
                    vals = (await _maybe_await(tr.read_item({'type': 'mqtt', 'name': 'cmd', 'paths': list(paths)})))['values']
            except Exception as e:  # noqa: BLE001
                return {'ok': False, 'err': f'exec: {e}'[:200]}
            return {'ok': True, 'values': vals if isinstance(vals, dict) else {}}
        if op != 'write':
            return {'ok': False, 'err': 'bad-command: op'}
        if not self.control_enabled:
            return {'ok': False, 'err': 'local-off'}
        prob = self._write_check(cmd)
        if prob:
            return {'ok': False, 'err': f'not-allowed: {prob}'}
        if not callable(getattr(tr, 'write', None)):
            return {'ok': False, 'err': 'exec: přenos mqtt neumí zápis'}
        comp, now = self._comp('mqtt')[0], self._clock()
        if not refresh and not self._rate_ok(comp, now):
            return {'ok': False, 'err': 'rate-limit'}
        pairs = [[p, v] for p, v in cmd['values']]
        if any(_wp.persistent(comp, p) for p, _v in pairs):   # trvalá nastavení (flash): denní limit profilu
            self._persist_writes = [t for t in self._persist_writes if now - t < 86400]
            per_day = (comp.get('limits') or {}).get('persistPerDay')
            if isinstance(per_day, int) and len(self._persist_writes) >= per_day:
                return {'ok': False, 'err': 'rate-limit: trvalá nastavení za den'}
            self._persist_writes.append(now)
        gap = self._last_write + WRITE_GAP_S - now
        if gap > 0:
            await self._sleep(gap)
        if self._clock() - received > ttl:
            return {'ok': False, 'err': 'expired'}
        if not refresh:
            self._writes.append(self._clock())
        try:
            echo = await _maybe_await(tr.write(pairs))
        except Exception as e:  # noqa: BLE001
            return {'ok': False, 'err': f'exec: {e}'[:200]}
        finally:
            self._last_write = self._clock()
        return {'ok': True, 'echo': echo if isinstance(echo, dict) else {}}

    async def run(self, sleep=None) -> None:
        sleep = sleep or self._sleep
        while True:
            try:
                await self.tick()
            except Revoked:
                raise
            except Exception:  # noqa: BLE001
                _LOGGER.exception('wattery: neočekávaná chyba ve smyčce')
                self._set(ERROR, 'neočekávaná chyba (viz log)')
            await sleep(self.poll_s())
