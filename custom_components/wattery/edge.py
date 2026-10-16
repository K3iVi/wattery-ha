"""Jádro klienta lokálního spojení wattery — bez závislosti na Home Assistantu (testovatelné samostatně).

Protokol (server monitoring.wattery.cz, `deye/edge.py` + `deye/edge_cmd.py`):
  POST /api/edge/v1/pair  {code, kind, sn, clientVer, label}  → {token, readPlan, nextPollS}
  POST /api/edge/v1/sync  (Bearer token) {client: {ver, backend, ctl}, reads: [{fc, addr, values, ts, sn}], results}
                          → {readPlan, nextPollS, commands, control, accepted, ignored}

Co a jak často číst, určuje server (`readPlan`) — nový registr = změna jen na serveru.

Řízení (od 0.2.0): server pošle v `commands` Modbus příkazy platformy (místo přes Deye cloud):
  {id, type: 'modbus', sn, fc: 3|4|16, addr, count | values, expiresInS}
Čtení (FC03/04) klient provede vždy. Zápis (FC16) jen se zapnutým přepínačem „Řízení z wattery“ (`control_enabled`,
výchozí vypnuto), jen do registrů z DEYE_PROFILE (stejný profil má server — `edge_deye.EDGE_WRITE_PROFILE`, shodu hlídá
test), nejvýš RATE_PER_MIN / RATE_PER_H zápisů s odstupem WRITE_GAP_S a hned po zápisu zpětné čtení. Výsledek
{id, ok, err | values | readBack, rbErr, dur} jde serveru hned dalším sync. Přepínač klient hlásí v každém sync
(`client.ctl`) — vypnutý = server řídí přes Deye cloud jako dřív.

Offline pojistka (od 0.3.0): klientovi s řízením posílá server v každé odpovědi `fallback` =
  {id, validUntil, writes: [{fc: 16, addr, values}]} — neutrální rozvrh (bez prodeje, baterie na rezervě) a do kdy
platí plán, který je ve střídači (TOU se opakuje každý den). Klient ho uloží (přežije restart HA). Když se po
validUntil k serveru nedostane (síť, výpadek serveru — ne odmítnutí 4xx), zapíše neutrál JEDNOU stejnými pojistkami
jako příkazy (přepínač, profil, limity, zpětné čtení) a po obnovení spojení ho nahlásí v `fallbackReport`.
"""
from __future__ import annotations

import asyncio
import logging
import os
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
RATE_PER_MIN, RATE_PER_H = 15, 60      # pojistka proti zahlcení střídače zápisy
MAX_COMMANDS = 8              # příkazů z jedné odpovědi
MAX_RESULTS = 16              # výsledků v jednom sync (stejně jako server)
KEEP_RESULTS = 32             # neodeslané výsledky (server nedostupný) — starší by server stejně zahodil
SEEN_IDS = 64                 # pamatovat si posledních 64 provedených příkazů (žádný se neprovede dvakrát)
FLUSH_ROUNDS = 3              # výsledky hned dalším sync; odpověď může nést další příkaz
# Offline pojistka
FALLBACK_MAX_WRITES = 6
FALLBACK_TTL_S = 120          # zápisy pojistky jdou po sobě s odstupem WRITE_GAP_S — žádný z nich „neprošlý“
FALLBACK_TRIES, FALLBACK_RETRY_S = 3, 300   # ha-solarman neodpověděl → zkusit znovu za 5 min, nejvýš 3×
FALLBACK_SAVE_S = 300         # platnost se posouvá každým sync; ukládat ji nejvýš jednou za 5 min

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


# ── Co smí wattery do střídače zapsat ───────────────────────────────────────
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


def write_problem(addr, values) -> str | None:
    """None = blok FC16 smí klient zapsat; jinak důvod."""
    return _wp.write_problem(PROFILE, addr, values)


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


class EdgeRunner:
    """Smyčka klienta: přečte bloky, které jsou na řadě, pošle je serveru a provede jeho příkazy."""

    def __init__(self, *, read_block: ReadBlock, post: Post, token: str, read_plan, version: str,
                 backend: str, next_poll_s=10, on_status: Callable[['EdgeRunner'], None] | None = None,
                 clock: Callable[[], float] = time.time, write_block: WriteBlock | None = None,
                 sn: str | None = None, sleep: Callable[[float], Awaitable] = asyncio.sleep,
                 state: dict | None = None, on_persist: Callable[[dict], None] | None = None):
        self._read_block, self._post, self._token = read_block, post, token
        self._version, self._backend, self._on_status, self._clock = version, backend, on_status, clock
        self._write_block, self._sn, self._sleep = write_block, (str(sn) if sn else None), sleep
        self._on_persist = on_persist
        self.plan = clean_plan(read_plan)
        self.next_poll_s = self._clamp_poll(next_poll_s)
        self._last_read: dict = {}           # (fc, addr, count) → čas posledního (pokusu o) čtení
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
        self._last_write = 0.0
        self._fast_until = 0.0
        # offline pojistka (stav se ukládá přes on_persist — HA Store, přežije restart)
        self.fallback: dict | None = None    # {id, deadline (čas tohoto klienta), writes}
        self.fallback_applied_at: float | None = None   # kdy naposled zapsána; pojistka s platností do té doby
                                                         # už proběhla (jednou za výpadek, i přes restart)
        self._fb_report: dict | None = None     # hlášení serveru po obnovení spojení
        self._fb_tries, self._fb_retry_at, self._fb_saved = 0, 0.0, 0.0
        self._sync_failed = False            # poslední sync nedošel (síť / 5xx) — jen tehdy pojistka
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

    def state(self) -> dict:
        return {'fallback': self.fallback, 'report': self._fb_report, 'appliedAt': self.fallback_applied_at}

    def _persist(self) -> None:
        if self.fallback:
            self._fb_saved = self.fallback['deadline']
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

    async def tick(self) -> None:
        """Jedno kolo: bloky na řadě → přečíst → poslat (+ výsledky příkazů) → provést příkazy z odpovědi.
        Vyhodí Revoked, když server token nezná."""
        now = self._clock()
        if now < self._backoff_until:
            return
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
        # Bez dat se ozvat jednou za HEARTBEAT_S; se zapnutým řízením na serveru á nextPollS (čekají tam příkazy).
        idle_s = self.next_poll_s - 0.5 if self.server_control else HEARTBEAT_S
        if not reads and not self._pending and now >= self._fast_until and now - self._last_sync < idle_s:
            return
        await self._sync(now, reads)
        for _ in range(FLUSH_ROUNDS):       # výsledky hned — volající na serveru na ně čeká
            if not self._pending or self.status != ONLINE:
                break
            await self._sync(self._clock(), [])
        await self._maybe_fallback(self._clock())

    async def _sync(self, now, reads) -> None:
        body = {'client': {'ver': self._version, 'backend': self._backend,
                           'ctl': 'on' if self.control_enabled else 'off'}, 'reads': reads}
        sent = self._pending[:MAX_RESULTS]
        if sent:
            body['results'] = sent
        report = self._fb_report
        if report:
            body['fallbackReport'] = report
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
        if code == 200:
            self._fb_tries, self._fb_retry_at = 0, 0.0   # spojení je zpět — případný příští výpadek zkouší znovu 3×
            self._accept_fallback(res, now)
            del self._pending[:len(sent)]
            plan = clean_plan(res.get('readPlan'))
            if plan:
                self.plan = plan
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
        what = f'FC{cmd.get("fc")} {cmd.get("addr")}' + (f' = {cmd["values"]}' if cmd.get('values') else '')
        self.last_command = f'{what}: ' + ('OK' if out['ok'] else out.get('err', 'chyba'))
        (_LOGGER.info if cmd.get('fc') == 16 or not out['ok'] else _LOGGER.debug)('wattery: příkaz %s', self.last_command)
        return out

    async def _execute(self, cmd, received) -> dict:
        if cmd.get('type', 'modbus') != 'modbus':
            return {'ok': False, 'err': 'bad-command: neznámý typ'}
        if self._sn and str(cmd.get('sn') or '') != self._sn:
            return {'ok': False, 'err': 'foreign-sn'}
        fc, addr = cmd.get('fc'), cmd.get('addr')
        if not isinstance(fc, int) or not isinstance(addr, int) or isinstance(addr, bool) or not 0 <= addr <= 0xFFFF:
            return {'ok': False, 'err': 'bad-command: fc / addr'}
        try:
            ttl = float(cmd.get('expiresInS') or 10)
        except (TypeError, ValueError):
            ttl = 10.0
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
        if not self.control_enabled:
            return {'ok': False, 'err': 'local-off'}
        prob = write_problem(addr, values)
        if prob:
            return {'ok': False, 'err': f'not-allowed: {prob}'}
        if self._write_block is None:
            return {'ok': False, 'err': 'exec: zápis není k dispozici'}
        now = self._clock()
        self._writes = [t for t in self._writes if now - t < 3600]
        if sum(1 for t in self._writes if now - t < 60) >= RATE_PER_MIN or len(self._writes) >= RATE_PER_H:
            return {'ok': False, 'err': 'rate-limit'}
        gap = self._last_write + WRITE_GAP_S - now
        if gap > 0:
            await self._sleep(gap)
        if self._clock() - received > ttl:
            return {'ok': False, 'err': 'expired'}
        self._writes.append(self._clock())
        try:
            await self._write_block(addr, list(values))
        except Exception as e:  # noqa: BLE001 — ha-solarman: timeout, spojení, odmítnutí
            return {'ok': False, 'err': f'exec: {e}'[:200]}
        finally:
            self._last_write = self._clock()
        out = {'ok': True}
        try:                                 # zpětné čtení — posoudí server (hodiny se mezitím posunou)
            rb = [int(v) for v in await self._read_block(3, addr, len(values))]
            if not _u16_list(rb, len(values)):
                raise ValueError('jiný počet registrů')
            out['readBack'] = rb
        except Exception as e:  # noqa: BLE001
            out['rbErr'] = str(e)[:120] or type(e).__name__
        return out

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
