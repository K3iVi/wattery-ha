"""Jádro klienta lokálního spojení wattery — bez závislosti na Home Assistantu (testovatelné samostatně).

Protokol (server monitoring.wattery.cz, `deye/edge.py`):
  POST /api/edge/v1/pair  {code, kind, sn, clientVer, label}  → {token, readPlan, nextPollS}
  POST /api/edge/v1/sync  (Bearer token) {client, reads: [{fc, addr, values, ts, sn}]}
                          → {readPlan, nextPollS, commands, accepted, ignored}

Co a jak často číst, určuje server (`readPlan`) — nový registr = změna jen na serveru.
Tahle verze jen ČTE: příkazy (`commands`) ignoruje, do střídače nic nezapisuje.
"""
from __future__ import annotations

import asyncio
import logging
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

# Stav pro senzor v HA.
STARTING, ONLINE, ERROR, REVOKED = 'starting', 'online', 'error', 'revoked'

ReadBlock = Callable[[int, int, int], Awaitable[list]]          # (fc, addr, count) → hodnoty u16
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


class EdgeRunner:
    """Smyčka klienta: přečte bloky, které jsou na řadě, a pošle je serveru."""

    def __init__(self, *, read_block: ReadBlock, post: Post, token: str, read_plan, version: str,
                 backend: str, next_poll_s=10, on_status: Callable[['EdgeRunner'], None] | None = None,
                 clock: Callable[[], float] = time.time):
        self._read_block, self._post, self._token = read_block, post, token
        self._version, self._backend, self._on_status, self._clock = version, backend, on_status, clock
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

    async def tick(self) -> None:
        """Jedno kolo: bloky na řadě → přečíst → poslat. Vyhodí Revoked, když server token nezná."""
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
        if not reads and now - self._last_sync < HEARTBEAT_S:
            return
        body = {'client': {'ver': self._version, 'backend': self._backend}, 'reads': reads}
        try:
            code, res = await self._post(SYNC_PATH, body, self._token)
        except Exception as e:  # noqa: BLE001 — síť: zkusit příště, data z tohoto kola propadnou
            self._last_sync = now
            self._set(ERROR, f'server nedostupný: {e}')
            return
        self._last_sync = now
        res = res if isinstance(res, dict) else {}
        if code == 200:
            plan = clean_plan(res.get('readPlan'))
            if plan:
                self.plan = plan
            if 'nextPollS' in res:
                self.next_poll_s = self._clamp_poll(res['nextPollS'])
            if res.get('commands'):
                _LOGGER.info('wattery: server poslal %d příkaz(ů) — tahle verze jen čte, ignoruji',
                             len(res['commands']))
            self.last_sync_at, self.last_accepted = now, res.get('accepted')
            self._set(ONLINE, self.last_error if not reads else None)
        elif code == 401:
            self._set(REVOKED, 'spojení bylo ve wattery odpojeno — spárujte znovu')
            raise Revoked()
        elif code == 429:
            self._backoff_until = now + 60
            self._set(ERROR, 'příliš častá komunikace — zpomaluji')
        else:
            if code in (409, 403):
                self._backoff_until = now + BACKOFF_S
            self._set(ERROR, res.get('message') or res.get('error') or f'HTTP {code}')

    async def run(self, sleep=asyncio.sleep) -> None:
        while True:
            try:
                await self.tick()
            except Revoked:
                raise
            except Exception:  # noqa: BLE001
                _LOGGER.exception('wattery: neočekávaná chyba ve smyčce')
                self._set(ERROR, 'neočekávaná chyba (viz log)')
            await sleep(self.next_poll_s)
