"""Profil zápisu wattery — co smí klient lokálního spojení zapsat do zařízení. Profil jsou DATA (JSON s verzí),
ne kód: pojmenované kontroly níž, stejně v klientu Home Assistantu i na serveru wattery (soubor je v obou repech
bajtově stejný, shodu hlídá test).

Formát (todo-edge-multibrand.md §2.5):
  {"id": "deye-lv@1", "brand": "deye", "transport": "modbus", "fc": [16], "maxBlock": 123,
   "identity": {...}, "limits": {"perMin": 15, "perHour": 60, "gapS": 1},
   "write": [položky], "never": [{"addr": 0, "why": "…"}]}
Položka „po registrech“ (bez `fields`): {addr, count=1, min?, max?, check?, …} — každý registr addr..addr+count-1 smí
  být zapsán sám nebo v libovolném bloku s jinými povolenými registry (Deye: TOU 148×12 přes dvě položky).
Položka „blok“ (s `fields`): {name, addr, count, fields: [{off, count=1, min?, max?, check?, …}]} — zápis musí být
  PŘESNĚ tento blok (adresa i délka) a pole musí pokrýt celý blok (Solax VPP).
`enabled: false` = položka se nepoužije (návrh). `persist: true` + `perDay` = registr v EEPROM / flash (denní limit
počítá klient). `never` = registry, které klient nezapíše nikdy, ať profil říká cokoli.
Kontroly: min/max (když jsou) A pojmenovaná `check`:
  range (jen min/max) · hhmm (HHMM 0000–2359) · mask {mask} (žádný jiný bit) · clock {part: ym|dh|ms} (hodiny
  Deye: rok|měsíc, den|hodina, minuta|sekunda) · enum {values} · zero (jen 0) · s32 / u32 {words: lo-hi|hi-lo}
  (dva registry, min/max na celé číslo).
Neznámá kontrola, překryv položek nebo nepokrytý blok = profil neplatný (ValueError) → klient nic nezapíše.
"""
from __future__ import annotations

import json
import os
import re

ID_RE = re.compile(r'^[a-z0-9][a-z0-9-]{0,40}@[0-9]{1,4}$')
MAX_BLOCK = 123                       # FC16: nejvýš 123 registrů
_REG_CHECKS = ('range', 'hhmm', 'mask', 'clock', 'enum', 'zero')
_WIDE_CHECKS = ('s32', 'u32')


def load(profile_id: str, directory: str) -> dict:
    """Profil `<directory>/<id>.json` (jen vestavěné profily; id bez cesty)."""
    if not isinstance(profile_id, str) or not ID_RE.match(profile_id):
        raise ValueError(f'neplatné id profilu {profile_id!r}')
    with open(os.path.join(directory, profile_id + '.json'), encoding='utf-8') as f:
        prof = json.load(f)
    if prof.get('id') != profile_id:
        raise ValueError(f'profil {profile_id}: id v souboru {prof.get("id")!r}')
    return prof


def _int(v, what):
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f'{what}: čekám celé číslo, je {v!r}')
    return v


def _reg_check(spec: dict, what: str):
    """Kontrola jednoho registru → funkce v -> bool."""
    lo = _int(spec['min'], what) if 'min' in spec else None
    hi = _int(spec['max'], what) if 'max' in spec else None
    name = spec.get('check', 'range')
    if name == 'range':
        if lo is None or hi is None:
            raise ValueError(f'{what}: range potřebuje min i max')
        extra = None
    elif name == 'hhmm':
        extra = lambda v: v // 100 <= 23 and v % 100 <= 59
    elif name == 'mask':
        m = _int(spec['mask'], what)
        extra = lambda v: v & ~m == 0
    elif name == 'clock':
        part = spec.get('part')
        if part == 'ym':
            extra = lambda v: 20 <= v >> 8 <= 60 and 1 <= v & 0xFF <= 12        # rok od 2000 | měsíc
        elif part == 'dh':
            extra = lambda v: 1 <= v >> 8 <= 31 and v & 0xFF <= 23              # den | hodina
        elif part == 'ms':
            extra = lambda v: v >> 8 <= 59 and v & 0xFF <= 59                   # minuta | sekunda
        else:
            raise ValueError(f'{what}: clock part {part!r}')
    elif name == 'enum':
        vals = frozenset(_int(x, what) for x in spec['values'])
        extra = lambda v: v in vals
    elif name == 'zero':
        extra = lambda v: v == 0
    else:
        raise ValueError(f'{what}: neznámá kontrola {name!r}')

    def ok(v):
        return (0 <= v <= 0xFFFF and (lo is None or v >= lo) and (hi is None or v <= hi)
                and (extra is None or extra(v)))
    return ok


def _wide_check(spec: dict, what: str):
    """Kontrola 32bitové hodnoty ve dvou registrech → funkce (r0, r1) -> bool."""
    words = spec.get('words')
    if words not in ('lo-hi', 'hi-lo'):
        raise ValueError(f'{what}: {spec["check"]} potřebuje words lo-hi | hi-lo')
    lo, hi = _int(spec['min'], what), _int(spec['max'], what)
    signed = spec['check'] == 's32'

    def ok(r0, r1):
        raw = (r1 << 16 | r0) if words == 'lo-hi' else (r0 << 16 | r1)
        if signed and raw & 0x80000000:
            raw -= 0x100000000
        return lo <= raw <= hi
    return ok


def compile_profile(prof: dict) -> dict:
    """Profil → {'regs': {adresa: kontrola}, 'blocks': {(adresa, délka): [(off, šířka, kontrola)]}, 'never',
    'maxBlock', 'fc', 'limits', 'id'}. Neplatný profil → ValueError."""
    if not isinstance(prof, dict) or not isinstance(prof.get('write'), list):
        raise ValueError('profil bez seznamu write')
    pid = prof.get('id')
    regs, blocks = {}, {}
    for i, e in enumerate(prof['write']):
        what = f'{pid} write[{i}]'
        if not isinstance(e, dict):
            raise ValueError(f'{what}: položka není objekt')
        if e.get('enabled', True) is False:
            continue
        addr, count = _int(e['addr'], what), _int(e.get('count', 1), what)
        if not (0 <= addr and 1 <= count and addr + count <= 0x10000):
            raise ValueError(f'{what}: adresa / počet mimo rozsah')
        if 'fields' not in e:
            ok = _reg_check(e, what)
            for a in range(addr, addr + count):
                if a in regs:
                    raise ValueError(f'{what}: registr {a} je v profilu dvakrát')
                regs[a] = ok
            continue
        if (addr, count) in blocks:
            raise ValueError(f'{what}: blok {addr}×{count} je v profilu dvakrát')
        cover, fields = [False] * count, []
        for j, f in enumerate(e['fields']):
            fw = f'{what} fields[{j}]'
            off, n = _int(f['off'], fw), _int(f.get('count', 1), fw)
            wide = f.get('check') in _WIDE_CHECKS
            if wide:
                if 'count' in f and n != 2:
                    raise ValueError(f'{fw}: {f["check"]} má vždy 2 registry')
                n = 2
            if not (0 <= off and off + n <= count) or any(cover[off:off + n]):
                raise ValueError(f'{fw}: pole mimo blok nebo překryv')
            cover[off:off + n] = [True] * n
            if wide:
                fields.append((off, 2, _wide_check(f, fw)))
            else:
                ok = _reg_check(f, fw)
                fields.extend((off + k, 1, ok) for k in range(n))
        if not all(cover):
            raise ValueError(f'{what}: pole nepokrývají celý blok')
        blocks[(addr, count)] = sorted(fields, key=lambda x: x[0])
    never = frozenset(_int(n['addr'], f'{pid} never') for n in prof.get('never') or [])
    max_block = min(MAX_BLOCK, _int(prof.get('maxBlock', MAX_BLOCK), f'{pid} maxBlock'))
    return {'id': pid, 'regs': regs, 'blocks': blocks, 'never': never, 'maxBlock': max_block,
            'fc': tuple(prof.get('fc') or (16,)), 'limits': dict(prof.get('limits') or {})}


def write_problem(comp: dict, addr, values) -> str | None:
    """None = blok FC16 smí klient zapsat; jinak důvod (registr mimo profil / hodnota mimo meze)."""
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= comp['maxBlock']:
        return 'prázdný nebo příliš dlouhý blok'
    if isinstance(addr, bool) or not isinstance(addr, int):
        return 'neplatná adresa'
    fields = comp['blocks'].get((addr, len(values)))
    for i, v in enumerate(values):
        ok = comp['regs'].get(addr + i) if fields is None else True
        if addr + i in comp['never'] or ok is None:
            return f'registr {addr + i} mimo profil'
        if isinstance(v, bool) or not isinstance(v, int) or not 0 <= v <= 0xFFFF or (fields is None and not ok(v)):
            return f'registr {addr + i}: hodnota {v} mimo meze'
    for off, width, ok in fields or ():
        if not (ok(values[off]) if width == 1 else ok(values[off], values[off + 1])):
            return f'registr {addr + off}: hodnota {values[off:off + width]} mimo meze'
    return None
