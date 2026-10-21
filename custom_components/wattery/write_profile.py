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
  Deye: rok|měsíc, den|hodina, minuta|sekunda) · enum {values} · zero (jen 0) · s16 (jeden registr se znaménkem,
  min/max povinné) · s32 / u32 {words: lo-hi|hi-lo}
  (dva registry, min/max na celé číslo).
Neznámá kontrola, překryv položek nebo nepokrytý blok = profil neplatný (ValueError) → klient nic nezapíše.

Profil MQTT (`"transport": "mqtt"`, Victron GX): položky {path, check: range|enum, min/max | values, int?, persist?}
  místo adres; cesta je relativní k `W/<portal>/` a smí mít vzory `{0..47}` (číslo z rozsahu) a `{n}` (libovolná
  instance). Hodnoty jsou čísla (i desetinná), `int: true` = jen celá. Platí první položka, jejíž vzor cestu pokryje.
  `limits.maxPaths` (povinné) = nejvíc cest v jedné dávce, `limits.persistPerDay` = denní limit trvalých nastavení.
"""
from __future__ import annotations

import json
import math
import os
import re

ID_RE = re.compile(r'^[a-z0-9][a-z0-9-]{0,40}@[0-9]{1,4}$')
MAX_BLOCK = 123                       # FC16: nejvýš 123 registrů
_REG_CHECKS = ('range', 'hhmm', 'mask', 'clock', 'enum', 'zero', 's16')
_WIDE_CHECKS = ('s32', 'u32')
_PATH_TOKEN = re.compile(r'(\{[^{}]*\})')
_PATH_RANGE = re.compile(r'\{([0-9]{1,4})\.\.([0-9]{1,4})\}')
_PATH_CHARS = re.compile(r'[A-Za-z0-9_./{}-]+')
MAX_PATH_LEN = 200


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
    elif name == 's16':
        if lo is None or hi is None:
            raise ValueError(f'{what}: s16 potřebuje min i max')
        slo, shi, lo, hi = lo, hi, None, None          # meze platí pro hodnotu se znaménkem, ne pro u16
        extra = lambda v: slo <= (v - 0x10000 if v & 0x8000 else v) <= shi
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


def _num(v, what):
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise ValueError(f'{what}: čekám číslo, je {v!r}')
    return v


def _path_re(pat, what):
    """Vzor cesty MQTT → regulární výraz (celá cesta)."""
    if (not isinstance(pat, str) or not 0 < len(pat) <= MAX_PATH_LEN or not _PATH_CHARS.fullmatch(pat)
            or pat.startswith('/') or pat.endswith('/') or '//' in pat):
        raise ValueError(f'{what}: neplatná cesta {pat!r}')
    out = ''
    for tok in _PATH_TOKEN.split(pat):
        if tok == '{n}':
            out += '[0-9]+'
        elif tok.startswith('{'):
            m = _PATH_RANGE.fullmatch(tok)
            if not m or not 0 <= int(m[2]) - int(m[1]) <= 1000:
                raise ValueError(f'{what}: neplatný vzor {tok!r}')
            out += '(?:' + '|'.join(str(k) for k in range(int(m[1]), int(m[2]) + 1)) + ')'
        elif '{' in tok or '}' in tok or '..' in tok:
            raise ValueError(f'{what}: neplatný vzor v cestě {pat!r}')
        else:
            out += re.escape(tok)
    return re.compile(out)


def _path_check(spec: dict, what: str):
    """Kontrola hodnoty jedné cesty MQTT → funkce (cesta, hodnota) -> důvod | None."""
    name = spec.get('check', 'range')
    as_int = spec.get('int', False)
    if not isinstance(as_int, bool):
        raise ValueError(f'{what}: int musí být true/false')
    if name == 'range':
        if 'min' not in spec or 'max' not in spec:
            raise ValueError(f'{what}: range potřebuje min i max')
        lo, hi = _num(spec['min'], what), _num(spec['max'], what)
        if lo > hi:
            raise ValueError(f'{what}: min > max')

        def problem(path, v):
            if as_int and v != int(v):
                return f'{path}: hodnota {v} není celé číslo'
            if not lo <= v <= hi:
                return f'{path}: hodnota {v} mimo {lo}–{hi}'
            return None
    elif name == 'enum':
        vals = frozenset(_int(x, what) for x in spec['values'])

        def problem(path, v):
            if not (v == int(v) and int(v) in vals):
                return f'{path}: hodnota {v} není povolená'
            return None
    else:
        raise ValueError(f'{what}: neznámá kontrola cesty {name!r}')
    return problem


def _compile_paths(prof: dict, pid) -> list:
    paths, seen = [], set()
    for i, e in enumerate(prof['write']):
        what = f'{pid} write[{i}]'
        if not isinstance(e, dict):
            raise ValueError(f'{what}: položka není objekt')
        if 'path' not in e or 'addr' in e:
            raise ValueError(f'{what}: profil mqtt má jen položky s cestou (path)')
        if e.get('enabled', True) is False:
            continue
        if e['path'] in seen:
            raise ValueError(f'{what}: cesta {e["path"]} je v profilu dvakrát')
        seen.add(e['path'])
        persist = e.get('persist', False)
        if not isinstance(persist, bool):
            raise ValueError(f'{what}: persist musí být true/false')
        paths.append((_path_re(e['path'], what), _path_check(e, what), persist))
    return paths


def compile_profile(prof: dict) -> dict:
    """Profil → {'regs': {adresa: kontrola}, 'blocks': {(adresa, délka): [(off, šířka, kontrola)]}, 'never',
    'maxBlock', 'fc', 'limits', 'id', 'transport', 'paths': [(vzor, kontrola, persist)], 'maxPaths'}. Neplatný
    profil → ValueError."""
    if not isinstance(prof, dict) or not isinstance(prof.get('write'), list):
        raise ValueError('profil bez seznamu write')
    pid = prof.get('id')
    transport = prof.get('transport', 'modbus')
    limits = dict(prof.get('limits') or {})
    if transport == 'mqtt':
        if 'maxPaths' not in limits:
            raise ValueError(f'{pid}: profil mqtt potřebuje limits.maxPaths')
        max_paths = _int(limits['maxPaths'], f'{pid} maxPaths')
        if max_paths < 1:
            raise ValueError(f'{pid}: maxPaths < 1')
        return {'id': pid, 'transport': transport, 'regs': {}, 'blocks': {}, 'never': frozenset(), 'maxBlock': 0,
                'fc': (), 'limits': limits, 'paths': _compile_paths(prof, pid), 'maxPaths': max_paths}
    if transport != 'modbus':
        raise ValueError(f'{pid}: neznámý přenos {transport!r}')
    regs, blocks = {}, {}
    for i, e in enumerate(prof['write']):
        what = f'{pid} write[{i}]'
        if not isinstance(e, dict):
            raise ValueError(f'{what}: položka není objekt')
        if 'path' in e:
            raise ValueError(f'{what}: cesta (path) patří jen do profilu mqtt')
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
    return {'id': pid, 'transport': transport, 'regs': regs, 'blocks': blocks, 'never': never,
            'maxBlock': max_block, 'fc': tuple(prof.get('fc') or (16,)), 'limits': limits, 'paths': [],
            'maxPaths': 0}


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


def _path_entry(comp: dict, path):
    if isinstance(path, str):
        for rx, problem, persist in comp['paths']:
            if rx.fullmatch(path):
                return problem, persist
    return None


def path_problem(comp: dict, path, value) -> str | None:
    """None = cestu MQTT smí klient zapsat touto hodnotou; jinak důvod."""
    ent = _path_entry(comp, path)
    if ent is None:
        return f'cesta {path} mimo profil'
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return f'{path}: hodnota {value!r} není číslo'
    return ent[0](path, value)


def batch_problem(comp: dict, values) -> str | None:
    """Dávka zápisu MQTT [[cesta, hodnota], …] (pořadí závazné) → None = celá smí; jinak první důvod. Žádná cesta
    dvakrát, nejvýš `maxPaths` cest."""
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= comp['maxPaths']:
        return 'prázdná nebo příliš dlouhá dávka'
    seen = set()
    for item in values:
        if not isinstance(item, (list, tuple)) or len(item) != 2 or not isinstance(item[0], str):
            return 'neplatná položka dávky'
        if item[0] in seen:
            return f'cesta {item[0]} je v dávce dvakrát'
        seen.add(item[0])
        bad = path_problem(comp, item[0], item[1])
        if bad:
            return bad
    return None


def persistent(comp: dict, path) -> bool:
    """Cesta je trvalé nastavení (flash) → počítá se do `limits.persistPerDay`."""
    ent = _path_entry(comp, path)
    return bool(ent and ent[1])
