"""Testy jádra (bez Home Assistantu): python tests/test_edge.py

Čtení, sync, řízení (příkazy, přepínač, profil, limity, zpětné čtení), offline pojistka a shoda profilu zápisu se serverem
(když je vedle repa checkout deye-monitor). Volitelně E2E proti běžícímu serveru deye-monitor s DEYE_EDGE=1 (lokálně!):
  python tests/test_edge.py --url http://127.0.0.1:3000 --code ABCD-EFGH --sn 2203084752
"""
import argparse
import asyncio
import importlib.util
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    'wattery_edge', os.path.join(_HERE, '..', 'custom_components', 'wattery', 'edge.py'))
edge = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(edge)

FAILS = []


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


PLAN = [{'name': 'live', 'type': 'modbus', 'fc': 3, 'addr': 586, 'count': 3, 'everyS': 30, 'sn': '123'},
        {'name': 'sn', 'type': 'modbus', 'fc': 3, 'addr': 3, 'count': 5, 'everyS': 3600},
        {'name': 'cmd', 'type': 'ha', 'service': 'switch.turn_on'},            # neznámý typ → přeskočit
        {'name': 'big', 'type': 'modbus', 'fc': 3, 'addr': 0, 'count': 200}]   # přes 125 → přeskočit


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


async def unit():
    check('decode_sn ASCII', edge.decode_sn([0x3235, 0x3039, 0x3036, 0x3532, 0x3136]) == '2509065216')
    check('decode_sn prázdné', edge.decode_sn([0, 0]) is None)
    cp = edge.clean_plan(PLAN)
    check('clean_plan: jen modbus a ≤125', [b['name'] for b in cp] == ['live', 'sn'], cp)

    clock, reads_log, posts = Clock(), [], []
    fail_addr = set()

    async def read_block(fc, addr, count):
        reads_log.append((fc, addr, count))
        if addr in fail_addr:
            raise TimeoutError('timeout')
        return list(range(addr, addr + count))

    responses = []

    async def post(path, body, token):
        posts.append((path, body, token))
        return responses.pop(0) if responses else (200, {'ok': True, 'nextPollS': 10, 'accepted': len(body['reads'])})

    statuses = []
    r = edge.EdgeRunner(read_block=read_block, post=post, token='T', read_plan=PLAN, version='test/1',
                        backend='fake', clock=clock, on_status=lambda rr: statuses.append(rr.status))
    await r.tick()
    check('1. kolo: oba bloky přečteny', reads_log == [(3, 586, 3), (3, 3, 5)], reads_log)
    body = posts[-1][1]
    check('sync s tokenem, verzí a stavem přepínače řízení (výchozí vypnuto)', posts[-1][0] == edge.SYNC_PATH
          and posts[-1][2] == 'T' and body['client'] == {'ver': 'test/1', 'backend': 'fake', 'ctl': 'off'}, posts[-1])
    check('čtení nese sn bloku a ts', body['reads'][0].get('sn') == '123' and body['reads'][0]['ts'] == 1_000_000
          and body['reads'][0]['values'] == [586, 587, 588], body['reads'][0])
    check('stav online', r.status == edge.ONLINE, r.status)

    clock.t += 10
    n = len(posts)
    await r.tick()
    check('po 10 s nic na řadě a heartbeat ještě ne → bez syncu', len(posts) == n, posts[n:])
    clock.t += 25
    await r.tick()
    check('po 35 s jen živý blok', reads_log[-1] == (3, 586, 3) and len(posts[-1][1]['reads']) == 1, posts[-1])

    fail_addr.add(586)
    clock.t += 30
    await r.tick()
    check('selhané čtení → sync bez dat (heartbeat), chyba zapsána', posts[-1][1]['reads'] == [] and r.read_errors == 1
          and 'live' in (r.last_error or ''), (posts[-1], r.last_error))
    fail_addr.clear()
    clock.t += 31
    k = len(reads_log)
    await r.tick()
    check('selhaný blok se zkusí znovu za 30 s (ne až za everyS)', (3, 586, 3) in reads_log[k:], reads_log[k:])

    responses.append((200, {'ok': True, 'nextPollS': 2, 'readPlan': PLAN + [
        {'name': 'batcap', 'type': 'modbus', 'fc': 3, 'addr': 102, 'count': 1, 'everyS': 3600}],
        'commands': [{'type': 'modbus', 'op': 'write'}]}))
    clock.t += 30
    await r.tick()
    check('nový plán ze serveru převzat', any(b['addr'] == 102 for b in r.plan), r.plan)
    check('nextPollS sevřen na minimum 5 s', r.next_poll_s == edge.POLL_MIN_S, r.next_poll_s)
    clock.t += 5
    await r.tick()
    check('nový blok se hned čte', (3, 102, 1) in reads_log, reads_log[-3:])
    check('příkaz bez id se ignoruje (nic nezapsáno, žádný výsledek)', all(fc in (3, 4) for fc, _, _ in reads_log)
          and not any('results' in b for _, b, _ in posts))

    responses.append((409, {'ok': False, 'error': 'device-gone', 'message': 'Střídač už k elektrárně nepatří'}))
    clock.t += 30
    await r.tick()
    check('409 → chyba se zprávou serveru', r.status == edge.ERROR and 'nepatří' in r.last_error, r.last_error)
    n = len(posts)
    clock.t += 60
    await r.tick()
    check('409 → 5 min pauza', len(posts) == n, posts[n:])
    clock.t += edge.BACKOFF_S
    await r.tick()
    check('po pauze zase sync', len(posts) == n + 1)

    async def post_down(path, body, token):
        raise OSError('síť')
    r2 = edge.EdgeRunner(read_block=read_block, post=post_down, token='T', read_plan=PLAN, version='t', backend='f',
                         clock=clock)
    await r2.tick()
    check('server nedostupný → chyba, bez pádu', r2.status == edge.ERROR and 'nedostupný' in r2.last_error, r2.last_error)

    responses.append((401, {'ok': False}))
    clock.t += 3600
    try:
        await r.tick()
        check('401 → Revoked', False, 'bez výjimky')
    except edge.Revoked:
        check('401 → Revoked + stav odpojeno', r.status == edge.REVOKED, r.status)
    check('senzor dostával změny stavu', edge.ONLINE in statuses and edge.ERROR in statuses, statuses)


def cmd(cid, fc=16, addr=143, values=None, count=None, sn='SN1', ttl=10, **kw):
    c = {'id': cid, 'type': 'modbus', 'sn': sn, 'fc': fc, 'addr': addr, 'expiresInS': ttl, **kw}
    if fc == 16:
        c['values'] = [15400] if values is None else values
    else:
        c['count'] = 1 if count is None else count
    return c


async def control():
    print('— řízení (příkazy serveru)')
    clock = Clock()
    regs = {143: 15500, 108: 100}
    reads_log, writes, posts, sleeps = [], [], [], []
    knob = {'write_err': None, 'rb_err': False, 'write_s': 0.3, 'down': False, 'code': 200, 'control': 'on'}
    queue = []                               # co server vrátí v dalších sync: seznamy příkazů

    async def read_block(fc, addr, count):
        reads_log.append((fc, addr, count))
        if knob['rb_err'] and writes:
            raise TimeoutError('čtení po zápisu neodpovědělo')
        return [regs.get(a, 0) for a in range(addr, addr + count)]

    async def write_block(addr, values):
        writes.append((clock.t, addr, list(values)))
        if knob['write_err']:
            raise knob['write_err']
        for i, v in enumerate(values):
            regs[addr + i] = v
        clock.t += knob['write_s']

    async def fake_sleep(s):
        sleeps.append(s)
        clock.t += s

    async def post(path, body, token):
        if knob['down']:
            raise OSError('síť')
        posts.append(body)
        if knob['code'] != 200:
            return knob['code'], {'ok': False, 'error': 'bad-results'}
        return 200, {'ok': True, 'nextPollS': 5 if knob['control'] else 10, 'control': knob['control'],
                     'commands': queue.pop(0) if queue else [], 'accepted': len(body['reads'])}

    r = edge.EdgeRunner(read_block=read_block, write_block=write_block, post=post, token='T', read_plan=[],
                        version='t', backend='f', clock=clock, sn='SN1', sleep=fake_sleep)

    async def step(*cmds, dt=5.0):
        """Server v příští odpovědi pošle `cmds`; jedno kolo klienta. → výsledky odeslané v tomto kole."""
        if cmds:
            queue.append(list(cmds))
        clock.t += dt
        n = len(posts)
        await r.tick()
        return [x for b in posts[n:] for x in b.get('results', [])]

    await step()
    check('server s řízením: control „on", poll 5 s', r.server_control == 'on' and r.next_poll_s == 5, r.server_control)
    n = len(posts)
    await step(dt=4.6)
    check('server s řízením: sync á nextPollS i bez dat (čekají tam příkazy)', len(posts) == n + 1, len(posts) - n)

    res = await step(cmd('c-1'))
    check('přepínač vypnutý: zápis odmítnut „local-off", do střídače nic', len(res) == 1 and res[0]['id'] == 'c-1'
          and res[0]['ok'] is False and res[0]['err'] == 'local-off' and not writes and regs[143] == 15500, res)
    check('výsledek odeslán hned v tomtéž kole (další sync)', posts[-1].get('results') == res, posts[-1])
    res = await step(cmd('c-2', fc=3, addr=143, count=2))
    check('přepínač vypnutý: čtení se provede', res and res[0]['ok'] and res[0]['values'] == [15500, 0], res)

    r.control_enabled = True
    res = await step(cmd('c-3'))
    check('přepínač zapnutý: zápis 143 → OK, zpětné čtení, registr změněn',
          res and res[0]['ok'] is True and res[0]['readBack'] == [15400] and regs[143] == 15400
          and writes[-1][1:] == (143, [15400]), res)
    check('sync hlásí zapnutý přepínač (ctl on)', posts[-1]['client']['ctl'] == 'on', posts[-1]['client'])
    check('po příkazu rychlé dotazování (výsledek / další blok TOU)', r.poll_s() == edge.FAST_POLL_S, r.poll_s())
    nw = len(writes)
    res = await step(cmd('c-3'))
    check('stejné id podruhé → jen znovu výsledek, do střídače podruhé nic', len(writes) == nw and res
          and res[0]['ok'] and res[0]['id'] == 'c-3', res)

    res = await step(cmd('c-4', addr=160, values=[5200]), cmd('c-5', addr=143, values=[1, 1]),
                     cmd('c-6', addr=143, values=[24001]), cmd('c-7', addr=148, values=[2460]),
                     cmd('c-8', addr=172, values=[0x40]), cmd('c-9', addr=143, values=[True]))
    check('mimo profil (napětí TOU, přes hranici, nad max, špatný čas, příznak, bool) → not-allowed',
          len(res) == 6 and all(x['ok'] is False and x['err'].startswith('not-allowed') for x in res)
          and len(writes) == nw, res)
    res = await step(cmd('c-10', addr=0x94, values=[0, 500, 900, 1300, 1700, 2100] + [5000] * 6),
                     cmd('c-11', addr=0xA6, values=[20, 21, 22, 23, 24, 25, 0, 1, 0, 0, 0x20, 0]),
                     cmd('c-12', addr=0x92, values=[0xFF]))
    check('TOU: 3 bloky platformy projdou profilem a zapíšou se', len(res) == 3 and all(x['ok'] for x in res)
          and regs[153] == 2100 and regs[173] == 1 and regs[176] == 0x20 and regs[146] == 0xFF, res)
    t_w = [w[0] for w in writes[-3:]]
    check('mezi zápisy aspoň WRITE_GAP_S', all(b - a >= edge.WRITE_GAP_S - 1e-9 for a, b in zip(t_w, t_w[1:])), t_w)
    res = await step(cmd('c-13', addr=62, values=[(26 << 8) | 10, (15 << 8) | 18, (5 << 8) | 7]))
    check('hodiny 62–64 projdou profilem', res and res[0]['ok'], res)

    res = await step(cmd('c-14', sn='JINE'), cmd('c-15', fc=6), cmd('c-16', fc=3, count=200),
                     {'type': 'modbus', 'fc': 16, 'addr': 143, 'values': [1]}, cmd('c-17', type='ha'))
    check('cizí SN / FC06 / počet / bez id / neznámý typ → foreign-sn / bad-command, bez id bez výsledku',
          [x['err'].split(':')[0] for x in res] == ['foreign-sn', 'bad-command', 'bad-command', 'bad-command'], res)

    knob['write_s'] = 12
    res = await step(cmd('c-18', values=[15300]), cmd('c-19', values=[15200]))
    knob['write_s'] = 0.3
    check('dva zápisy, první trvá 12 s → druhý po lhůtě (expired), neprovede se',
          res[0]['ok'] and res[1]['err'] == 'expired' and regs[143] == 15300, res)

    knob['write_err'] = OSError('Timeout')
    res = await step(cmd('c-20'))
    knob['write_err'] = None
    check('ha-solarman zápis selže → exec: … (server pak pošle přes cloud)', res and res[0]['err'] == 'exec: Timeout', res)
    knob['rb_err'] = True
    res = await step(cmd('c-21', values=[15500]))
    knob['rb_err'] = False
    check('zpětné čtení selže → zápis OK s rbErr (posoudí server)', res and res[0]['ok'] and 'readBack' not in res[0]
          and 'neodpovědělo' in res[0].get('rbErr', ''), res)

    clock.t += 3600
    r._writes = []
    oks = 0
    for i in range(edge.RATE_PER_MIN + 1):
        out = await r._command(cmd(f'r-{i}', addr=108, values=[50 + i]), clock())
        oks += out['ok'] is True
    check(f'limit: {edge.RATE_PER_MIN} zápisů za minutu, další rate-limit', oks == edge.RATE_PER_MIN
          and out['err'] == 'rate-limit', (oks, out))
    clock.t += 61
    out = await r._command(cmd('r-x', addr=108, values=[90]), clock())
    check('po minutě zase zápis', out['ok'] is True, out)
    r._writes = [clock() - 100 - i for i in range(edge.RATE_PER_H)]
    out = await r._command(cmd('r-h', addr=108, values=[91]), clock())
    check(f'limit: {edge.RATE_PER_H} zápisů za hodinu', out['err'] == 'rate-limit', out)
    r._writes = []

    r._pending.append({'id': 'c-99', 'ok': True, 'values': [1], 'dur': 0.1})
    knob['down'] = True
    await step()
    check('server nedostupný → výsledek zůstane k odeslání', r._pending and r._pending[-1]['id'] == 'c-99', r._pending)
    knob['down'] = False
    res = await step(dt=0.1)
    check('příští kolo výsledek pošle (i bez dat a před lhůtou)', any(x['id'] == 'c-99' for x in res) and not r._pending,
          res)
    r._pending.append({'id': 'c-98', 'ok': True, 'values': [1], 'dur': 0.1})
    knob['code'] = 400
    await step()
    knob['code'] = 200
    check('400 od serveru → výsledky zahozeny (ne pořád dokola)', not r._pending, r._pending)

    clock.t += edge.FAST_WINDOW_S + 1
    knob['control'] = None
    await step()
    check('server bez řízení → control None, poll podle serveru, ne rychlý', r.server_control is None
          and r.poll_s() == 10, (r.server_control, r.poll_s()))
    n = len(posts)
    await step(dt=10)
    check('server bez řízení: bez dat jen heartbeat á 30 s', len(posts) == n, len(posts) - n)
    r.set_control(False)
    await step(dt=1)
    check('přepnutí přepínače → hned sync s novým stavem (i bez dat, před heartbeatem)', len(posts) == n + 1
          and posts[-1]['client']['ctl'] == 'off', (len(posts) - n, posts[-1]['client']))
    n = len(posts)
    r.set_control(False)
    await step(dt=1)
    check('přepínač beze změny → žádný sync navíc', len(posts) == n, len(posts) - n)

    r3 = edge.EdgeRunner(read_block=read_block, post=post, token='T', read_plan=[], version='t', backend='f',
                         clock=clock, sn='SN1', sleep=fake_sleep)
    r3.control_enabled = True
    out = await r3._command(cmd('w-1'), clock())
    check('bez funkce zápisu → exec (záloha přes cloud)', out['err'].startswith('exec'), out)


NEUTRAL = [{'fc': 16, 'addr': 0x94, 'values': [0, 500, 900, 1300, 1700, 2100] + [5000] * 6},
           {'fc': 16, 'addr': 0xA6, 'values': [20, 20, 20, 20, 20, 20, 0, 0, 0, 0, 0, 0]},
           {'fc': 16, 'addr': 0x92, 'values': [0xFF]}]


async def fallback():
    print('— offline pojistka')
    fb = edge.clean_fallback({'id': 'a', 'writes': NEUTRAL})
    check('clean_fallback: neutrál projde (bez fc)', fb and [w['addr'] for w in fb['writes']] == [0x94, 0xA6, 0x92]
          and 'fc' not in fb['writes'][0], fb)
    bad = [{'id': 'a', 'writes': NEUTRAL[:2] + [{'fc': 16, 'addr': 160, 'values': [5200]}]},
           {'id': 'a', 'writes': [{'fc': 6, 'addr': 0x92, 'values': [0xFF]}]},
           {'id': 'a', 'writes': NEUTRAL * 3}, {'id': 'x' * 41, 'writes': NEUTRAL}, {'id': 'a', 'writes': []},
           {'id': 'a', 'writes': [{'addr': True, 'values': [1]}]}, {'writes': NEUTRAL}, None, 'x']
    check('clean_fallback: zápis mimo profil / FC06 / moc zápisů / dlouhé id / prázdné / bool adresa → celé None',
          all(edge.clean_fallback(b) is None for b in bad), [edge.clean_fallback(b) for b in bad])

    clock = Clock()
    regs, writes, posts, saved = {}, [], [], []
    knob = {'down': False, 'code': 200, 'fb': None, 'skew': 1000.0, 'write_err': None, 'ack': True, 'nofb': False}

    async def read_block(fc, addr, count):
        return [regs.get(a, 0) for a in range(addr, addr + count)]

    async def write_block(addr, values):
        writes.append((clock.t, addr, list(values)))
        if knob['write_err']:
            raise knob['write_err']
        for i, v in enumerate(values):
            regs[addr + i] = v
        clock.t += 0.3

    async def fake_sleep(s):
        clock.t += s

    async def post(path, body, token):
        posts.append(body)
        if knob['down']:
            raise OSError('síť')
        if knob['code'] != 200:
            return knob['code'], {'ok': False, 'error': 'x'}
        res = {'ok': True, 'serverTime': clock.t + knob['skew'], 'nextPollS': 5, 'control': 'on', 'commands': [],
               'accepted': 0}
        if not knob['nofb']:
            res['fallback'] = knob['fb']
        if body.get('fallbackReport') and knob['ack']:
            res['fallbackAck'] = body['fallbackReport']['id']
        return 200, res

    def runner(state=None):
        return edge.EdgeRunner(read_block=read_block, write_block=write_block, post=post, token='T', read_plan=[],
                               version='t', backend='f', clock=clock, sn='SN1', sleep=fake_sleep, state=state,
                               on_persist=lambda st: saved.append(json.loads(json.dumps(st))))

    def server_fb(fid='fb-1', valid_in=600.0):
        """Pojistka, jak ji pošle server: validUntil v čase serveru (hodiny o `skew` napřed)."""
        return {'id': fid, 'validUntil': clock.t + knob['skew'] + valid_in, 'writes': NEUTRAL}

    async def step(r, dt=5.0):
        clock.t += dt
        await r.tick()

    r = runner()
    r.control_enabled = True
    t0 = clock.t
    knob['fb'] = server_fb(valid_in=600)
    await step(r)
    check('pojistka přijata, platnost převedena na hodiny klienta (server o 1000 s napřed)', r.fallback
          and abs(r.fallback['deadline'] - (t0 + 600)) < 1e-6 and r.fallback['id'] == 'fb-1', r.fallback)
    check('nová pojistka → uložena (HA Store)', len(saved) == 1 and saved[-1]['fallback']['id'] == 'fb-1', saved)
    knob['fb'] = dict(knob['fb'], validUntil=knob['fb']['validUntil'] + 5)
    await step(r)
    check('stejná pojistka, platnost o pár s dál → neukládat při každém sync', len(saved) == 1, len(saved))
    knob['fb'] = dict(knob['fb'], validUntil=knob['fb']['validUntil'] + edge.FALLBACK_SAVE_S)
    await step(r)
    check(f'platnost posunutá o {edge.FALLBACK_SAVE_S} s → uložit', len(saved) == 2, len(saved))
    knob['nofb'] = True
    await step(r)
    knob['nofb'] = False
    check('odpověď bez klíče fallback (starší server) → pojistka zůstává', r.fallback and r.fallback['id'] == 'fb-1',
          r.fallback)
    good = knob['fb']
    knob['fb'] = {'id': 'zly', 'validUntil': good['validUntil'], 'writes': [{'fc': 16, 'addr': 160, 'values': [5200]}]}
    await step(r)
    check('pojistka mimo profil → odmítnuta, dosavadní smazána (radši nic než cizí zápis)', r.fallback is None
          and saved[-1]['fallback'] is None, (r.fallback, saved[-1]))
    knob['fb'] = None
    await step(r)
    check('server pojistku neposílá (řízení vypnuto / neaktivní obchodování) → žádná', r.fallback is None, r.fallback)

    knob['fb'] = server_fb(valid_in=-60)
    await step(r)
    check('platnost už prošla, ale server odpovídá → nic nezapsat (řídí server)', r.fallback and not writes
          and r.fallback_applied_at is None, writes)

    knob['fb'] = server_fb(valid_in=600)
    await step(r)
    deadline = r.fallback['deadline']
    knob['down'] = True
    await step(r, dt=300)
    check('výpadek před koncem platnosti → nic', not writes, writes)
    r.set_control(False)
    clock.t = deadline + 10
    await r.tick()
    check('po platnosti bez spojení, ale přepínač vypnutý → nic', not writes and r.fallback_applied_at is None, writes)
    r.set_control(True)
    await step(r, dt=1)
    check('přepínač zapnut → neutrál zapsán (3 bloky v pořadí ze serveru)', [w[1] for w in writes] == [0x94, 0xA6, 0x92]
          and regs[0x92] == 0xFF and regs[0xA6] == 20, [w[1] for w in writes])
    t_w = [w[0] for w in writes]
    check('i pojistka drží odstup WRITE_GAP_S', all(b - a >= edge.WRITE_GAP_S - 1e-9 for a, b in zip(t_w, t_w[1:])), t_w)
    rep = r._fb_report
    check('hlášení připraveno: id, ok, kroky se zpětným čtením', rep and rep['id'] == 'fb-1' and rep['ok'] is True
          and len(rep['steps']) == 3 and rep['steps'][2]['readBack'] == [0xFF], rep)
    check('stav uložen (kdy, hlášení) a viditelný v senzoru', saved[-1]['appliedAt'] == r.fallback_applied_at
          and saved[-1]['report']['id'] == 'fb-1' and r.fallback_applied_at and 'pojistka' in r.last_command,
          (saved[-1], r.last_command))
    nw = len(writes)
    for _ in range(5):
        await step(r, dt=600)
    check('dál bez spojení → podruhé nic (jednou za výpadek)', len(writes) == nw, len(writes) - nw)
    check('hlášení se zkouší posílat při každém pokusu o sync', posts[-1].get('fallbackReport', {}).get('id') == 'fb-1',
          posts[-1])

    r2 = runner(state=saved[-1])
    r2.control_enabled = True
    await step(r2, dt=600)
    check('restart HA během výpadku: pojistka i čas zápisu načtené → znovu nezapisovat', len(writes) == nw
          and r2.fallback and r2.fallback['deadline'] == deadline and r2._fb_report, (len(writes) - nw, r2.state()))
    knob['down'] = False
    n_saved = len(saved)
    await step(r2, dt=edge.HEARTBEAT_S)
    sent = [b for b in posts if b.get('fallbackReport')]
    check('spojení zpět → hlášení odesláno, potvrzeno (fallbackAck) a smazáno', sent and r2._fb_report is None
          and len(saved) > n_saved and saved[-1]['report'] is None, r2._fb_report)
    n = len(posts)
    await step(r2)
    check('další sync už bez hlášení', 'fallbackReport' not in posts[n], posts[n])
    knob['skew'] += 0.37                       # stejná prošlá pojistka, jen jiná odchylka hodin (ms posun platnosti)
    await step(r2)
    knob['down'] = True
    await step(r2, dt=600)
    check('krátce spojení se starou (prošlou) pojistkou, pak zase výpadek → znovu nezapisovat', len(writes) == nw,
          len(writes) - nw)
    knob['down'] = False
    knob['fb'] = server_fb(fid='fb-2', valid_in=600)
    await step(r2, dt=edge.HEARTBEAT_S)
    knob['down'] = True
    clock.t = r2.fallback['deadline'] + 1
    await r2.tick()
    check('nová pojistka (platnost po posledním zápisu) a nový výpadek → zapsat znovu', len(writes) == nw + 3
          and r2._fb_report['id'] == 'fb-2', (len(writes) - nw, r2._fb_report))
    knob['down'] = False
    await step(r2, dt=edge.HEARTBEAT_S)

    # 5xx = výpadek, 4xx = odmítnutí
    for code, expect in ((503, True), (409, False)):
        writes.clear()
        r3 = runner()
        r3.control_enabled = True
        knob['fb'], knob['code'] = server_fb(valid_in=60), 200
        await step(r3)
        knob['code'] = code
        clock.t = r3.fallback['deadline'] + 1
        await r3.tick()
        check(f'HTTP {code} po platnosti → ' + ('pojistka (server nedostupný)' if expect else 'nic (server odmítá, ne výpadek)'),
              bool(writes) == expect, writes)
    knob['code'] = 200

    # ha-solarman neodpovídá → zkusit znovu, nejvýš FALLBACK_TRIES×
    writes.clear()
    r4 = runner()
    r4.control_enabled = True
    knob['fb'] = server_fb(fid='fb-4', valid_in=60)
    await step(r4)
    knob['down'], knob['write_err'] = True, OSError('Timeout')
    clock.t = r4.fallback['deadline'] + 1
    await r4.tick()
    check('zápis selže (exec) → bez hlášení, další pokus naplánován', len(writes) == 3 and r4._fb_report is None
          and r4.fallback_applied_at is None, (len(writes), r4._fb_report))
    await step(r4, dt=100)
    check(f'před {edge.FALLBACK_RETRY_S} s znovu nic', len(writes) == 3, len(writes))
    await step(r4, dt=edge.FALLBACK_RETRY_S)
    check('2. pokus', len(writes) == 6 and r4._fb_report is None, len(writes))
    knob['write_err'] = None
    await step(r4, dt=edge.FALLBACK_RETRY_S)
    check('3. pokus vyjde → hlášení ok', len(writes) == 9 and r4._fb_report and r4._fb_report['ok'] is True,
          r4._fb_report)

    writes.clear()
    r5 = runner()
    r5.control_enabled = True
    knob['down'], knob['fb'] = False, server_fb(fid='fb-5', valid_in=60)
    await step(r5)
    knob['down'], knob['write_err'] = True, OSError('Timeout')
    clock.t = r5.fallback['deadline'] + 1
    for _ in range(edge.FALLBACK_TRIES + 2):
        await r5.tick()
        clock.t += edge.FALLBACK_RETRY_S + 1
    check(f'pořád selhává → po {edge.FALLBACK_TRIES} pokusech konec, hlášení ok=False s chybami',
          len(writes) == 3 * edge.FALLBACK_TRIES and r5._fb_report and r5._fb_report['ok'] is False
          and all(s['err'] == 'exec: Timeout' for s in r5._fb_report['steps']), (len(writes), r5._fb_report))
    knob['down'], knob['write_err'], knob['ack'] = False, None, False
    await step(r5)
    check('server hlášení nepotvrdí (200 bez fallbackAck) → stejně smazat, neposílat dokola', r5._fb_report is None,
          r5._fb_report)
    knob['ack'] = True

    r6 = runner(state={'fallback': {'id': 'x', 'deadline': 'zítra', 'writes': NEUTRAL}, 'appliedAt': None,
                       'report': 'nesmysl'})
    check('poškozený uložený stav → bez pojistky, bez pádu', r6.fallback is None and r6._fb_report is None, r6.state())
    r7 = runner(state={'fallback': {'id': 'x', 'deadline': 1.0, 'writes': [{'addr': 160, 'values': [5200]}]}})
    check('uložená pojistka mimo profil (jiná verze profilu) → zahodit', r7.fallback is None, r7.state())


def profile_data():
    """Profil zápisu jako data (bez serveru — běží i v CI)."""
    p = edge.PROFILE
    check('profil deye-lv@1: limity v datech = limity klienta',
          p['limits'] == {'perMin': edge.RATE_PER_MIN, 'perHour': edge.RATE_PER_H, 'gapS': edge.WRITE_GAP_S}
          and p['maxBlock'] == 123, p['limits'])
    check('profil deye-lv@1: TOU, WRITABLE i hodiny projdou, napětí TOU / Remote Mode / mimo meze ne',
          edge.write_problem(148, [0, 500, 1300, 1700, 2100, 2330] + [5000] * 6) is None
          and edge.write_problem(62, [(26 << 8) | 10, (15 << 8) | 18, (5 << 8) | 7]) is None
          and all(edge.write_problem(a, v) for a, v in ((160, [5200]), (0x044C, [1]), (143, [24001]), (148, [2460]),
                                                        (172, [0x40]), (108, [True]), (62, [0, 0, 0]))))
    wp = edge._wp
    bad = []
    for pid in ('../x@1', 'deye-lv', 'Deye@1'):
        try:
            wp.load(pid, edge.PROFILES_DIR)
            bad.append(pid)
        except ValueError:
            pass
    try:
        wp.compile_profile({'id': 'x@1', 'write': [{'addr': 1, 'check': 'neznama'}]})
        bad.append('neznámá kontrola')
    except ValueError:
        pass
    check('profil: id bez cesty, neznámá kontrola = neplatný profil', not bad, bad)


def parity():
    dm = os.path.join(_HERE, '..', '..', 'deye-monitor')
    if not os.path.isdir(os.path.join(dm, 'deye')):
        print('SKIP profil × server: vedle repa chybí checkout deye-monitor')
        return
    os.environ.setdefault('DEYE_DB_PATH', os.path.join(tempfile.mkdtemp(prefix='wattery-parity-'), 'x.db'))
    os.environ.setdefault('HTTPS_PROXY', 'http://127.0.0.1:9')
    sys.path.insert(0, os.path.abspath(dm))
    from deye import edge_deye
    sp, cp = edge_deye.EDGE_WRITE_PROFILE, edge.DEYE_PROFILE
    check('profil zápisu: stejné registry jako server (edge_deye.EDGE_WRITE_PROFILE)', set(sp) == set(cp),
          sorted(set(sp) ^ set(cp)))
    diffs = []
    for a in sorted(set(sp) & set(cp)):
        for v in range(0x10000):
            if (edge_deye.edge_write_problem(a, [v]) is None) != (edge.write_problem(a, [v]) is None):
                diffs.append((a, v))
                break
    check('profil zápisu: stejné meze a kontroly jako server (všech 65 536 hodnot)', not diffs, diffs[:5])
    # Profil jsou data (profiles/*.json + write_profile.py): server drží bajtově stejné kopie.
    cc = os.path.join(_HERE, '..', 'custom_components', 'wattery')
    same = []
    for mine, theirs in ((os.path.join(cc, 'write_profile.py'), os.path.join(dm, 'deye', 'edge_profile.py')),
                         *((os.path.join(cc, 'profiles', f), os.path.join(dm, 'deye', 'edge_profiles', f))
                           for f in sorted(os.listdir(os.path.join(cc, 'profiles'))))):
        with open(mine, 'rb') as a, open(theirs, 'rb') as b:
            same.append((os.path.basename(mine), a.read().replace(b'\r\n', b'\n') == b.read().replace(b'\r\n', b'\n')))
    check('profil: engine a JSON profily bajtově stejné jako na serveru (edge_profile.py, edge_profiles/)',
          all(ok for _f, ok in same), same)
    diffs = [(a, v) for a in sorted(cp) for v in range(0x10000)
             if (edge_deye.derived_write_problem(a, [v]) is None) != (edge.write_problem(a, [v]) is None)]
    check('profil zápisu = kód platformy na serveru (WRITABLE, TOU, hodiny — všech 65 536 hodnot)', not diffs, diffs[:5])


def _http_post(base):
    async def post(path, body, token=None):
        def go():
            req = urllib.request.Request(base + path, data=json.dumps(body).encode(), method='POST',
                                         headers={'Content-Type': 'application/json',
                                                  **({'Authorization': f'Bearer {token}'} if token else {})})
            try:
                with urllib.request.urlopen(req, timeout=20) as resp:
                    return resp.status, json.loads(resp.read() or b'{}')
            except urllib.error.HTTPError as e:
                return e.code, json.loads(e.read() or b'{}')
        return await asyncio.get_running_loop().run_in_executor(None, go)
    return post


async def e2e(url, code, sn):
    sys.path.insert(0, os.path.join(_HERE, '..', '..', 'deye-monitor', 'tools'))
    import edge_sim                                       # vymyšlené registry střídače (deye-monitor)
    import time
    post = _http_post(url)
    st, res = await post(edge.PAIR_PATH, {'code': code, 'kind': 'ha', 'sn': sn, 'clientVer': 'ha-int/test',
                                          'label': 'test'})
    check('E2E pair 200', st == 200 and res.get('token'), (st, res))
    if st != 200:
        return
    regs = edge_sim.fake_regs(sn, time.time())

    async def read_block(fc, addr, count):
        return [regs.get(a, 0) for a in range(addr, addr + count)]
    r = edge.EdgeRunner(read_block=read_block, post=post, token=res['token'], read_plan=res['readPlan'],
                        version='ha-int/test', backend='test', next_poll_s=res.get('nextPollS', 10))
    await r.tick()
    check('E2E sync: online, všechny bloky přijaté', r.status == edge.ONLINE and r.last_accepted == len(r.plan),
          (r.status, r.last_error, r.last_accepted, len(r.plan)))
    bad = edge.EdgeRunner(read_block=read_block, post=post, token='spatny-token', read_plan=res['readPlan'],
                          version='t', backend='t')
    try:
        await bad.tick()
        check('E2E špatný token → Revoked', False)
    except edge.Revoked:
        check('E2E špatný token → Revoked', True)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--url')
    ap.add_argument('--code')
    ap.add_argument('--sn')
    a = ap.parse_args()
    asyncio.run(unit())
    asyncio.run(control())
    asyncio.run(fallback())
    profile_data()
    parity()
    if a.url:
        asyncio.run(e2e(a.url, a.code, a.sn))
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    sys.exit(1 if FAILS else 0)
