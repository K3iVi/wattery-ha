"""Testy jádra (bez Home Assistantu): python tests/test_edge.py

Čtení, sync, řízení (příkazy, přepínač, profil, limity, zpětné čtení), offline pojistka, protokol 2 (caps, pojistky
příkazů s obnovou, readBack, profil od serveru, přenos mqtt) a shoda profilu zápisu se serverem
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
          and posts[-1][2] == 'T' and body['client'] == {'ver': 'test/1', 'backend': 'fake', 'ctl': 'off',
                                                     'caps': edge.client_caps()}, posts[-1])
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


def _match(flt, path):
    f, p = flt.split('/'), path.split('/')
    return len(f) == len(p) and all(a in ('+', b) for a, b in zip(f, p))


class FakeGx:
    """Přenos mqtt jako transport_mqtt_gx.GxMqtt (portal, read_item s base, set_filters) + zápis s ozvěnou (A5)."""
    D = 'settings/0/Settings/DynamicEss/'

    def __init__(self, portal='c0ffee000001', can_write=True):
        self.portal, self.filters, self.writes = portal, [], []
        self.values = {'system/0/Dc/Battery/Soc': 55.5, self.D + 'Mode': 0, self.D + 'Schedule/0/Soc': 20}
        self.ts = {p: 999_000.7 for p in self.values}
        if not can_write:
            self.write = None

    async def set_filters(self, filters):
        self.filters = list(filters)

    def read_item(self, it):
        base = it.get('base') or ''
        vals = {p[len(base):]: v for p, v in self.values.items()
                if p.startswith(base) and any(_match(base + f, p) for f in it['paths'])}
        return {'type': 'mqtt', 'name': it['name'], 'dev': self.portal, 'values': vals,
                'tsBy': {p: self.ts[base + p] for p in vals}, 'ts': 0}

    async def write(self, pairs):
        self.writes.append([list(x) for x in pairs])
        self.values.update({p: v for p, v in pairs})
        return {p: self.values[p] for p, _v in pairs}


def _mk(sn='SN1', plan=(), transports=None, state=None, regs=None, modbus=True, integrations=()):
    """Klient proti falešnému serveru protokolu 2 (leases, profile, příkazy z fronty)."""
    clock, regs = Clock(), ({} if regs is None else regs)
    e = {'posts': [], 'writes': [], 'reads': [], 'saved': [], 'clock': clock, 'regs': regs,
         'srv': {'leases': None, 'profile': None, 'cmds': [], 'down': False, 'ack': True, 'control': 'on'}}

    async def read_block(fc, addr, count):
        e['reads'].append((fc, addr, count))
        return [regs.get(a, 0) for a in range(addr, addr + count)]

    async def write_block(addr, values):
        e['writes'].append((clock.t, addr, list(values)))
        regs.update({addr + i: v for i, v in enumerate(values)})
        clock.t += 0.2

    async def sleep(s):
        clock.t += s

    async def post(path, body, token):
        s = e['srv']
        e['posts'].append(body)
        if s['down']:
            raise OSError('síť')
        res = {'ok': True, 'serverTime': clock.t, 'nextPollS': 5, 'control': s['control'],
               'commands': s['cmds'].pop(0) if s['cmds'] else [], 'accepted': len(body['reads'])}
        if s['leases'] is not None:
            res['leases'] = s['leases']() if callable(s['leases']) else s['leases']
        if s.get('readPlan') is not None:
            res['readPlan'] = s['readPlan']
        if s['profile'] or s.get('nullProfile'):
            res['profile'] = s['profile']
        if body.get('leaseReport') and s['ack']:
            res['leaseAck'] = body['leaseReport']['id']
        if body.get('fallbackReport'):
            res['fallbackAck'] = body['fallbackReport']['id']
        return 200, res
    r = edge.EdgeRunner(read_block=read_block if modbus else None, write_block=write_block if modbus else None,
                        post=post, token='T', read_plan=list(plan), version='t', backend='f', clock=clock, sn=sn,
                        sleep=sleep, state=state, on_persist=lambda st: e['saved'].append(json.loads(json.dumps(st))),
                        transports=transports, integrations=integrations)
    return r, e


async def _step(r, e, dt=5.0, cmds=None):
    if cmds:
        e['srv']['cmds'].append(list(cmds))
    e['clock'].t += dt
    n = len(e['posts'])
    await r.tick()
    return [x for b in e['posts'][n:] for x in b.get('results', [])]


async def proto2():
    print('— protokol 2 (caps, leases, readBack, profil, mqtt)')
    caps = edge.client_caps()
    check('caps: Modbus, vestavěné profily, lease + readBack', caps == {
        'proto': 2, 'transports': ['modbus'], 'profiles': sorted(edge.PROFILES), 'features': ['lease', 'readBack', 'plan']}
        and 'deye-lv@1' in caps['profiles'], caps)
    check('caps: zapojený přenos mqtt se hlásí', edge.client_caps({'mqtt': object()})['transports'] == ['modbus', 'mqtt'])
    plan = [{'type': 'mqtt', 'name': 'dess', 'dev': 'c0ffee000001', 'base': FakeGx.D, 'paths': ['Schedule/+/Soc', 'Mode'],
             'everyS': 300},
            {'type': 'mqtt', 'name': 'live', 'paths': ['system/0/Dc/Battery/Soc'], 'everyS': 30},
            {'type': 'mqtt', 'name': 'zle', 'paths': 'system/#', 'everyS': 30},
            {'type': 'ha_entity', 'name': 'ent', 'integration': 'solax_modbus', 'everyS': 60},
            {'type': 'modbus', 'fc': 3, 'addr': 586, 'count': 3, 'everyS': 30}]
    items = edge.clean_items(plan, {'mqtt': object()})
    check('clean_items: jen položky zapojených přenosů a platné (paths seznam)', [i['name'] for i in items] == ['dess', 'live']
          and items[0]['base'] == FakeGx.D and items[0]['dev'] == 'c0ffee000001'
          and [b['addr'] for b in edge.clean_plan(plan)] == [586], items)
    big = [{'type': 'mqtt', 'name': f'n{i}', 'values': {'x' * 40 + str(k): k for k in range(400)}} for i in range(3)]
    huge = {'type': 'mqtt', 'name': 'huge', 'values': {'y' * 40 + str(k): k for k in range(2000)}}
    ch = edge.split_reads(big + [huge])
    ch2 = edge.split_reads([{'fc': 3, 'addr': a, 'values': [0]} for a in range(20)])
    check('split_reads: pod 48 kB a nejvýš 16 položek v sync, položka přes strop vynechána',
          [len(c) for c in ch] == [2, 1] and all(len(json.dumps(c)) < edge.READS_BUDGET for c in ch)
          and [len(c) for c in ch2] == [16, 4] and edge.split_reads([]) == [], [len(c) for c in ch])

    # ── pojistky (Deye profil: obnova i onExpire do registru 143) ──
    r, e = _mk()
    S, clock = e['srv'], e['clock']
    r.control_enabled = True
    station = lambda t: {'id': 'st-1', 'source': 'station', 'validUntil': t, 'onExpire': [
        {'type': 'modbus', 'sn': 'SN1', 'fc': 16, 'addr': w['addr'], 'values': w['values']} for w in NEUTRAL]}

    def cmd_lease(t, lid='L1', hard=None, **kw):
        out = {'id': lid, 'source': 'command', 'validUntil': t,
               'refresh': {'cmds': [{'type': 'modbus', 'sn': 'SN1', 'unit': 1, 'fc': 16, 'addr': 143,
                                     'values': [15400]}], 'everyS': 10},
               'onExpire': [{'type': 'modbus', 'sn': 'SN1', 'unit': 1, 'fc': 16, 'addr': 143, 'values': [15500]}]}
        if hard is not None:
            out['hardUntil'] = hard
        return dict(out, **kw)
    t0 = clock.t
    S['leases'] = lambda: [station(clock.t + 600), cmd_lease(clock.t + 180, hard=t0 + 905)]
    await _step(r, e)
    check('každý sync nese caps', all(b['client'].get('caps') == edge.client_caps() for b in e['posts']), e['posts'][-1])
    L = r.leases.get('L1') or {}
    check('pojistka stanice → cesta fallback (jako 0.3.0), pojistka příkazu do leases',
          r.fallback and r.fallback['id'] == 'st-1' and abs(r.fallback['deadline'] - (clock.t + 600)) < 1e-6
          and [w['addr'] for w in r.fallback['writes']] == [0x94, 0xA6, 0x92] and list(r.leases) == ['L1'], r.state())
    check('obnova: everyS nejméně 30 s, platnost = validUntil (hodiny klienta), uloženo',
          L.get('refresh', {}).get('everyS') == edge.REFRESH_MIN_S and abs(L['deadline'] - (clock.t + 180)) < 1e-6
          and e['saved'] and e['saved'][-1]['leases'][0]['id'] == 'L1', L)
    for _ in range(6):
        await _step(r, e)
    ref = [w for w in e['writes'] if w[1:] == (143, [15400])]
    check('obnova á 30 s (jeden zápis za 30 s), do limitu zápisů se nepočítá', len(ref) == 1 and r._writes == []
          and r.refreshes == 1, (e['writes'], r._writes))
    await _step(r, e, dt=30)
    check('další obnova po 30 s', len([w for w in e['writes'] if w[1:] == (143, [15400])]) == 2, e['writes'])
    r.control_enabled = False
    n = len(e['writes'])
    await _step(r, e, dt=31)
    check('přepínač vypnutý → žádná obnova', len(e['writes']) == n, e['writes'][n:])
    r.control_enabled = True
    S['leases'] = lambda: [station(clock.t + 600)]
    n = len(e['writes'])
    await _step(r, e)
    check('pojistka ze seznamu zmizela → zahodit BEZ onExpire', r.leases == {} and len(e['writes']) == n, e['writes'][n:])
    S['leases'] = lambda: [cmd_lease(clock.t + 180, onExpire=[{'type': 'modbus', 'sn': 'SN1', 'fc': 16, 'addr': 160,
                                                                 'values': [5200]}])]
    await _step(r, e)
    check('onExpire mimo profil → celá pojistka odmítnuta; bez pojistky stanice fallback None', r.leases == {}
          and r.fallback is None, r.leases)
    for bad in (cmd_lease(clock.t + 180, onExpire=[{'type': 'modbus', 'sn': 'JINY', 'fc': 16, 'addr': 143, 'values': [1]}]),
                cmd_lease(clock.t + 180, refresh={'cmds': [], 'everyS': 60}), cmd_lease(clock.t + 180, validUntil='x'),
                cmd_lease(clock.t + 180, onExpire=[{'type': 'mqtt', 'dev': 'x', 'op': 'write', 'values': [['a', 1]]}])):
        S['leases'] = [bad]
        await _step(r, e)
        if r.leases:
            break
    check('cizí SN / prázdná obnova / validUntil text / mqtt bez profilu → odmítnuto', r.leases == {}, r.leases)

    # výpadek serveru: obnova do platnosti, pak jednou onExpire, hlášení po návratu
    S['leases'] = lambda: [cmd_lease(clock.t + 60, lid='L2')]
    await _step(r, e)
    deadline = r.leases['L2']['deadline']
    S['down'] = True
    n = len(e['writes'])
    for _ in range(30):
        await _step(r, e)
    w = e['writes'][n:]
    check('bez spojení: obnova jen do platnosti, potom JEDNOU onExpire',
          all(t < deadline + 1 for t, a, v in w if v == [15400]) and [x for x in w if x[2] == [15500]]
          and len([x for x in w if x[2] == [15500]]) == 1 and min(t for t, a, v in w if v == [15500]) >= deadline, w)
    rep = r._lease_reports[0] if r._lease_reports else {}
    check('hlášení pojistky: id, source, ok, kroky s typem, adresou, hodnotami a zpětným čtením',
          rep.get('id') == 'L2' and rep.get('source') == 'command' and rep.get('ok') is True
          and rep['steps'][0]['type'] == 'modbus' and rep['steps'][0]['addr'] == 143
          and rep['steps'][0]['readBack'] == [15500] and e['saved'][-1]['leaseReports'][0]['id'] == 'L2', rep)
    r2, e2 = _mk(state=e['saved'][-1], regs=e['regs'])
    check('po restartu HA: pojistka i hlášení z uloženého stavu', 'L2' in r2.leases
          and r2.leases['L2']['appliedAt'] and r2._lease_reports and r2._lease_reports[0]['id'] == 'L2', r2.state())
    S['down'] = False
    S['leases'] = lambda: []
    await _step(r, e)
    sent = [b for b in e['posts'] if b.get('leaseReport')]
    check('spojení zpět → leaseReport odeslán, potvrzen (leaseAck), smazán; pojistka zahozena',
          sent and sent[-1]['leaseReport']['id'] == 'L2' and r._lease_reports == [] and r.leases == {}
          and e['saved'][-1]['leaseReports'] == [], r.state())

    # ── readBack podle zadání serveru (Solax: jiná mapa čtení) ──
    r, e = _mk(regs={143: 1, 500: 7, 501: 8})
    r.control_enabled = True
    res = await _step(r, e, cmds=[cmd('c-1', readBack=[{'fc': 3, 'addr': 143, 'count': 1},
                                                       {'fc': 4, 'addr': 500, 'count': 2}])])
    check('readBack spec: po zápisu přečte zadané bloky → [{fc, addr, count, values}]', res and res[0]['ok']
          and res[0]['readBack'] == [{'fc': 3, 'addr': 143, 'count': 1, 'values': [15400]},
                                     {'fc': 4, 'addr': 500, 'count': 2, 'values': [7, 8]}] and (4, 500, 2) in e['reads'], res)
    n = len(e['writes'])
    bad_specs = ([{'fc': 16, 'addr': 1, 'count': 1}], [{'fc': 3, 'addr': 1, 'count': 0}],
                 [{'fc': 3, 'addr': 1, 'count': 1}] * 5, 'x')
    res = await _step(r, e, cmds=[cmd(f'c-b{i}', readBack=s) for i, s in enumerate(bad_specs)])
    check('readBack neplatný (FC16 / 0 registrů / 5 bloků / text) → bad-command, nic nezapsáno',
          [x['err'] for x in res] == ['bad-command: readBack'] * 4 and len(e['writes']) == n, res)

    # ── profil zápisu od serveru ──
    S = e['srv']
    S['profile'] = 'neznamy@1'
    res = await _step(r, e, cmds=[cmd('c-p1'), cmd('c-p2', fc=3, addr=143, count=1)])
    check('server určí profil, který klient nemá → zápis odmítnut, čtení projde',
          res[0]['err'] == 'not-allowed: profil neznamy@1 v klientu není' and res[1]['ok'], res)
    S['profile'] = 'victron-gx@1'
    res = await _step(r, e, cmds=[cmd('c-p3')])
    check('profil mqtt na příkaz Modbus → odmítnut', 'není pro přenos modbus' in res[0]['err'], res)
    edge.PROFILES['test-mb@1'] = edge._wp.compile_profile({'id': 'test-mb@1', 'transport': 'modbus',
                                                           'write': [{'addr': 143, 'min': 0, 'max': 24000}]})
    edge.PROFILE_IDENTITY['test-mb@1'] = {'fc': 3, 'addr': 3, 'count': 5, 'decode': 'ascii'}
    try:
        sn_regs = {3 + i: v for i, v in enumerate([0x534E, 0x3100, 0, 0, 0])}     # 'SN1'
        e['regs'].update(sn_regs)
        S['profile'] = 'test-mb@1'
        res = await _step(r, e, cmds=[cmd('c-p4'), cmd('c-p5', addr=108, values=[100])])
        check('vlastní profil: identita podle profilu sedí na SN → zápis podle něj (108 v něm není)',
              res[0]['ok'] and res[1]['err'].startswith('not-allowed: registr 108') and r._verified.get('test-mb@1') is True
              and e['saved'][-1]['profile'] == 'test-mb@1', (res, r._verified))
        r3, e3 = _mk(sn='JINYSN', regs=dict(sn_regs))
        r3.control_enabled = True
        e3['srv']['profile'] = 'test-mb@1'
        res = await _step(r3, e3, cmds=[cmd('c-p6', sn='JINYSN')])
        check('identita podle profilu nesedí (jiné SN) → zápisy odmítnuty (chybný server nepošle cizí blok)',
              'identita zařízení neověřená' in res[0]['err'] and r3._verified == {'test-mb@1': False}
              and not e3['writes'], res)
    finally:
        edge.PROFILES.pop('test-mb@1', None)
        edge.PROFILE_IDENTITY.pop('test-mb@1', None)

    # ── profile null = jen čtení (server: baterie, střídač bez profilu zápisu) ──
    r7, e7 = _mk(regs={143: 1})
    r7.control_enabled = True
    S7 = e7['srv']
    S7.update(nullProfile=True, profile=None,
              leases=lambda: [cmd_lease(e7['clock'].t + 180, lid='N1')])
    res = await _step(r7, e7, cmds=[cmd('c-n1'), cmd('c-n2', fc=3, addr=143, count=1)])
    check('profile null → zápis odmítnut, čtení projde, pojistka příkazu odmítnuta, uloženo',
          r7.profile_id is None and 'jen ke čtení' in res[0]['err'] and res[1]['ok'] and not e7['writes']
          and r7.leases == {} and e7['saved'][-1]['profile'] is None, (res, r7.leases))
    r8, _e8 = _mk(state=e7['saved'][-1])
    r9, _e9 = _mk(state={'fallback': None})
    check('po restartu HA zůstane jen čtení; stav bez klíče profile (0.3.0) → Deye',
          r8.profile_id is None and r9.profile_id == edge.PROFILE_ID, (r8.profile_id, r9.profile_id))
    S7.update(nullProfile=False, leases=None)
    res = await _step(r7, e7, cmds=[cmd('c-n3', fc=3, addr=143, count=1)])
    check('server bez klíče profile (v1) „jen čtení“ nezruší', r7.profile_id is None and res[0]['ok'], res)
    S7['profile'] = edge.PROFILE_ID
    res = await _step(r7, e7, cmds=[cmd('c-n4')])
    check('server profil zase určí → zápis projde', r7.profile_id == edge.PROFILE_ID and res[0]['ok'], res)

    # ── přenos mqtt (Victron GX, zápis až A5 — tady falešný přenos) ──
    gx = FakeGx()
    P = FakeGx.D
    r, e = _mk(sn=gx.portal, plan=plan, transports={'mqtt': gx})
    S = e['srv']
    S['profile'] = 'victron-gx@1'
    await _step(r, e)
    body = e['posts'][-1]
    rd = {x['name']: x for x in body['reads'] if x.get('type') == 'mqtt'}
    check('mqtt: odběr filtrů z plánu (s base), čtení položek s dev, cesty bez base, tsBy celé sekundy',
          gx.filters == [P + 'Schedule/+/Soc', P + 'Mode', 'system/0/Dc/Battery/Soc'] and set(rd) == {'dess', 'live'}
          and rd['dess']['values'] == {'Mode': 0, 'Schedule/0/Soc': 20} and rd['dess']['dev'] == gx.portal
          and rd['dess']['tsBy'] == {'Mode': 999000, 'Schedule/0/Soc': 999000}
          and body['client']['caps']['transports'] == ['modbus', 'mqtt'], (gx.filters, rd))
    check('profil victron-gx@1: identita = portal přenosu', r._verified == {'victron-gx@1': True}, r._verified)
    mq = lambda cid, values, **kw: dict({'id': cid, 'type': 'mqtt', 'dev': gx.portal, 'op': 'write', 'values': values,
                                         'expiresInS': 10}, **kw)
    batch = [[P + 'Schedule/0/Soc', 50], [P + 'Mode', 4]]
    res = await _step(r, e, cmds=[mq('m-0', batch)])
    check('mqtt zápis s vypnutým přepínačem → local-off', res[0]['err'] == 'local-off' and not gx.writes, res)
    r.control_enabled = True
    res = await _step(r, e, cmds=[mq('m-1', batch), mq('m-2', [[P + 'Mode', 1]]), mq('m-3', batch, dev='deadbeef0001'),
                                  {'id': 'm-4', 'type': 'mqtt', 'dev': gx.portal, 'op': 'read', 'paths': [P + 'Mode']},
                                  mq('m-5', [[P + 'Schedule/0/Soc', 1]] * 2)])
    check('mqtt zápis: dávka v pořadí → ozvěna; Mode 1 / cizí GX / cesta dvakrát → odmítnuto; čtení cest',
          res[0]['ok'] and res[0]['echo'] == {P + 'Schedule/0/Soc': 50, P + 'Mode': 4} and gx.writes == [batch]
          and res[1]['err'].startswith('not-allowed') and res[2]['err'] == 'foreign-sn'
          and res[3]['ok'] and res[3]['values'] == {P + 'Mode': 4} and 'dvakrát' in res[4]['err'], res)
    gx2 = FakeGx(can_write=False)
    r4, e4 = _mk(sn=gx2.portal, plan=plan, transports={'mqtt': gx2})
    r4.control_enabled = True
    e4['srv']['profile'] = 'victron-gx@1'
    res = await _step(r4, e4, cmds=[mq('m-6', batch)])
    check('přenos bez zápisu (GX dnes jen čtení) → exec, nic nezapsáno', res[0]['err'] == 'exec: přenos mqtt neumí zápis', res)
    r5, e5 = _mk(sn='SN1')
    res = await _step(r5, e5, cmds=[mq('m-7', batch)])
    check('klient bez přenosu mqtt → bad-command', res[0]['err'] == 'bad-command: přenos mqtt není zapojený', res)
    gx.values.clear()
    r6, e6 = _mk(sn=gx.portal, plan=plan, transports={'mqtt': gx})
    await _step(r6, e6)
    check('přenos zatím nic nemá → položka se neposílá (neprázdný stav na serveru nepřepíše)',
          not [x for x in e6['posts'][-1]['reads'] if x.get('type') == 'mqtt'], e6['posts'][-1]['reads'])


class FakeEntities:
    """Přenos ha_entity jako transport_ha_entities.HaEntities (read_item → values + prefix)."""

    def __init__(self, values, prefix=None, domain='victron_gx'):
        self.values, self.prefix, self.domain = values, prefix, domain

    def read_item(self, it):
        if (it.get('integration') or self.domain) != self.domain:
            return None
        return dict({'values': dict(self.values)}, **({'prefix': self.prefix} if self.prefix else {}))


async def transports_a5():
    print('— přenosy bez Modbusu (A5: GX přímo, entity jiné integrace)')
    c1 = edge.client_caps(('mqtt',), modbus=False)
    c2 = edge.client_caps(('ha_entity',), modbus=False, integrations=('victron_gx', 'Zlá-doména', 'victron_gx'))
    c3 = edge.client_caps(integrations=('solarman',))
    check('caps: GX bez modbus a bez integrations; entity s doménou (jen platné, bez duplicit); Deye + solarman',
          c1['transports'] == ['mqtt'] and 'integrations' not in c1 and c2['transports'] == ['ha_entity']
          and c2['integrations'] == ['victron_gx'] and c3['transports'] == ['modbus']
          and c3['integrations'] == ['solarman'], (c1, c2, c3))
    plan = [{'type': 'modbus', 'fc': 3, 'addr': 586, 'count': 3, 'everyS': 30},
            {'type': 'mqtt', 'name': 'live', 'paths': ['system/0/Dc/Battery/Soc'], 'everyS': 30}]
    gx = FakeGx()
    r, e = _mk(sn=gx.portal, plan=plan, transports={'mqtt': gx}, modbus=False)
    await _step(r, e)
    body = e['posts'][-1]
    check('GX: žádné bloky Modbus (ani z plánu), čtení mqtt, caps bez modbus',
          r.plan == [] and not e['reads'] and [x.get('type') for x in body['reads']] == ['mqtt']
          and body['client']['caps']['transports'] == ['mqtt'], (r.plan, e['reads'], body))
    e['srv']['readPlan'] = plan
    res = await _step(r, e, dt=40, cmds=[cmd('c-a5', fc=3, addr=586, count=3, sn=gx.portal)])
    check('GX: plán ze sync s blokem modbus → bloky se nečtou; příkaz modbus → bad-command',
          r.plan == [] and not e['reads'] and res and res[0]['err'] == 'bad-command: přenos modbus není zapojený',
          (r.plan, e['reads'], res))
    ki = edge.clean_items([{'type': 'ha_entity', 'name': 'sx', 'integration': 'solax_modbus', 'everyS': 30,
                            'keys': ['battery_capacity', 'run_mode']},
                           {'type': 'ha_entity', 'name': 'zle', 'integration': 'solax_modbus', 'everyS': 30,
                            'keys': [1, 2]}], {'ha_entity': object()})
    check('clean_items: ha_entity si nechá keys (jen seznam krátkých textů)',
          ki[0].get('keys') == ['battery_capacity', 'run_mode'] and 'keys' not in ki[1], ki)
    ents = {'sensor.victron_battery_soc': {'state': '55.5', 'unit': '%', 'uid': 'c0ffee000001_battery_512_battery_soc'}}
    pl = [{'type': 'ha_entity', 'name': 'victron_gx', 'integration': 'victron_gx', 'everyS': 30},
          {'type': 'ha_entity', 'name': 'jina', 'integration': 'solax_modbus', 'everyS': 30}]
    r2, e2 = _mk(sn='c0ffee000001', plan=pl, transports={'ha_entity': FakeEntities(ents, prefix='Hub')},
                 modbus=False, integrations=('victron_gx',))
    await _step(r2, e2)
    b2 = e2['posts'][-1]
    check('entity: položka své integrace s values a prefix, cizí integrace se neposílá, caps s doménou',
          [x['name'] for x in b2['reads']] == ['victron_gx'] and b2['reads'][0]['values'] == ents
          and b2['reads'][0]['prefix'] == 'Hub' and b2['client']['caps'].get('integrations') == ['victron_gx']
          and b2['client']['caps']['transports'] == ['ha_entity'], b2)


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
    # Profil MQTT (A3b): položky s cestou, vzory {0..N} a {n}, dávka s maxPaths, persist.
    m = wp.compile_profile({'id': 'test-mqtt@1', 'transport': 'mqtt', 'limits': {'maxPaths': 3},
                            'write': [{'path': 'settings/0/X/Mode', 'check': 'enum', 'values': [0, 4], 'persist': True},
                                      {'path': 'settings/0/X/Slot/{0..2}/Soc', 'min': 0, 'max': 100, 'int': True},
                                      {'path': 'dev/{n}/Current', 'min': 6.0, 'max': 32.0}]})
    P = lambda p, v: wp.path_problem(m, p, v)
    check('profil MQTT: vzory, celá čísla, desetinná čísla, enum; cizí cesta / text / NaN ne',
          P('settings/0/X/Mode', 4) is None and P('settings/0/X/Slot/2/Soc', 55) is None
          and P('dev/17/Current', 6.5) is None and P('settings/0/X/Mode', 1) and P('settings/0/X/Slot/3/Soc', 5)
          and P('settings/0/X/Slot/1/Soc', 5.5) and P('dev/x/Current', 10) and P('dev/1/Current', 32.5)
          and P('dev/1/Current', 'x') and P('dev/1/Current', float('nan')) and P('dev/1/Current', True)
          and P('settings/0/X/Mode\n', 4))
    check('profil MQTT: dávka do maxPaths, bez opakování cesty; persist jen u označených',
          wp.batch_problem(m, [['settings/0/X/Slot/0/Soc', 1], ['settings/0/X/Mode', 4]]) is None
          and wp.batch_problem(m, [['dev/1/Current', 6]] * 2) and wp.batch_problem(m, [])
          and wp.batch_problem(m, [['dev/%d/Current' % i, 6] for i in range(4)])
          and wp.persistent(m, 'settings/0/X/Mode') and not wp.persistent(m, 'dev/1/Current'))
    bad = []
    for prof in ({'id': 'x@1', 'transport': 'mqtt', 'write': [{'path': 'a', 'min': 0, 'max': 1}]},
                 {'id': 'x@1', 'transport': 'mqtt', 'limits': {'maxPaths': 1}, 'write': [{'path': 'a/#', 'min': 0, 'max': 1}]},
                 {'id': 'x@1', 'write': [{'path': 'a', 'min': 0, 'max': 1}]}):
        try:
            wp.compile_profile(prof)
            bad.append(prof)
        except ValueError:
            pass
    check('profil MQTT: bez maxPaths / zástupný znak / cesta v profilu modbus = neplatný', not bad, bad[:1])


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

def mk_battery_plan(t0, pattern, pid='p#aaa', valid_s=3 * 3600, tz=None):
    """Neutrální plán jako ze serveru (deye/plan_neutral.py): sloty po 900 s od t0, okna ze shodných akcí."""
    from datetime import datetime, timedelta, timezone
    tz = tz or timezone(timedelta(hours=2))
    t = [t0 + 900 * i for i in range(len(pattern))]
    pw = [5000 if a == 'charge_grid' else 3000 if a == 'discharge_grid' else 0 for a in pattern]
    soc = [50.0 + i for i in range(len(pattern))]
    iso = lambda x: datetime.fromtimestamp(x, tz).isoformat()
    wins, i0 = [], 0
    for i in range(1, len(t) + 1):
        if i == len(t) or pattern[i] != pattern[i0]:
            wins.append({'from': iso(t[i0]), 'to': iso(t[i] if i < len(t) else t[-1] + 900), 'action': pattern[i0],
                         'powerW': max(pw[i0:i]), 'socEnd': soc[i - 1]})
            i0 = i
    return {'v': 1, 'id': pid, 'computedAt': t0, 'validUntil': t0 + valid_s, 'reservePct': 15.0, 'currency': 'CZK',
            'tz': 'Europe/Prague', 'windows': wins,
            'slots': {'t': t, 'action': list(pattern), 'powerW': pw, 'soc': soc, 'buy': [3.1] * len(t),
                      'sell': [1.9] * len(t)}}


async def plan_a9():
    print('— plán jako data (A9: planId, plan, uložení, sloty, okna, export)')
    check('caps: vlastnost plan', 'plan' in edge.client_caps()['features'])
    PAT = ['charge_grid'] * 4 + ['hold'] * 4 + ['self_use'] * 4
    p = mk_battery_plan(1_000_000 - 1_000_000 % 900, PAT)
    c = edge.clean_battery_plan(p)
    check('clean: platný plán projde, okna dostanou fromTs / toTs', c and c['id'] == 'p#aaa'
          and [w['action'] for w in c['windows']] == ['charge_grid', 'hold', 'self_use']
          and c['windows'][1]['fromTs'] == p['slots']['t'][4] and c['windows'][-1]['toTs'] == p['slots']['t'][-1] + 900, c)
    import copy
    bad = []
    for mut in (lambda q: q.update(v=2), lambda q: q['slots']['action'].__setitem__(0, 'sell_all'),
                lambda q: q['slots']['soc'].pop(), lambda q: q['slots']['t'].__setitem__(1, q['slots']['t'][0]),
                lambda q: q['windows'][0].update(to=q['windows'][0]['from']),
                lambda q: q['windows'][0].update(**{'from': '2026-10-19T14:00:00'}),
                lambda q: q.update(validUntil='zítra'), lambda q: q['slots'].update(t=[]),
                lambda q: q['slots']['buy'].__setitem__(0, '3,1')):
        q = copy.deepcopy(p)
        mut(q)
        bad.append(edge.clean_battery_plan(q))
    check('clean: verze, neznámá akce, různé délky, čas neroste, prázdné okno, okno bez zóny, validUntil, '
          'prázdné sloty, text místo čísla → None', bad == [None] * 9, bad)
    r, e = _mk()
    S, clock = e['srv'], e['clock']
    clock.t = p['slots']['t'][0] + 60
    srv_plan = {'p': p, 'id': p['id']}

    async def post(path, body, token):
        e['posts'].append(body)
        res = {'ok': True, 'serverTime': clock.t, 'nextPollS': 5, 'control': 'on', 'commands': [], 'accepted': 0}
        if srv_plan['id'] is not False:
            res['planId'] = srv_plan['id']
            if srv_plan['id'] and body['client'].get('planId') != srv_plan['id']:
                res['plan'] = srv_plan['p']
        return 200, res
    r._post = post
    await _step(r, e)
    check('1. sync bez planId → plán přijat a uložen (HA Store)', r.battery_plan and r.battery_plan['id'] == 'p#aaa'
          and 'planId' not in e['posts'][-1]['client'] and (e['saved'][-1].get('plan') or {}).get('id') == 'p#aaa',
          (r.battery_plan, e['posts'][-1]['client']))
    n_saved = len(e['saved'])
    await _step(r, e)
    check('další sync posílá planId, stejné id → beze změny a bez ukládání',
          e['posts'][-1]['client'].get('planId') == 'p#aaa' and len(e['saved']) == n_saved)
    i = edge.plan_slot(r.battery_plan, clock.t)
    check('plan_slot / plan_window: teď 1. slot nákupu, okno 0', i == 0 and edge.plan_window(r.battery_plan, clock.t) == 0
          and edge.plan_slot(r.battery_plan, p['slots']['t'][5] + 1) == 5
          and edge.plan_window(r.battery_plan, p['slots']['t'][5] + 1) == 1, i)
    check('plan_slot: před začátkem, po konci slotů a po validUntil → None',
          edge.plan_slot(r.battery_plan, p['slots']['t'][0] - 1) is None
          and edge.plan_slot(r.battery_plan, p['slots']['t'][-1] + 900) is None
          and edge.plan_slot(dict(r.battery_plan, validUntil=clock.t), clock.t) is None)
    x = edge.plan_export(r.battery_plan, clock.t)
    check('plan_export: 3 okna a 12 čtvrthodin s ISO časy, akce, výkon, SOC, ceny', x and len(x['slots']) == 12
          and len(x['windows']) == 3 and x['slots'][0]['action'] == 'charge_grid' and x['slots'][0]['power_w'] == 5000
          and x['slots'][0]['price_buy'] == 3.1 and x['slots'][-1]['end'] == edge._iso(p['slots']['t'][-1] + 900)
          and x['windows'][0]['target_soc'] == 53.0, x and x['slots'][:1])
    check('plan_export po validUntil → None', edge.plan_export(r.battery_plan, p['validUntil']) is None)
    p2 = mk_battery_plan(p['slots']['t'][0], ['discharge_grid'] * 12, pid='p#bbb')
    srv_plan.update(p=p2, id='p#bbb')
    await _step(r, e)
    check('nové id → nový plán přijat a uložen', r.battery_plan['id'] == 'p#bbb'
          and e['saved'][-1]['plan']['id'] == 'p#bbb')
    srv_plan.update(p=dict(p2, id='p#ccc', slots=dict(p2['slots'], action=['x'] * 12)), id='p#ccc')
    await _step(r, e)
    check('neplatný plán od serveru → ponechán předchozí', r.battery_plan['id'] == 'p#bbb')
    srv_plan.update(id=False)
    await _step(r, e)
    check('server bez vlastnosti plan (bez planId) → plán beze změny', r.battery_plan['id'] == 'p#bbb')
    srv_plan.update(id=None)
    await _step(r, e)
    check('planId None → plán zahozen i z uloženého stavu', r.battery_plan is None and e['saved'][-1]['plan'] is None)
    r2 = edge.EdgeRunner(read_block=None, post=post, token='T', read_plan=[], version='t', backend='f',
                         state={'plan': p2})
    check('obnova z uloženého stavu (restart HA)', r2.battery_plan and r2.battery_plan['id'] == 'p#bbb')
    # změna času: slot 02:45 letního času trvá do 03:00 zimního (1 h 15 min)
    from datetime import datetime, timedelta, timezone
    t245 = int(datetime(2026, 10, 25, 2, 45, tzinfo=timezone(timedelta(hours=2))).timestamp())
    dst = mk_battery_plan(t245, ['hold', 'charge_grid'])
    dst['slots']['t'][1] = t245 + 4500
    dst['windows'][0]['to'] = dst['windows'][1]['from'] = datetime.fromtimestamp(t245 + 4500, timezone(timedelta(hours=1))).isoformat()
    dst['windows'][1]['to'] = datetime.fromtimestamp(t245 + 5400, timezone(timedelta(hours=1))).isoformat()
    dc = edge.clean_battery_plan(dst)
    check('změna času: dvojitá hodina — slot trvá do dalšího t, okno s offsetem +01:00',
          dc and edge.plan_slot(dc, t245 + 4000) == 0 and edge.plan_slot(dc, t245 + 4500) == 1
          and dc['windows'][1]['fromTs'] == t245 + 4500, dc and dc['windows'])


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--url')
    ap.add_argument('--code')
    ap.add_argument('--sn')
    a = ap.parse_args()
    asyncio.run(unit())
    asyncio.run(control())
    asyncio.run(fallback())
    asyncio.run(proto2())
    asyncio.run(transports_a5())
    asyncio.run(plan_a9())
    profile_data()
    parity()
    if a.url:
        asyncio.run(e2e(a.url, a.code, a.sn))
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    sys.exit(1 if FAILS else 0)
