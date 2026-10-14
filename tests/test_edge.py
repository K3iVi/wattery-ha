"""Testy jádra (bez Home Assistantu): python tests/test_edge.py

Volitelně E2E proti běžícímu serveru deye-monitor s DEYE_EDGE=1 (lokálně!):
  python tests/test_edge.py --url http://127.0.0.1:3000 --code ABCD-EFGH --sn 2203084752
"""
import argparse
import asyncio
import importlib.util
import json
import os
import sys
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
    check('sync s tokenem a verzí', posts[-1][0] == edge.SYNC_PATH and posts[-1][2] == 'T'
          and body['client'] == {'ver': 'test/1', 'backend': 'fake'}, posts[-1])
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
    check('příkazy se ignorují (nic nezapisuje)', all(fc in (3, 4) for fc, _, _ in reads_log))

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
    if a.url:
        asyncio.run(e2e(a.url, a.code, a.sn))
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')
    sys.exit(1 if FAILS else 0)
