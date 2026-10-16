"""E2E klient ↔ server v jednom procesu: skutečné jádro klienta (custom_components/wattery/edge.py) proti skutečnému
serveru deye-monitor (edge.sync, edge_cmd, DeyeClient.custom_control_sync, modbus_write, spot_auto TOU) nad dočasnou
DB, bez sítě (Deye cloud = záznam volání). Potřebuje vedle repa checkout deye-monitor.
  python tests/server_e2e.py
Zkouší: párování → stínová data s ověřeným SN; řízení na serveru × přepínač u klienta (ctl) → kudy jde příkaz;
zápis 143 se zpětným čtením, TOU (3 bloky) a hodiny přes klienta; klient odmítne zápis mimo svůj profil, i když ho
server pošle; vypnutý přepínač → cloud; klient přestal pollovat → zrušeno + cloud."""
import asyncio
import importlib.util
import os
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DM = os.path.abspath(os.path.join(HERE, '..', '..', 'deye-monitor'))
TMP = tempfile.mkdtemp(prefix='wattery-e2e-')
os.environ.update({'DEYE_DB_PATH': os.path.join(TMP, 'e2e.db'), 'DEYE_EDGE': '1', 'DEYE_EDGE_CONTROL': '1',
                   'HTTPS_PROXY': 'http://127.0.0.1:9', 'HTTP_PROXY': 'http://127.0.0.1:9',
                   'DEYE_SECRET_KEY': 'jen-e2e', 'PYTHONUTF8': '1'})
sys.path.insert(0, DM)
sys.path.insert(0, os.path.join(DM, 'tools'))
from deye import edge as srv, edge_cmd, modbus, modbus_write, spot_auto, store  # noqa: E402
from deye.client import DeyeClient  # noqa: E402
import edge_sim  # noqa: E402

store.DB_PATH = os.environ['DEYE_DB_PATH']
store._initialized = False
_spec = importlib.util.spec_from_file_location('wattery_edge', os.path.join(HERE, '..', 'custom_components', 'wattery',
                                                                             'edge.py'))
cedge = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cedge)

FAILS = []


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


async def main():
    store.init()
    sid, sn, uid = 990101, 'E2ESN00001', 901
    store.upsert_station({'id': sid, 'name': 'E2E'}, user_id=uid)
    store.upsert_device({'deviceSn': sn, 'deviceType': 'INVERTER'}, station_id=sid, user_id=uid)
    store.grant_station_access(sid, uid)
    store.grant_device_access(sn, uid)
    code = srv.create_pair_code(sid, uid)['code']
    st, res = srv.pair({'code': code, 'kind': 'ha', 'sn': sn, 'clientVer': 'ha-int/e2e', 'label': 'e2e'})
    check('párování', st == 200 and res.get('token'), (st, res))
    token, client_id = res['token'], res['clientId']

    regs = edge_sim.fake_regs(sn, time.time())
    regs.update({143: 15500, 108: 100})
    writes = []

    async def read_block(fc, addr, count):
        await asyncio.sleep(0.05)              # ha-solarman přes Wi-Fi stick něco trvá
        return [regs.get(a, 0) for a in range(addr, addr + count)]

    async def write_block(addr, values):
        await asyncio.sleep(0.1)
        writes.append((addr, list(values)))
        regs.update({addr + i: v for i, v in enumerate(values)})

    async def post(path, body, tok):
        cl = srv.client_by_token(tok)
        return (401, {'ok': False}) if cl is None else srv.sync(cl, body)

    runner = cedge.EdgeRunner(read_block=read_block, write_block=write_block, post=post, token=token, sn=sn,
                              read_plan=res['readPlan'], version='ha-int/e2e', backend='e2e',
                              next_poll_s=res.get('nextPollS', 10))
    task = asyncio.create_task(runner.run())
    cloud = []
    cli = DeyeClient.__new__(DeyeClient)
    cli._custom_control_sync = lambda s, hx, *a, **k: (cloud.append(hx) or {
        'ok': True, 'orderId': 'cloud', 'analysisResult': edge_cmd.write_echo(1, 143, 1)})
    loop = asyncio.get_running_loop()

    async def platform(fn, *args, **kw):
        """Volání platformy (blokující — jako worker serveru) mimo smyčku klienta. → (výsledek, sekundy)."""
        t0 = time.time()
        out = await loop.run_in_executor(None, lambda: fn(*args, **kw))
        return out, time.time() - t0

    async def until(cond, timeout=15):
        for _ in range(int(timeout / 0.1)):
            if cond():
                return True
            await asyncio.sleep(0.1)
        return False

    try:
        ok = await until(lambda: runner.status == cedge.ONLINE and runner.last_accepted)
        with store._conn() as c:
            snap = c.execute("SELECT data_json FROM edge_snapshots WHERE device_sn=?", (sn,)).fetchone()
        check('klient online, stínová data s ověřeným SN', ok and snap and '"snVerified": true' in snap['data_json'],
              (runner.status, runner.last_error))
        f143 = modbus.build_write_multiple(1, 143, [15400])
        r, _ = await platform(cli.custom_control_sync, sn, f143, timeout_seconds=60)
        check('bez řízení na serveru → zápis přes cloud', r.get('orderId') == 'cloud' and regs[143] == 15500, r)

        edge_cmd.set_control(client_id, 'on')
        t0 = time.time()
        ok = await until(lambda: runner.server_control == 'on', 45)
        check(f'server: řízení „on" → klient se to dozví příštím sync ({time.time() - t0:.0f} s) a ptá se á 5 s',
              ok and runner.next_poll_s == edge_cmd.POLL_S, (runner.server_control, runner.next_poll_s))
        cloud.clear()
        r, _ = await platform(cli.custom_control_sync, sn, f143, timeout_seconds=60)
        check('přepínač v HA vypnutý → server pošle přes cloud, do střídače přes HA nic',
              r.get('orderId') == 'cloud' and len(cloud) == 1 and not writes, r)

        runner.set_control(True)
        t0 = time.time()
        ok = await until(lambda: edge_cmd.route(sn) is not None, 15)
        check(f'přepínač v HA zapnut → server směruje přes HA do {time.time() - t0:.1f} s', ok)
        cloud.clear()
        w, dt = await platform(modbus_write.write_register, cli, sn, 'max_sell_power', 15400)
        check(f'modbus_write 143 = 15400 přes HA: zápis + ověření zpětným čtením ({dt:.1f} s)',
              w.get('ok') and w.get('verified') is True and w.get('echoAck') is True and regs[143] == 15400
              and not cloud and str(w.get('orderId', '')).startswith('edge-'), w)
        check('vyzvednutí do 5 s (poll), celkem do 8 s', dt < 8, dt)
        tou6 = [{'time': t, 'power': 5000, 'soc': 20 + i, 'enableGridCharge': i == 1, 'enableSell': i == 4}
                for i, t in enumerate(('00:01', '05:00', '09:00', '13:00', '17:00', '21:00'))]
        run, dt = await platform(spot_auto._apply_tou_modbus, cli, sn, tou6, {}, False)
        check(f'TOU 3 bloky přes HA ({dt:.1f} s; další blok po rychlém dotazu)', run.get('ok') and regs.get(148) == 1
              and regs.get(153) == 2100 and regs.get(166) == 20 and regs.get(173) == 1 and regs.get(176) == 0x20
              and regs.get(146) == 0xFF and not cloud, run)
        check('TOU do 12 s (1. blok ≤ 5 s poll, další á 1,5 s)', dt < 12, dt)
        clk = [(26 << 8) | 10, (15 << 8) | 18, (5 << 8) | 7]
        r, _ = await platform(cli.custom_control_sync, sn, modbus.build_write_multiple(1, 62, clk), timeout_seconds=60)
        check('hodiny 62–64 přes HA (zpětné čtení hodin s tolerancí)', r.get('ok') and r.get('via') == 'edge'
              and regs[64] == clk[2], r)
        rr, _ = await platform(cli.read_modbus_registers, sn, 586, 10)
        check('čtení FC03 přes HA', rr.get('ok') and rr['values'] == [regs.get(586 + i, 0) for i in range(10)], rr)

        # Server (chybou / útokem) pošle zápis mimo profil — klient ho sám odmítne.
        cl_row = srv.client_by_token(token)
        cid = edge_cmd.enqueue(cl_row, sn, {'fc': 16, 'addr': 160, 'count': 1, 'values': [5200]}, '', 'e2e')
        row, _ = await platform(edge_cmd.wait, cid)
        check('klient odmítne zápis mimo svůj profil (napětí TOU), i když ho server pošle',
              row['status'] == 'done' and not row['ok'] and (row['error'] or '').startswith('not-allowed')
              and 160 not in regs, row)

        runner.set_control(False)
        t0 = time.time()
        ok = await until(lambda: edge_cmd.route(sn) is None, 15)
        n = len(writes)
        r, _ = await platform(cli.custom_control_sync, sn, f143, timeout_seconds=60)
        check(f'přepínač vypnut → server to ví do {time.time() - t0:.1f} s, zápis zase přes cloud',
              ok and r.get('orderId') == 'cloud' and len(writes) == n, r)
        runner.set_control(True)
        await until(lambda: edge_cmd.route(sn) is not None, 15)

        # Offline pojistka: ozbrojené obchodování, zkouška výpadku z CLI (krátká platnost, pak server 503).
        params = {'gridKw': 10, 'inverterKw': 12, 'reservePct': 15, 'batteryKwh': 10}
        now = int(time.time())
        store.save_spot_auto_config(sid, uid, sn, True, 'arbitrage', params, now, auto_apply='apply')
        store.set_spot_auto_applied(sid, 'sigE2E', now - 600, {'ok': True, 'dryRun': False, 'masterSn': sn,
                                                               'touBlocks': {}, 'tou6': [{'time': '00:01'}]})
        ok = await until(lambda: runner.fallback is not None, 15)
        exp = [w['values'] for w in srv.sync(srv.client_by_token(token), {'reads': []})[1]['fallback']['writes']]
        check('ozbrojeno → klient má pojistku (neutrál ze serveru prošel jeho profilem)', ok
              and [w['values'] for w in runner.fallback['writes']] == exp, runner.fallback)
        n = len(writes)
        edge_cmd.set_fallback_test(client_id, hold_s=4, outage_in_s=7, outage_s=20)
        t0 = time.time()
        ok = await until(lambda: runner.fallback_applied_at is not None, 30)
        check(f'výpadek serveru (503) po platnosti → klient zapsal neutrál sám ({time.time() - t0:.0f} s)',
              ok and [a for a, _ in writes[n:]] == [0x94, 0xA6, 0x92] and regs[166] == 15 and regs[146] == 0xFF
              and runner.status == cedge.ERROR, (writes[n:], runner.status, runner.last_error))
        ok = await until(lambda: runner._fb_report is None and runner.status == cedge.ONLINE, 30)
        with store._conn() as c:
            aud = [dict(x) for x in c.execute("SELECT addr, ok, status FROM edge_commands WHERE client_id=? AND "
                                              "source='fallback' ORDER BY id", (client_id,))]
        last = (store.get_spot_auto_config(sid) or {}).get('lastApply') or {}
        check('spojení zpět → hlášení přijato: audit 3× fallback OK, plán se přepíše (last_applied_at 0, neutrál)',
              ok and [(a['addr'], a['ok']) for a in aud] == [(0x94, 1), (0xA6, 1), (0x92, 1)]
              and last.get('edgeFallback') and (store.get_spot_auto_config(sid) or {}).get('lastAppliedAt') == 0,
              (aud, last.get('edgeFallback'), (store.get_spot_auto_config(sid) or {}).get('lastAppliedAt')))
        n = len(writes)
        await asyncio.sleep(6)
        check('po obnovení nová pojistka a už žádný zápis navíc', len(writes) == n and runner.fallback
              and runner.fallback['deadline'] > runner.fallback_applied_at, (writes[n:], runner.fallback))
        edge_cmd.set_fallback_test(client_id)

        task.cancel()
        edge_cmd.DELIVER_WAIT_S = 3
        cloud.clear()
        r, dt = await platform(cli.custom_control_sync, sn, f143, timeout_seconds=60)
        check(f'klient přestal pollovat (HA restart) → po {dt:.1f} s zrušeno a záloha přes cloud',
              r.get('orderId') == 'cloud' and edge_cmd.recent(client_id, 1)[0]['status'] == 'cancelled', r)
        log = edge_cmd.recent(client_id, 50)
        check('audit: každý příkaz v edge_commands se zdrojem a výsledkem',
              all(x['status'] in ('done', 'cancelled') for x in log) and any(x['source'] for x in log),
              [(x['id'], x['status'], x['source']) for x in log])
    finally:
        task.cancel()
    print('VŠE OK' if not FAILS else f'SELHALO: {", ".join(FAILS)}')


if __name__ == '__main__':
    asyncio.run(main())
    sys.exit(1 if FAILS else 0)
