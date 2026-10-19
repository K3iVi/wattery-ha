"""Testy přenosu MQTT na Victron GX (bez Home Assistantu, bez sítě): python tests/test_transport_mqtt_gx.py

Proti simulátoru brokeru GX (tests/gx_sim.py): přihlášení heslem, zjištění portal ID, odběr podle plánu čtení,
změny hodnot, zmizelá hodnota, keepalive se suppress-republish, ŽÁDNÉ publikování mimo R/<portal>/keepalive,
otisk certifikátu. Když je vedle repa checkout deye-monitor, projde i celý řetězec plán serveru → přenos →
dekódování adaptérem (deye/edge_victron.py).
"""
import asyncio
import importlib.util
import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import gx_sim  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    'wattery_gx', os.path.join(_HERE, '..', 'custom_components', 'wattery', 'transport_mqtt_gx.py'))
gx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gx)

FAILS = []


def check(name, ok, detail=''):
    print(('OK   ' if ok else 'FAIL ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


PLAN = [{'name': 'live', 'type': 'mqtt', 'everyS': 30, 'paths': ['system/0/Dc/Battery/Soc', 'system/0/Ac/Grid/+/Power']},
        {'name': 'meter', 'type': 'mqtt', 'everyS': 60, 'paths': ['grid/+/Ac/Energy/Forward', 'solarcharger/+/Pv/+/P']},
        {'name': 'dess', 'type': 'mqtt', 'everyS': 300, 'paths': ['settings/0/Settings/DynamicEss/Schedule/+/Soc']}]


async def settle(c, s=0.3):
    end = asyncio.get_event_loop().time() + s
    while asyncio.get_event_loop().time() < end:
        await c.poll(0.05)


async def scenario():
    b = await gx_sim.GxBroker(password='tajne').start()
    try:
        bad = gx.GxMqtt('127.0.0.1', b.port, tls=False, password='spatne')
        try:
            await bad.start()
            check('špatné heslo → chyba', False)
        except gx.GxError as e:
            check('špatné heslo → GxAuthError „GX odmítl heslo“', 'heslo' in str(e) and isinstance(e, gx.GxAuthError), e)
        await bad.close()

        t = [1000.0]
        c = gx.GxMqtt('127.0.0.1', b.port, tls=False, password='tajne', clock=lambda: t[0] + asyncio.get_event_loop().time())
        await c.set_filters([f for it in PLAN for f in it['paths']] + ['../W/x', 'a/b+/c', 'x/#/y', ''])
        check('neplatné filtry ze serveru se zahodí', len(c.filters) == 5, c.filters)
        await c.start()
        check('portal ID zjištěno z N/+/system/0/Serial', c.portal == gx_sim.PORTAL)
        await settle(c)
        r = c.read_item(PLAN[0])
        check('čtení položky plánu: SOC a 3 fáze sítě', r['type'] == 'mqtt' and r['dev'] == gx_sim.PORTAL
              and r['values'].get('system/0/Dc/Battery/Soc') == 64.5
              and sorted(k for k in r['values'] if '/Grid/' in k) == [f'system/0/Ac/Grid/L{k}/Power' for k in (1, 2, 3)],
              r)
        check('jen odebírané cesty (nic navíc ze stromu)', 'system/0/Dc/Pv/Power' not in c.values
              and len(c.read_item(PLAN[2])['values']) == 48)
        m = c.read_item(PLAN[1])['values']
        check('wildcard instance a tracker', m.get('grid/30/Ac/Energy/Forward') == 1000.5
              and m.get('solarcharger/280/Pv/1/P') == 900.0)
        b.set('system/0/Dc/Battery/Soc', 65.0)
        b.set('system/0/Ac/Grid/L2/Power', None)
        await settle(c)
        r = c.read_item(PLAN[0])
        check('změna hodnoty dorazí; zmizelá hodnota = None', r['values']['system/0/Dc/Battery/Soc'] == 65.0
              and r['values']['system/0/Ac/Grid/L2/Power'] is None)
        n_full = len([x for x in b.client_topics if x.endswith('/keepalive')])
        t[0] += gx.GX_KEEPALIVE_S + 1
        await settle(c, 0.2)
        check('keepalive po 30 s (suppress-republish, ne plný přehled)',
              len([x for x in b.client_topics if x.endswith('/keepalive')]) == n_full + 1)
        await c.set_filters(['evcharger/+/SetCurrent'])
        await settle(c)
        check('nový filtr za běhu → hned hodnoty', c.values.get('evcharger/40/SetCurrent') == 10)
        try:
            c._publish(f'W/{c.portal}/settings/0/Settings/DynamicEss/Mode', {'value': 4})
            refused = False
        except gx.GxError:
            refused = True
        check('publikování W/ odmítnuto (klient jen čte)', refused)
        check('klient nikdy neposlal nic jiného než R/<portal>/keepalive',
              b.client_topics and all(x == f'R/{gx_sim.PORTAL}/keepalive' for x in b.client_topics), b.client_topics)
        await c.close()
        try:
            c.read_item(PLAN[0])
            stale = True
        except gx.GxError:
            stale = False
        check('po ztrátě spojení se staré hodnoty nečtou (GxError)', not stale)
    finally:
        await b.stop()

    b2 = await gx_sim.GxBroker(password=None).start()
    try:
        c2 = gx.GxMqtt('127.0.0.1', b2.port, tls=False, portal=gx_sim.PORTAL)
        await c2.set_filters(['system/0/Dc/Battery/Soc'])
        await c2.start()
        await settle(c2)
        check('Unsecured (1883 bez hesla), zadané portal ID', c2.values.get('system/0/Dc/Battery/Soc') == 64.5)
        await c2.close()
    finally:
        await b2.stop()

    c3 = gx.GxMqtt('127.0.0.1', 1, tls=False)
    try:
        await c3.start()
        check('nedostupné GX → GxError', False)
    except gx.GxError:
        check('nedostupné GX → GxError', True)


def pin():
    c = gx.GxMqtt('h', tls=True)
    c._check_pin(b'cert-A')
    fp = c.cert_sha256
    ok = fp == gx.cert_fingerprint(b'cert-A')
    c2 = gx.GxMqtt('h', tls=True, cert_sha256=fp)
    c2._check_pin(b'cert-A')
    try:
        c2._check_pin(b'cert-B')
        changed = False
    except gx.GxError:
        changed = True
    check('otisk certifikátu: první spojení uloží, změna = odmítnout', ok and changed)
    c3 = gx.GxMqtt('h', tls=True)
    try:
        c3._check_pin(b'')
        empty = False
    except gx.GxCertError:
        empty = c3.cert_sha256 is None
    check('TLS bez certifikátu → GxCertError, otisk se neuloží', empty)
    try:
        c2._check_pin(b'cert-B')
    except gx.GxError as e:
        check('změna certifikátu = GxCertError (podtřída GxError)', isinstance(e, gx.GxCertError), type(e))


async def tls():
    """TLS jako Secured profil GX: certifikát podepsaný sám sebou (openssl), otisk při prvním spojení, pak pin."""
    import shutil
    import ssl
    import subprocess
    exe = shutil.which('openssl')
    if not exe:
        print('SKIP TLS (není openssl)')
        return
    d = tempfile.mkdtemp(prefix='gx-tls-')
    key, crt = os.path.join(d, 'k.pem'), os.path.join(d, 'c.pem')
    subprocess.run([exe, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key, '-out', crt, '-days', '2',
                    '-subj', '/CN=venus.local'], check=True, capture_output=True)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(crt, key)
    b = await gx_sim.GxBroker(password='tajne', ssl=ctx).start()
    try:
        c = gx.GxMqtt('127.0.0.1', b.port, tls=True, password='tajne')
        await c.set_filters(['system/0/Dc/Battery/Soc'])
        await c.start()
        await settle(c)
        fp = c.cert_sha256
        check('TLS: spojení s certifikátem GX podepsaným sám sebou, otisk uložen',
              c.values.get('system/0/Dc/Battery/Soc') == 64.5 and fp and len(fp) == 64)
        await c.close()
        c2 = gx.GxMqtt('127.0.0.1', b.port, tls=True, password='tajne', cert_sha256='0' * 64)
        try:
            await c2.start()
            check('TLS: jiný certifikát než uložený → odmítnuto', False)
        except gx.GxError as e:
            check('TLS: jiný certifikát než uložený → odmítnuto', 'certifikát' in str(e), e)
        await c2.close()
    finally:
        await b.stop()


async def chain():
    """Plán serveru (deye-monitor) → přenos → dekódování adaptérem, když je checkout vedle."""
    dm = os.path.normpath(os.path.join(_HERE, '..', '..', 'deye-monitor'))
    if not os.path.exists(os.path.join(dm, 'deye', 'edge_victron.py')):
        print('SKIP řetězec s adaptérem (deye-monitor vedle repa není)')
        return
    os.environ.setdefault('DEYE_DB_PATH', os.path.join(tempfile.mkdtemp(prefix='gx-chain-'), 'x.db'))
    sys.path.insert(0, dm)
    from deye import edge_victron as ev
    sim = ev.GxTree(1_760_090_400)
    b = await gx_sim.GxBroker(password='tajne', portal=sim.portal, tree=sim.values).start()
    try:
        c = gx.GxMqtt('127.0.0.1', b.port, tls=False, password='tajne')
        plan = ev.read_plan(caps={'transports': ['mqtt']})
        await c.set_filters(gx.GxMqtt.plan_filters(plan))
        await c.start()
        await settle(c, 0.5)
        values = {}
        for it in plan:
            r = c.read_item(it)
            values.update({it.get('base', '') + k: v for k, v in r['values'].items()})   # jako hub
        loc = {d['key']: d['value'] for d in ev.decode({'values': values, 'at': 1})['dataList']}
        ref = {d['key']: d['value'] for d in ev.decode({'values': sim.values, 'at': 1})['dataList']}
        check(f'řetězec plán → přenos → adaptér: stejný dataList jako strom GX ({len(ref)} klíčů)',
              loc == ref and len(ref) >= 20, {k: (ref.get(k), loc.get(k)) for k in set(ref) | set(loc)
                                              if ref.get(k) != loc.get(k)})
        check('identita z přenosu = portal stromu', ev.identity(values) == sim.portal == c.portal)
        d = c.read_item(next(it for it in plan if it.get('base')))
        check('položka s base: cesty bez prefixu, 384 hodnot rozvrhu DESS',
              len(d['values']) == 384 and all(k.startswith('Schedule/') for k in d['values']))
        await c.close()
    finally:
        await b.stop()


if __name__ == '__main__':
    asyncio.run(scenario())
    pin()
    asyncio.run(tls())
    asyncio.run(chain())
    print(f'\n{"FAIL" if FAILS else "OK"} — {len(FAILS)} chyb')
    sys.exit(1 if FAILS else 0)
