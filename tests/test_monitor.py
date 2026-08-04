import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import asyncio, logging, time
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

from core.models import Host, CheckType
from core.store import SupervisionStore
from core.monitor import SupervisionEngine

async def main():
    store = SupervisionStore()
    engine = SupervisionEngine(store)

    hosts = [
        Host(id='h-http-ok', name='Example.com', address='example.com', check_type=CheckType.HTTP, url='https://example.com', interval=2, timeout=3),
        Host(id='h-tcp-ok', name='GitHub 443', address='github.com', check_type=CheckType.TCP, port=443, interval=2, timeout=3),
        Host(id='h-tcp-down', name='Port ferme', address='127.0.0.1', check_type=CheckType.TCP, port=9, interval=2, timeout=1),
        Host(id='h-http-down', name='Domaine inexistant', address='x', check_type=CheckType.HTTP, url='http://nonexistent.invalid.test', interval=2, timeout=2),
    ]
    for h in hosts:
        await store.add_host(h)

    t0 = time.perf_counter()
    await engine.start()
    for i in range(6):
        loop_start = time.perf_counter()
        await asyncio.sleep(1)
        drift = (time.perf_counter() - loop_start) - 1
        print(f'heartbeat {i} - drift={drift*1000:.1f}ms (doit rester proche de 0 => non bloquant)')

    for h in hosts:
        hist = await store.get_history(h.id)
        print(h.id, '->', [(m.status.value, m.latency_ms, m.error) for m in hist])

    await engine.stop()
    print('Duree totale test:', round(time.perf_counter()-t0, 2), 's')

asyncio.run(main())