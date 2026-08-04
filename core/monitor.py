"""Moteur de supervision asynchrone.

Chaque hôte est surveillé par sa propre tâche asyncio qui boucle
indéfiniment (check -> attente -> check ...) sans jamais bloquer les
autres. Deux types de check :
  - TCP : ouverture de socket (asyncio.open_connection)
  - HTTP : requête GET/HEAD via aiohttp.ClientSession

Le résultat de chaque check est poussé dans le SupervisionStore et
publié sur un asyncio.Queue de diffusion (pour le WebSocket FastAPI).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional

import aiohttp
from icmplib import async_ping, ICMPSocketError, SocketPermissionError

from core.models import CheckType, Host, Measurement, Status
from core.store import SupervisionStore

logger = logging.getLogger("netsupervisor.monitor")


async def check_tcp(host: Host) -> Measurement:
    start = time.perf_counter()
    try:
        fut = asyncio.open_connection(host.address, host.port)
        reader, writer = await asyncio.wait_for(fut, timeout=host.timeout)
        latency = (time.perf_counter() - start) * 1000
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return Measurement(host_id=host.id, status=Status.UP, latency_ms=round(latency, 2))
    except (asyncio.TimeoutError, OSError) as e:
        return Measurement(host_id=host.id, status=Status.DOWN, error=str(e) or type(e).__name__)


async def check_http(host: Host, session: aiohttp.ClientSession) -> Measurement:
    start = time.perf_counter()
    try:
        timeout = aiohttp.ClientTimeout(total=host.timeout)
        async with session.get(host.url, timeout=timeout, allow_redirects=True) as resp:
            await resp.read()
            latency = (time.perf_counter() - start) * 1000
            if resp.status < 400:
                return Measurement(host_id=host.id, status=Status.UP, latency_ms=round(latency, 2))
            return Measurement(host_id=host.id, status=Status.DOWN,
                                latency_ms=round(latency, 2), error=f"HTTP {resp.status}")
    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
        return Measurement(host_id=host.id, status=Status.DOWN, error=str(e) or type(e).__name__)


async def check_icmp(host: Host) -> Measurement:
    """Ping ICMP réel via icmplib. Nécessite des privilèges administrateur
    (socket raw ICMP) sous Windows et Linux."""
    try:
        result = await async_ping(host.address, count=1, timeout=host.timeout, privileged=True)
        if result.is_alive:
            return Measurement(host_id=host.id, status=Status.UP, latency_ms=round(result.avg_rtt, 2))
        return Measurement(host_id=host.id, status=Status.DOWN, error="Aucune réponse au ping (timeout)")
    except SocketPermissionError:
        return Measurement(
            host_id=host.id, status=Status.DOWN,
            error="Permission refusée: relancez le programme en administrateur pour le ping ICMP",
        )
    except ICMPSocketError as e:
        return Measurement(host_id=host.id, status=Status.DOWN, error=f"Erreur socket ICMP: {e}")


class SupervisionEngine:
    """Orchestre les tâches de supervision concurrentes, une par hôte."""

    def __init__(self, store: SupervisionStore):
        self.store = store
        self._tasks: Dict[str, asyncio.Task] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._subscribers: list[asyncio.Queue] = []

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        for host in await self.store.list_hosts():
            self._spawn(host)
        logger.info("Moteur de supervision démarré (%d hôtes)", len(self._tasks))

    async def stop(self) -> None:
        for t in self._tasks.values():
            t.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        if self._session:
            await self._session.close()
        logger.info("Moteur de supervision arrêté")

    def subscribe(self) -> asyncio.Queue:
        """Utilisé par le WebSocket FastAPI (ou tout autre consommateur) pour
        recevoir chaque nouvelle mesure en temps réel."""
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def add_host(self, host: Host) -> None:
        await self.store.add_host(host)
        self._spawn(host)

    async def remove_host(self, host_id: str) -> bool:
        task = self._tasks.pop(host_id, None)
        if task:
            task.cancel()
        return await self.store.remove_host(host_id)

    def _spawn(self, host: Host) -> None:
        if host.id in self._tasks:
            return
        self._tasks[host.id] = asyncio.create_task(
            self._supervise_loop(host), name=f"monitor:{host.id}"
        )

    async def _supervise_loop(self, host: Host) -> None:
        while True:
            try:
                measurement = await self._run_check(host)
            except Exception as e:  # ne doit jamais tuer la boucle
                logger.exception("Erreur inattendue en supervisant %s", host.id)
                measurement = Measurement(host_id=host.id, status=Status.DOWN, error=str(e))

            await self.store.record_measurement(measurement)
            await self._broadcast(measurement)

            if measurement.status == Status.DOWN:
                logger.warning("ALERTE: %s (%s) est DOWN - %s", host.name, host.id, measurement.error)

            try:
                await asyncio.sleep(host.interval)
            except asyncio.CancelledError:
                break

    async def _run_check(self, host: Host) -> Measurement:
        if host.check_type == CheckType.TCP:
            return await check_tcp(host)
        elif host.check_type == CheckType.HTTP:
            assert self._session is not None
            return await check_http(host, self._session)
        elif host.check_type == CheckType.ICMP:
            return await check_icmp(host)
        else:
            raise NotImplementedError(f"check_type {host.check_type} non supporté")

    async def _broadcast(self, measurement: Measurement) -> None:
        for q in list(self._subscribers):
            if q.full():
                continue
            await q.put(measurement)