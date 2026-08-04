"""Stockage en mémoire (thread-safe via asyncio.Lock) des hôtes et mesures.

Volontairement simple (pas de DB) pour rester focalisé sur asyncio/PyQt6/
aiohttp/FastAPI. Facilement remplaçable par SQLite plus tard si besoin.
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Dict, List, Optional

from core.models import Host, Measurement, Status


class SupervisionStore:
    def __init__(self, history_size: int = 200):
        self._hosts: Dict[str, Host] = {}
        self._history: Dict[str, Deque[Measurement]] = {}
        self._last_status: Dict[str, Status] = {}
        self._history_size = history_size
        self._lock = asyncio.Lock()

    async def add_host(self, host: Host) -> None:
        async with self._lock:
            self._hosts[host.id] = host
            self._history.setdefault(host.id, deque(maxlen=self._history_size))
            self._last_status.setdefault(host.id, Status.UNKNOWN)

    async def remove_host(self, host_id: str) -> bool:
        async with self._lock:
            existed = host_id in self._hosts
            self._hosts.pop(host_id, None)
            self._history.pop(host_id, None)
            self._last_status.pop(host_id, None)
            return existed

    async def list_hosts(self) -> List[Host]:
        async with self._lock:
            return list(self._hosts.values())

    async def get_host(self, host_id: str) -> Optional[Host]:
        async with self._lock:
            return self._hosts.get(host_id)

    async def record_measurement(self, m: Measurement) -> None:
        async with self._lock:
            if m.host_id not in self._history:
                self._history[m.host_id] = deque(maxlen=self._history_size)
            self._history[m.host_id].append(m)
            self._last_status[m.host_id] = m.status

    async def get_history(self, host_id: str, limit: int = 50) -> List[Measurement]:
        async with self._lock:
            hist = list(self._history.get(host_id, []))
            return hist[-limit:]

    async def get_status(self, host_id: str) -> Status:
        async with self._lock:
            return self._last_status.get(host_id, Status.UNKNOWN)

    async def snapshot(self) -> Dict[str, dict]:
        """Retourne un instantané complet (hôte + dernier statut + dernière mesure)."""
        async with self._lock:
            out = {}
            for hid, host in self._hosts.items():
                last = self._history.get(hid)
                last_m = last[-1] if last else None
                out[hid] = {
                    "host": host,
                    "status": self._last_status.get(hid, Status.UNKNOWN),
                    "last_measurement": last_m,
                }
            return out