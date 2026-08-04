"""Modèles de données pour le système de supervision réseau."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from typing import Optional


class CheckType(str, Enum):
    """Type de vérification à effectuer sur une cible."""
    TCP = "tcp"       # ouverture de socket TCP sur host:port
    HTTP = "http"      # requête HTTP(S) GET/HEAD via aiohttp
    ICMP = "icmp"      # ping ICMP (nécessite privilèges -> fallback TCP si indispo)


class Status(str, Enum):
    UP = "up"
    DOWN = "down"
    UNKNOWN = "unknown"


@dataclass
class Host:
    """Une cible supervisée."""
    id: str                      # identifiant unique (slug)
    name: str                    # nom lisible
    address: str                 # IP ou hostname
    check_type: CheckType = CheckType.TCP
    port: Optional[int] = None      # requis pour TCP
    url: Optional[str] = None       # requis pour HTTP
    interval: float = 5.0          # secondes entre deux vérifications
    timeout: float = 3.0           # secondes avant timeout

    def __post_init__(self):
        if self.check_type == CheckType.TCP and not self.port:
            raise ValueError(f"Host {self.id}: port requis pour un check TCP")
        if self.check_type == CheckType.HTTP and not self.url:
            raise ValueError(f"Host {self.id}: url requise pour un check HTTP")


@dataclass
class Measurement:
    """Résultat d'une vérification ponctuelle."""
    host_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: Status = Status.UNKNOWN
    latency_ms: Optional[float] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "host_id": self.host_id,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "latency_ms": self.latency_ms,
            "error": self.error,
        }