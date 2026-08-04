"""API FastAPI : REST (WebSocket ajouté à l'étape suivante)."""
from __future__ import annotations
import asyncio
from fastapi import WebSocket, WebSocketDisconnect
import logging
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from core.models import CheckType, Host, Measurement, Status
from core.monitor import SupervisionEngine
from core.store import SupervisionStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("netsupervisor.api")

store = SupervisionStore()
engine = SupervisionEngine(store)

# Hôtes de démarrage par défaut (tu pourras en ajouter/retirer via l'API ensuite)
DEFAULT_HOSTS = [
    Host(id="github", name="GitHub", address="github.com", check_type=CheckType.TCP, port=443, interval=5, timeout=3),
    Host(id="example", name="Example.com", address="example.com", check_type=CheckType.HTTP, url="https://example.com", interval=5, timeout=3),
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    for h in DEFAULT_HOSTS:
        await store.add_host(h)
    await engine.start()
    logger.info("API démarrée")
    yield
    await engine.stop()
    logger.info("API arrêtée")


app = FastAPI(title="NetSupervisor API", version="0.1.0", lifespan=lifespan)


# ---------------------- Schémas Pydantic ----------------------

class HostIn(BaseModel):
    id: str
    name: str
    address: str
    check_type: CheckType = CheckType.TCP
    port: Optional[int] = None
    url: Optional[str] = None
    interval: float = Field(5.0, gt=0)
    timeout: float = Field(3.0, gt=0)


class HostOut(BaseModel):
    id: str
    name: str
    address: str
    check_type: CheckType
    port: Optional[int]
    url: Optional[str]
    interval: float
    timeout: float

    @classmethod
    def from_host(cls, h: Host) -> "HostOut":
        return cls(id=h.id, name=h.name, address=h.address, check_type=h.check_type,
                    port=h.port, url=h.url, interval=h.interval, timeout=h.timeout)


class MeasurementOut(BaseModel):
    host_id: str
    timestamp: str
    status: Status
    latency_ms: Optional[float]
    error: Optional[str]

    @classmethod
    def from_measurement(cls, m: Measurement) -> "MeasurementOut":
        return cls(**m.to_dict())


class HostStatusOut(BaseModel):
    host: HostOut
    status: Status
    last_measurement: Optional[MeasurementOut]


# ---------------------- Endpoints REST ----------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/hosts", response_model=List[HostOut])
async def list_hosts():
    return [HostOut.from_host(h) for h in await store.list_hosts()]


@app.get("/hosts/{host_id}", response_model=HostOut)
async def get_host(host_id: str):
    host = await store.get_host(host_id)
    if host is None:
        raise HTTPException(status_code=404, detail=f"Hôte '{host_id}' introuvable")
    return HostOut.from_host(host)


@app.get("/hosts/{host_id}/status", response_model=HostStatusOut)
async def get_host_status(host_id: str):
    host = await store.get_host(host_id)
    if host is None:
        raise HTTPException(status_code=404, detail=f"Hôte '{host_id}' introuvable")
    snap = (await store.snapshot())[host_id]
    last_m = snap["last_measurement"]
    return HostStatusOut(
        host=HostOut.from_host(snap["host"]),
        status=snap["status"],
        last_measurement=MeasurementOut.from_measurement(last_m) if last_m else None,
    )


@app.get("/hosts/{host_id}/history", response_model=List[MeasurementOut])
async def get_host_history(host_id: str, limit: int = 50):
    host = await store.get_host(host_id)
    if host is None:
        raise HTTPException(status_code=404, detail=f"Hôte '{host_id}' introuvable")
    hist = await store.get_history(host_id, limit=limit)
    return [MeasurementOut.from_measurement(m) for m in hist]


@app.get("/status")
async def get_full_status():
    """Instantané complet : tous les hôtes + statut + dernière mesure."""
    snap = await store.snapshot()
    out = {}
    for hid, entry in snap.items():
        last_m = entry["last_measurement"]
        out[hid] = HostStatusOut(
            host=HostOut.from_host(entry["host"]),
            status=entry["status"],
            last_measurement=MeasurementOut.from_measurement(last_m) if last_m else None,
        )
    return out


@app.post("/hosts", response_model=HostOut, status_code=201)
async def add_host(payload: HostIn):
    if await store.get_host(payload.id) is not None:
        raise HTTPException(status_code=409, detail=f"Hôte '{payload.id}' existe déjà")
    try:
        host = Host(id=payload.id, name=payload.name, address=payload.address,
                    check_type=payload.check_type, port=payload.port, url=payload.url,
                    interval=payload.interval, timeout=payload.timeout)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    await engine.add_host(host)
    logger.info("Hôte ajouté: %s (%s)", host.id, host.address)
    return HostOut.from_host(host)


@app.delete("/hosts/{host_id}", status_code=204)
async def delete_host(host_id: str):
    removed = await engine.remove_host(host_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Hôte '{host_id}' introuvable")
    logger.info("Hôte supprimé: %s", host_id)

@app.websocket("/ws")
async def websocket_measurements(websocket: WebSocket):
    """Diffuse chaque nouvelle mesure en temps réel au format JSON.

    Message envoyé pour chaque mesure : le contenu de Measurement.to_dict().
    """
    await websocket.accept()
    queue = engine.subscribe()
    logger.info("Client WebSocket connecté (%d abonné(s))", len(engine._subscribers))
    try:
        while True:
            measurement = await queue.get()
            await websocket.send_json(measurement.to_dict())
    except (WebSocketDisconnect, RuntimeError):
        logger.info("Client WebSocket déconnecté")
    finally:
        engine.unsubscribe(queue)