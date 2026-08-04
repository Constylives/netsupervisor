"""Tests de l'API REST via TestClient (synchrone, pas besoin de serveur lancé)."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from fastapi.testclient import TestClient
from api.main import app


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_list_hosts_default(client):
    r = client.get("/hosts")
    assert r.status_code == 200
    ids = [h["id"] for h in r.json()]
    assert "github" in ids
    assert "example" in ids


def test_get_host_not_found(client):
    r = client.get("/hosts/inexistant")
    assert r.status_code == 404


def test_add_then_get_then_delete_host(client):
    payload = {
        "id": "test-host-tmp",
        "name": "Test temporaire",
        "address": "cloudflare.com",
        "check_type": "http",
        "url": "https://cloudflare.com",
        "interval": 5,
        "timeout": 3,
    }
    r = client.post("/hosts", json=payload)
    assert r.status_code == 201
    assert r.json()["id"] == "test-host-tmp"

    r = client.get("/hosts/test-host-tmp")
    assert r.status_code == 200

    r = client.post("/hosts", json=payload)
    assert r.status_code == 409  # doublon

    r = client.delete("/hosts/test-host-tmp")
    assert r.status_code == 204

    r = client.get("/hosts/test-host-tmp")
    assert r.status_code == 404


def test_add_host_invalid_tcp_missing_port(client):
    payload = {
        "id": "bad-host",
        "name": "Mauvais host",
        "address": "1.2.3.4",
        "check_type": "tcp",
        "interval": 5,
        "timeout": 3,
    }
    r = client.post("/hosts", json=payload)
    assert r.status_code == 400


def test_status_and_history_shape(client):
    r = client.get("/status")
    assert r.status_code == 200
    data = r.json()
    assert "github" in data
    assert "status" in data["github"]

    r = client.get("/hosts/github/history")
    assert r.status_code == 200
    assert isinstance(r.json(), list)

def test_websocket_receives_measurement(client):
    with client.websocket_connect("/ws") as ws:
        data = ws.receive_json()
        assert "host_id" in data
        assert "status" in data
        assert data["status"] in ("up", "down", "unknown")
        assert "timestamp" in data