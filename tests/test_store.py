"""Tests pour core/store.py."""
import pytest

from core.models import CheckType, Host, Measurement, Status
from core.store import SupervisionStore


@pytest.fixture
def store():
    return SupervisionStore(history_size=3)


@pytest.fixture
def host():
    return Host(id="h1", name="Test", address="127.0.0.1", check_type=CheckType.TCP, port=22)


async def test_add_et_list_hosts(store, host):
    await store.add_host(host)
    hosts = await store.list_hosts()
    assert hosts == [host]


async def test_get_host_existant(store, host):
    await store.add_host(host)
    assert await store.get_host("h1") == host


async def test_get_host_inexistant_retourne_none(store):
    assert await store.get_host("inconnu") is None


async def test_historique_est_borne(store, host):
    await store.add_host(host)
    for i in range(5):
        await store.record_measurement(Measurement(host_id="h1", status=Status.UP, latency_ms=i))
    hist = await store.get_history("h1")
    # history_size=3 -> seules les 3 dernières mesures sont conservées
    assert [m.latency_ms for m in hist] == [2, 3, 4]


async def test_get_status_reflete_derniere_mesure(store, host):
    await store.add_host(host)
    await store.record_measurement(Measurement(host_id="h1", status=Status.UP))
    assert await store.get_status("h1") == Status.UP
    await store.record_measurement(Measurement(host_id="h1", status=Status.DOWN))
    assert await store.get_status("h1") == Status.DOWN


async def test_snapshot_contient_derniere_mesure(store, host):
    await store.add_host(host)
    m = Measurement(host_id="h1", status=Status.UP, latency_ms=5.0)
    await store.record_measurement(m)
    snap = await store.snapshot()
    assert snap["h1"]["status"] == Status.UP
    assert snap["h1"]["last_measurement"] is m


async def test_remove_host_existant(store, host):
    await store.add_host(host)
    assert await store.remove_host("h1") is True
    assert await store.list_hosts() == []


async def test_remove_host_inexistant_retourne_false(store):
    assert await store.remove_host("inconnu") is False