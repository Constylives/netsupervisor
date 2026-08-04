"""Tests pour core/models.py."""
import pytest

from core.models import CheckType, Host, Measurement, Status


def test_host_tcp_valide():
    h = Host(id="h1", name="Test", address="127.0.0.1", check_type=CheckType.TCP, port=22)
    assert h.port == 22


def test_host_tcp_sans_port_leve_erreur():
    with pytest.raises(ValueError, match="port requis"):
        Host(id="h1", name="Test", address="127.0.0.1", check_type=CheckType.TCP)


def test_host_http_valide():
    h = Host(id="h2", name="Test", address="example.com", check_type=CheckType.HTTP, url="https://example.com")
    assert h.url == "https://example.com"


def test_host_http_sans_url_leve_erreur():
    with pytest.raises(ValueError, match="url requise"):
        Host(id="h2", name="Test", address="example.com", check_type=CheckType.HTTP)


def test_host_icmp_sans_port_ni_url_ok():
    # ICMP n'a besoin ni de port ni d'url
    h = Host(id="h3", name="Test", address="8.8.8.8", check_type=CheckType.ICMP)
    assert h.port is None
    assert h.url is None


def test_measurement_to_dict():
    m = Measurement(host_id="h1", status=Status.UP, latency_ms=12.34)
    d = m.to_dict()
    assert d["host_id"] == "h1"
    assert d["status"] == "up"
    assert d["latency_ms"] == 12.34
    assert "timestamp" in d


def test_measurement_defaut_statut_unknown():
    m = Measurement(host_id="h1")
    assert m.status == Status.UNKNOWN
    assert m.latency_ms is None