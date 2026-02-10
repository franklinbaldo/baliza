"""Integration tests for SSRF protection."""

import socket
from unittest.mock import patch

import pytest

from baliza.extractor import PNCPExtractor
from baliza.utils import validate_url


def test_validate_url_public_ip():
    """Test that public IPs are allowed."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a public IP
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 80))]
        url = "https://google.com/api"
        assert validate_url(url) == url

def test_validate_url_private_ip(monkeypatch):
    """Test that private IPs are rejected."""
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a private IP
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 80))]
        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            validate_url("https://internal-service")

def test_validate_url_loopback(monkeypatch):
    """Test that loopback IPs are rejected."""
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 80))]
        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            validate_url("https://localhost")

def test_validate_url_reserved_ip(monkeypatch):
    """Test that reserved IPs (e.g. 240.0.0.0/4) are rejected."""
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a reserved IP (Class E)
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("240.0.0.1", 80))]
        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            validate_url("https://reserved-ip")

def test_validate_url_multicast_ip(monkeypatch):
    """Test that multicast IPs are rejected."""
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a multicast IP
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("224.0.0.1", 80))]
        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            validate_url("https://multicast-ip")

def test_validate_url_bypass(monkeypatch):
    """Test that private IPs are allowed when bypass env var is set."""
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    url = "https://localhost"
    # Should not raise
    assert validate_url(url) == url

def test_extractor_enforces_ssrf(tmp_path, monkeypatch):
    """Test that PNCPExtractor enforces SSRF protection on init."""
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)
    db_path = tmp_path / "test.duckdb"

    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 80))]

        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            PNCPExtractor(db_path, base_url="https://internal-api")

def test_extractor_allows_valid_url(tmp_path):
    """Test that PNCPExtractor allows valid public URLs."""
    db_path = tmp_path / "test.duckdb"

    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 80))]

        # Should not raise
        extractor = PNCPExtractor(db_path, base_url="https://public-api.com")
        assert extractor.base_url == "https://public-api.com"
