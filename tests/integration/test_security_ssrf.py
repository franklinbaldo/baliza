import socket
import pytest
from pathlib import Path
from unittest.mock import patch
from src.baliza.extractor import PNCPExtractor
from src.baliza.utils import validate_url

def test_validate_url_rejects_private_ips_literals():
    """Test that validate_url rejects private IP address literals."""
    # These do not require DNS resolution, so they are safe to run without mocks
    private_urls = [
        "http://127.0.0.1:8000",
        "http://localhost:3000", # localhost typically resolves locally via /etc/hosts
        "http://10.0.0.1",
        "http://192.168.1.1",
        "http://[::1]",
    ]
    for url in private_urls:
        with pytest.raises(ValueError, match="Internal/Private network access denied"):
            validate_url(url)

@patch("socket.getaddrinfo")
def test_validate_url_rejects_private_dns(mock_getaddrinfo):
    """Test that validate_url rejects hostnames resolving to private IPs."""
    # Mock resolution of "private.local" to 192.168.1.1
    mock_getaddrinfo.return_value = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('192.168.1.1', 80))
    ]

    with pytest.raises(ValueError, match="Internal/Private network access denied"):
        validate_url("http://private.local")

@patch("socket.getaddrinfo")
def test_validate_url_accepts_public_ips(mock_getaddrinfo):
    """Test that validate_url accepts public IP addresses."""
    # Mock resolution of "example.com" to a public IP (e.g., 8.8.8.8)
    mock_getaddrinfo.return_value = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 443))
    ]

    url = "https://example.com"
    assert validate_url(url) == url

def test_validate_url_rejects_invalid_schemes():
    """Test that validate_url rejects non-http/https schemes."""
    with pytest.raises(ValueError, match="Invalid URL scheme"):
        validate_url("ftp://example.com")
    with pytest.raises(ValueError, match="Invalid URL scheme"):
        validate_url("file:///etc/passwd")

def test_validate_url_bypass_env_var(monkeypatch):
    """Test that BALIZA_ALLOW_PRIVATE_NETWORKS allows private IPs."""
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    url = "http://127.0.0.1:8000"
    assert validate_url(url) == url

def test_extractor_enforces_ssrf_protection():
    """Test that PNCPExtractor enforces SSRF protection on init."""
    with pytest.raises(ValueError, match="Internal/Private network access denied"):
        PNCPExtractor(Path(":memory:"), base_url="http://127.0.0.1:8000")

def test_extractor_allows_private_ips_with_bypass(monkeypatch):
    """Test that PNCPExtractor allows private IPs when bypass is enabled."""
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    extractor = PNCPExtractor(Path(":memory:"), base_url="http://127.0.0.1:8000")
    assert extractor.base_url == "http://127.0.0.1:8000"
