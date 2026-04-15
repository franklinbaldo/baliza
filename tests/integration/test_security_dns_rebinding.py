"""Integration tests for DNS Rebinding / TOCTOU protection."""

import socket
from unittest.mock import patch

import pytest

from baliza.utils import secure_url_connection_params


def test_secure_url_connection_params_http_rewrites_ip():
    """Test that HTTP URLs are rewritten to use the resolved IP."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a public IP
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 80))
        ]
        url = "http://example.com/api"
        safe_url, headers = secure_url_connection_params(url)

        # Should rewrite to IP
        assert safe_url == "http://93.184.216.34/api"
        # Should set Host header
        assert headers["Host"] == "example.com"


def test_secure_url_connection_params_https_preserves_url():
    """Test that HTTPS URLs are preserved (relying on SSL validation)."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a public IP
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
        ]
        url = "https://example.com/api"
        safe_url, headers = secure_url_connection_params(url)

        # Should NOT rewrite URL
        assert safe_url == "https://example.com/api"
        # Should NOT set Host header (httpx handles it)
        assert headers == {}


def test_secure_url_connection_params_ipv6():
    """Test that IPv6 addresses are correctly bracketed in rewritten URLs."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a public IPv6
        mock_getaddrinfo.return_value = [
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                6,
                "",
                ("2606:2800:220:1:248:1893:25c8:1946", 80, 0, 0),
            )
        ]
        url = "http://example.com/api"
        safe_url, headers = secure_url_connection_params(url)

        # Should rewrite to bracketed IP
        assert safe_url == "http://[2606:2800:220:1:248:1893:25c8:1946]/api"
        assert headers["Host"] == "example.com"


def test_secure_url_connection_params_private_ip(monkeypatch):
    """Test that private IPs are rejected."""
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to a private IP
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 80))
        ]
        url = "http://internal.com"

        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            secure_url_connection_params(url)


def test_secure_url_connection_params_bypass(monkeypatch):
    """Test that private IPs are allowed when bypass is enabled."""
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    # Even with bypass, we probably don't rewrite URL or maybe we do?
    # If bypass is enabled, validate_url currently returns original URL.
    # So secure_url_connection_params should also return original URL without rewriting,
    # effectively disabling the protection (since it's a bypass).

    url = "http://internal.com"
    safe_url, headers = secure_url_connection_params(url)

    assert safe_url == "http://internal.com"
    assert headers == {}
