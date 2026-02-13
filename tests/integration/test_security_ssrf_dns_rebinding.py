"""Integration tests for DNS Rebinding/SSRF protection."""

import socket
from unittest.mock import patch

import pytest

from baliza.utils import secure_url_connection_params


def test_dns_rebinding_mixed_ips_public_first(monkeypatch):
    """Test that if a hostname resolves to BOTH public and private IPs (public first), it is rejected.

    This simulates a DNS Rebinding attack where the attacker returns multiple A records.
    If the application only checks the first one, it might be tricked into allowing the connection.
    """
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)

    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Mock resolving to one public (8.8.8.8) and one private (192.168.1.1) IP
        # The public IP is first, so a naive check would pass it
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 80)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 80))
        ]

        # This SHOULD fail because one of the IPs is private
        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            secure_url_connection_params("https://evil-domain.com")


def test_dns_rebinding_mixed_ips_private_first(monkeypatch):
    """Test that if a hostname resolves to BOTH private and public IPs (private first), it is rejected.

    This is the easier case, as the first check should catch it.
    """
    monkeypatch.delenv("BALIZA_ALLOW_PRIVATE_NETWORKS", raising=False)

    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        # Private IP first
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 80)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 80))
        ]

        # This should fail immediately on the first IP
        with pytest.raises(ValueError, match="resolves to non-global or multicast IP"):
            secure_url_connection_params("https://evil-domain.com")
