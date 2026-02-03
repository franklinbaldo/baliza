"""Tests for SSRF protection."""
import os
import socket
from pathlib import Path
from unittest.mock import patch

import pytest

from src.baliza.extractor import PNCPExtractor
from src.baliza.utils import validate_url


def test_validate_url_public_ok():
    """Test that public URLs are allowed."""
    # Mock resolution to a public IP
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 80))
        ]
        url = "https://google.com/api"
        assert validate_url(url) == url


def test_validate_url_private_fails():
    """Test that private IPs are rejected."""
    # Mock resolution to a private IP
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 80))
        ]
        with pytest.raises(ValueError, match="SSRF attempted"):
            validate_url("https://internal-service")


def test_validate_url_localhost_fails():
    """Test that localhost is rejected."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 80))
        ]
        with pytest.raises(ValueError, match="SSRF attempted"):
            validate_url("http://localhost:8000")


def test_validate_url_bypass_env_var():
    """Test that validation can be bypassed with env var."""
    with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
        # Even with localhost, it should pass if env var is set
        # We don't even need to mock getaddrinfo because it returns early
        url = "http://localhost:8000"
        assert validate_url(url) == url


def test_extractor_init_validates_url():
    """Test that PNCPExtractor validates base_url on init."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 80))
        ]

        with pytest.raises(ValueError, match="SSRF attempted"):
            PNCPExtractor(
                Path(":memory:"),
                base_url="https://internal-api"
            )

def test_validate_url_ipv6_loopback_fails():
    """Test that IPv6 loopback is rejected."""
    with patch("socket.getaddrinfo") as mock_getaddrinfo:
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 80, 0, 0))
        ]
        # Note: brackets are required for urlparse to handle IPv6 literals
        with pytest.raises(ValueError, match="SSRF attempted"):
            validate_url("http://[::1]:8000")
