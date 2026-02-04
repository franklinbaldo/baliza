
import os
import pytest
from unittest.mock import patch, MagicMock
from baliza.cli import _is_safe_url, _SecureClient

def test_is_safe_url_public():
    # Google's public DNS should be safe
    with patch('socket.getaddrinfo') as mock_getaddrinfo:
        # Mocking 8.8.8.8
        mock_getaddrinfo.return_value = [(2, 1, 6, '', ('8.8.8.8', 0))]
        assert _is_safe_url("http://google.com") is True

def test_is_safe_url_private():
    # Private IP
    with patch('socket.getaddrinfo') as mock_getaddrinfo:
        # Mocking 192.168.1.1
        mock_getaddrinfo.return_value = [(2, 1, 6, '', ('192.168.1.1', 0))]
        assert _is_safe_url("http://192.168.1.1") is False

def test_is_safe_url_localhost():
    # Localhost
    with patch('socket.getaddrinfo') as mock_getaddrinfo:
        # Mocking 127.0.0.1
        mock_getaddrinfo.return_value = [(2, 1, 6, '', ('127.0.0.1', 0))]
        assert _is_safe_url("http://localhost") is False

def test_is_safe_url_bypass():
    with patch.dict(os.environ, {"BALIZA_ALLOW_PRIVATE_NETWORKS": "1"}):
        assert _is_safe_url("http://localhost") is True

def test_secure_client_blocks_unsafe():
    client = _SecureClient()
    # Mock _is_safe_url to return False
    with patch("baliza.cli._is_safe_url", return_value=False):
        with pytest.raises(ValueError, match="Blocked request to potentially unsafe URL"):
            client.get("http://unsafe.local")
