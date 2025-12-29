from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from baliza.cli import _default_http_client_factory, _FallbackClient, _SecureClient


def test_fallback_client_unbounded_read_dos():
    """
    Test that _FallbackClient limits the response size to prevent DoS via memory exhaustion.
    We simulate a very large response and expect a failure.
    """
    # 15MB of data
    large_payload = b"A" * (15 * 1024 * 1024)

    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_response.read.side_effect = [large_payload, b""]
    # We also need to support enter/exit because it's used as context manager
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None

    client = _FallbackClient()

    with patch("urllib.request.urlopen", return_value=mock_response):
        with pytest.raises(RuntimeError, match="Response too large"):
            client.get("http://example.com/large")

def test_secure_client_dos_protection(httpx_mock):
    """
    Test that the new SecureClient (wrapping httpx) enforces the size limit.
    """
    # 15MB of data
    large_payload = b"A" * (15 * 1024 * 1024)

    # Mock network response
    httpx_mock.add_response(content=large_payload)

    # This factory should now return _SecureClient wrapping httpx.Client
    factory = _default_http_client_factory()

    with factory as client:
        # Verify wrapper is in place
        assert isinstance(client, _SecureClient)

        # Verify it raises RuntimeError instead of consuming 15MB
        with pytest.raises(RuntimeError, match="Response too large"):
            client.get("http://example.com/large")

def test_secure_client_scheme_validation(httpx_mock):
    """Test that SecureClient blocks non-http/https schemes (SSRF protection)."""
    factory = _default_http_client_factory()
    with factory as client:
        with pytest.raises(ValueError, match="URL scheme must be http or https"):
            client.get("file:///etc/passwd")

def test_secure_client_normal_operation(httpx_mock):
    """Test that normal small responses work as expected."""
    httpx_mock.add_response(json={"ok": True})

    factory = _default_http_client_factory()
    with factory as client:
        response = client.get("http://example.com/api")
        assert response.status_code == 200
        assert response.json() == {"ok": True}
