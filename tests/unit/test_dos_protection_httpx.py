from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch
import httpx
from baliza.cli import _SecureClient

# We need to test _SecureClient specifically, but since it wraps httpx,
# and the project uses a custom httpx_mock fixture that mocks the factory
# (returning a _MockClient, not a _SecureClient or httpx.Client),
# we need to be careful.

# However, _SecureClient uses httpx.Client internally or inherits from it.
# If we instantiate _SecureClient directly, it will create a real httpx.Client inside.
# We want to mock the network calls of that real httpx.Client.

# pytest-httpx provides a `httpx_mock` fixture. BUT the conftest.py overrides it!
# This is the problem.

# To bypass the conftest override, we can try to import the original fixture or use `pytest.fixture` logic,
# but easier is just to mock `httpx.Client.send` or `httpx.Response.iter_bytes`.

def test_secure_client_enforces_size_limit():
    """
    Test that _SecureClient limits the response size to prevent DoS.
    """
    # 15MB of data
    large_payload = b"A" * (15 * 1024 * 1024)

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = httpx.Headers()
    mock_response.request = MagicMock()
    mock_response.extensions = {}
    mock_response.history = []

    # iter_bytes should yield chunks
    def iter_bytes(chunk_size=8192):
        yield large_payload

    mock_response.iter_bytes.side_effect = iter_bytes

    # We need to mock super().send() call inside _SecureClient.send()
    with patch("httpx.Client.send", return_value=mock_response):
        with _SecureClient() as client:
            with pytest.raises(RuntimeError, match="Response too large"):
                 client.get("http://example.com/large")

def test_secure_client_redacts_query_params_on_error():
    """
    Test that _SecureClient redacts query parameters in exception messages.
    """
    # Simulate an exception during send
    with patch("httpx.Client.send", side_effect=httpx.ConnectError("Connection refused")):
        with _SecureClient() as client:
            with pytest.raises(RuntimeError) as excinfo:
                client.get("http://example.com/error?secret=123")

            msg = str(excinfo.value)
            assert "http://example.com/error" in msg
            assert "secret=123" not in msg
            # Check that original exception info is preserved in message or cause
            assert "Connection refused" in msg or "ConnectError" in str(excinfo.value)

def test_secure_client_valid_response():
    """
    Test that _SecureClient correctly handles valid responses.
    """
    payload = b'{"key": "value"}'

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.headers = httpx.Headers({"Content-Type": "application/json"})
    mock_response.request = MagicMock()
    mock_response.extensions = {}
    mock_response.history = []
    mock_response.iter_bytes.return_value = [payload]

    # Mock close
    mock_response.close.return_value = None

    with patch("httpx.Client.send", return_value=mock_response):
        with _SecureClient() as client:
            response = client.get("http://example.com/api")
            assert response.status_code == 200
            assert response.json() == {"key": "value"}
