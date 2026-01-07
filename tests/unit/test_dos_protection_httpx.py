from __future__ import annotations
import pytest
import httpx
from baliza.cli import _SecureClient, _default_http_client_factory

def test_secure_client_unbounded_read_dos():
    """
    Test that _SecureClient limits the response size to prevent DoS via memory exhaustion.
    We simulate a very large response using httpx mock and expect a failure.
    """

    def large_response_handler(request):
        # Return a generator that yields 15MB
        def stream():
            yield b"A" * (15 * 1024 * 1024)
        return httpx.Response(200, content=stream())

    # We need to inject the transport into the client
    transport = httpx.MockTransport(large_response_handler)
    client = _SecureClient(transport=transport)

    with pytest.raises(RuntimeError, match="Response too large"):
         # This should trigger reading the content because we are not using stream=True
         # AND _SecureClient should be enforcing limit during the read
         client.get("http://example.com/large")

def test_secure_client_happy_path():
    """Test that normal requests work correctly."""
    def handler(request):
        # httpx default json serializer does not add spaces
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = _SecureClient(transport=transport)

    response = client.get("http://example.com/api")
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    # Check that content is populated correctly
    assert response.content == b'{"ok": true}' or response.content == b'{"ok":true}'

def test_secure_client_scheme_validation():
    client = _SecureClient()
    with pytest.raises(ValueError, match="URL scheme must be http or https"):
        client.get("file:///etc/passwd")

def test_secure_client_redaction():
    def error_handler(request):
        raise httpx.ReadTimeout("Timeout")

    transport = httpx.MockTransport(error_handler)
    client = _SecureClient(transport=transport)

    with pytest.raises(RuntimeError) as excinfo:
        client.get("http://example.com/api?secret=123")

    # Check that exception message does NOT contain the secret
    assert "secret=123" not in str(excinfo.value)
    assert "http://example.com/api" in str(excinfo.value)

def test_factory_uses_secure_client():
    # If httpx is installed (which it is), factory should return _SecureClient
    with _default_http_client_factory() as client:
        assert isinstance(client, _SecureClient)
