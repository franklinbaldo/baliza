
import pytest
import httpx
from unittest.mock import MagicMock, patch
from baliza.cli import _default_http_client_factory

def test_secure_client_size_limit():
    """
    Test that _SecureClient limits the response size to prevent DoS via memory exhaustion.
    """
    # Force httpx to be used
    if httpx is None:
        pytest.skip("httpx not installed")

    # Create a client
    client = _default_http_client_factory()

    # Mock httpx.Client.stream behavior
    # We need to mock the response object returned by client.stream()

    # 15MB of data
    large_payload = b"A" * (15 * 1024 * 1024)

    # We will mock request logic because _SecureClient overrides request()
    # BUT wait, _SecureClient.request calls super().stream()
    # It's hard to mock internal inheritance calls easily without full patching.

    # Instead, let's just use respx or httpx mock if available?
    # pytest-httpx is installed.

    # However, pytest-httpx mocks the transport.
    # Let's try to mock the transport response to return a generator that yields chunks.

    # Actually, simpler: define a custom transport that yields infinite data or large data

    def large_response_handler(request):
        # Return a stream that is too large
        def stream_content():
            yield b"A" * (8 * 1024 * 1024) # 8MB
            yield b"B" * (3 * 1024 * 1024) # +3MB = 11MB > 10MB limit

        return httpx.Response(200, content=stream_content())

    # We can inject a transport into the client returned by factory
    # But factory creates a new client.

    # Let's instantiate _SecureClient directly if we can import it?
    # No, it's defined inside an if block in cli.py.

    # But we can get the class from the object returned by factory.
    SecureClientClass = type(client)

    transport = httpx.MockTransport(large_response_handler)
    client = SecureClientClass(transport=transport)

    with pytest.raises(RuntimeError, match="Response too large"):
        client.get("http://example.com/large")

def test_secure_client_redaction():
    """
    Test that _SecureClient redacts query parameters in exception messages.
    """
    if httpx is None:
        pytest.skip("httpx not installed")

    def error_handler(request):
        raise Exception("Connection failed")

    SecureClientClass = type(_default_http_client_factory())
    transport = httpx.MockTransport(error_handler)
    client = SecureClientClass(transport=transport)

    secret_url = "http://example.com/api?api_key=secret123"

    with pytest.raises(RuntimeError) as excinfo:
        client.get(secret_url)

    error_msg = str(excinfo.value)
    assert "api_key=secret123" not in error_msg
    assert "http://example.com/api" in error_msg
    assert "failed" in error_msg
