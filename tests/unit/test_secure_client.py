from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch
import httpx
from baliza.cli import _SecureClient

def test_secure_client_unbounded_read_dos():
    """
    Test that _SecureClient limits the response size to prevent DoS via memory exhaustion.
    We simulate a very large response from httpx and expect a failure.
    """
    # 15MB of data
    # We use a generator to simulate streaming
    def large_stream():
        yield b"A" * (15 * 1024 * 1024)

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.iter_bytes.side_effect = lambda: large_stream()
    mock_response.status_code = 200
    mock_response.close = MagicMock()

    mock_client = MagicMock(spec=httpx.Client)
    mock_client.build_request.return_value = MagicMock()
    mock_client.send.return_value = mock_response
    # Also support context manager
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = None

    secure_client = _SecureClient(mock_client)

    with pytest.raises(RuntimeError, match="Response too large"):
        secure_client.get("http://example.com/large")

    mock_response.close.assert_called()

def test_secure_client_scheme_validation():
    """Test that _SecureClient rejects non-http/https schemes."""
    mock_client = MagicMock(spec=httpx.Client)
    secure_client = _SecureClient(mock_client)

    with pytest.raises(ValueError, match="URL scheme must be http or https"):
        secure_client.get("ftp://example.com/file")

    with pytest.raises(ValueError, match="URL scheme must be http or https"):
        secure_client.get("file:///etc/passwd")

def test_secure_client_valid_request():
    """Test that valid requests work as expected."""
    content = b"Hello World"

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.iter_bytes.return_value = [content]
    mock_response.status_code = 200
    mock_response.close = MagicMock()
    # Mock internal _content attribute since we assign to it
    mock_response._content = b""

    mock_client = MagicMock(spec=httpx.Client)
    mock_client.build_request.return_value = MagicMock()
    mock_client.send.return_value = mock_response

    secure_client = _SecureClient(mock_client)

    response = secure_client.get("http://example.com/ok")

    assert response.status_code == 200
    assert response._content == content
    mock_response.close.assert_called_once()
