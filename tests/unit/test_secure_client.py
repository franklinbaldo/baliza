
import pytest
import httpx
from baliza.cli import _SecureClient
from unittest.mock import MagicMock

def test_secure_client_rejects_large_response_mocked():
    """Verify that _SecureClient raises RuntimeError for responses exceeding limits."""

    mock_client = MagicMock()

    # Mock a large response
    mock_response = MagicMock()
    mock_response.iter_bytes.return_value = [b"x" * (10 * 1024 * 1024 + 1)] # 10MB + 1 byte
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.request = MagicMock()

    mock_client.stream.return_value.__enter__.return_value = mock_response

    secure_client = _SecureClient(mock_client)

    with secure_client:
        with pytest.raises(RuntimeError, match="Response too large"):
            secure_client.get("https://example.com/large")

def test_secure_client_rejects_file_scheme():
    """Verify that _SecureClient rejects file:// schemes."""
    client = MagicMock()
    secure_client = _SecureClient(client)

    with secure_client:
        with pytest.raises(ValueError, match="URL scheme must be http or https"):
            secure_client.get("file:///etc/passwd")

def test_secure_client_redacts_url_in_error_mocked():
    """Verify that query parameters are redacted from error messages."""

    mock_client = MagicMock()
    # Make .stream raise an exception
    mock_client.stream.side_effect = httpx.ConnectError("Connection failed")

    secure_client = _SecureClient(mock_client)

    with secure_client:
        with pytest.raises(RuntimeError) as excinfo:
            secure_client.get("https://example.com/api", params={"secret": "123"})

    assert "secret=123" not in str(excinfo.value)
    assert "https://example.com/api" in str(excinfo.value)

def test_secure_client_passes_small_response_mocked():
    """Verify that _SecureClient returns valid response for small content."""

    mock_client = MagicMock()

    mock_response = MagicMock()
    mock_response.iter_bytes.return_value = [b'{"key": "value"}']
    mock_response.status_code = 200
    mock_response.headers = {}
    mock_response.request = MagicMock()

    mock_client.stream.return_value.__enter__.return_value = mock_response

    secure_client = _SecureClient(mock_client)

    with secure_client:
        response = secure_client.get("https://example.com/api")
        assert response.status_code == 200
        assert response.text == '{"key": "value"}'
        assert response.json() == {"key": "value"}
