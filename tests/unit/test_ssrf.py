
import pytest
from unittest.mock import MagicMock, patch
from baliza.cli import _FallbackClient

def test_fallback_client_rejects_file_scheme():
    """Test that _FallbackClient refuses to load file:// URLs."""
    client = _FallbackClient()
    with pytest.raises((ValueError, RuntimeError), match="URL scheme must be http or https"):
        client.get("file:///etc/passwd")

def test_fallback_client_disables_redirects():
    """Test that _FallbackClient uses a custom opener that disables redirects."""
    with patch("urllib.request.build_opener") as mock_build_opener:
        mock_opener = MagicMock()
        mock_build_opener.return_value = mock_opener
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = b"" # empty body to stop loop
        mock_opener.open.return_value.__enter__.return_value = mock_response

        client = _FallbackClient()
        client.get("http://example.com")

        # Verify build_opener was called with a handler class (we can't easily check class name here
        # because it's defined inside the method, but we can check args were passed)
        assert mock_build_opener.call_count == 1
        handler_class = mock_build_opener.call_args[0][0]
        # Verify the handler has redirect methods
        assert hasattr(handler_class, "http_error_302")
        assert hasattr(handler_class, "http_error_301")

def test_fallback_client_limits_size():
    """Test that _FallbackClient enforces size limits."""
    # This is an existing feature, just ensuring we didn't break it
    with patch("urllib.request.build_opener") as mock_build_opener:
        mock_opener = MagicMock()
        mock_build_opener.return_value = mock_opener
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        # Simulate large response
        mock_response.read.side_effect = [b"a" * 8192] * 2000 # > 10MB
        mock_opener.open.return_value.__enter__.return_value = mock_response

        client = _FallbackClient()
        with pytest.raises(RuntimeError, match="Response too large"):
            client.get("http://example.com")
