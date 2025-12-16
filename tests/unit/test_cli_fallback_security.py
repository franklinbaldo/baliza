
import pytest
from unittest.mock import patch
from baliza.cli import _FallbackClient

def test_fallback_client_redacts_query_params_in_error():
    """
    Test that _FallbackClient redacts query parameters in exception messages.
    """
    client = _FallbackClient()
    url = "https://api.example.com/resource"
    params = {"api_key": "secret_123", "other": "public"}

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = Exception("Network error")

        with pytest.raises(RuntimeError) as exc_info:
            client.get(url, params=params)

        error_msg = str(exc_info.value)
        # Ensure sensitive data is NOT in the error message
        assert "secret_123" not in error_msg
        # Ensure redaction marker IS present
        assert "https://api.example.com/resource?[REDACTED]" in error_msg
