from __future__ import annotations
import pytest
from unittest.mock import MagicMock, patch
from baliza.cli import _FallbackClient

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

    # This mock will be returned by the patched build_opener
    mock_opener = MagicMock()
    mock_opener.open.return_value = mock_response

    client = _FallbackClient()

    mock_opener = MagicMock()
    mock_opener.open.return_value = mock_response

    with patch("urllib.request.build_opener", return_value=mock_opener):
        with pytest.raises(RuntimeError, match="Response too large"):
            client.get("http://example.com/large")
