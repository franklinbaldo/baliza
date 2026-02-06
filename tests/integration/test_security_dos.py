from unittest.mock import Mock

import httpx
import pytest

from baliza.extractor import _fetch_page


def test_fetch_page_size_limit():
    """Test that _fetch_page raises ValueError if response is too large."""
    client = Mock(spec=httpx.Client)

    # Mock stream response
    def mock_stream(*args, **kwargs):
        response = Mock()
        response.raise_for_status = Mock()

        # Generator that yields chunks
        def iter_bytes():
            # Yield 11 chunks of 1MB each = 11MB
            # Total size > 10MB default limit
            for _ in range(11):
                yield b"a" * (1024 * 1024)

        response.iter_bytes = iter_bytes

        cm = Mock()
        cm.__enter__ = Mock(return_value=response)
        cm.__exit__ = Mock(return_value=None)
        return cm

    client.stream = Mock(side_effect=mock_stream)

    # Mock client.get for the legacy path (to ensure test doesn't error out on attribute access)
    client.get.return_value.status_code = 200
    client.get.return_value.json.return_value = {"data": []}

    # Expect ValueError due to size limit
    with pytest.raises(ValueError, match="Response too large"):
        _fetch_page(client, "http://example.com", {})
