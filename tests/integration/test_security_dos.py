import json
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from baliza.engine import BalizaEngine
from baliza.extractor import PNCPExtractor

def test_fetch_page_size_limit(tmp_path):
    """Test that fetch_page raises ValueError if response is too large (DoS protection)."""
    db_path = tmp_path / "test_dos.duckdb"
    engine = BalizaEngine(db_path)
    extractor = PNCPExtractor(engine)

    # 1. Create a mock response that behaves like a real httpx.Response for streaming
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = Mock()
    
    # iter_bytes should return an iterator of chunks
    # 16 chunks of 1MB each = 16MB (Exceeds 15MB limit)
    chunks = [b"a" * (1024 * 1024) for _ in range(16)]
    mock_resp.iter_bytes.return_value = iter(chunks)

    # 2. Mock the context manager returned by self.client.stream
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_resp
    mock_cm.__exit__.return_value = None

    # Patch the stream method AND ensure cache is skipped
    with patch.object(extractor.client, "stream", return_value=mock_cm) as mock_stream:
        with patch("baliza.extractor.Path.exists", return_value=False):
            start_date = datetime(2024, 3, 1)
            end_date = datetime(2024, 3, 31)

            # Expect ValueError due to size limit
            with pytest.raises(ValueError, match="Response too large"):
                extractor.fetch_page("contratos", start_date, end_date, page=1)
            
            mock_stream.assert_called_once()
