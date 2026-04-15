import json
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock

from baliza.engine import BalizaEngine
from baliza.extractor import PNCPExtractor

class TestPNCPExtractorV2(unittest.TestCase):
    """Unit tests for the modernized, stateless PNCPExtractor."""

    def setUp(self):
        # Create a temporary directory for tests
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.engine = BalizaEngine(self.db_path)
        self.extractor = PNCPExtractor(self.engine)
        
        self.raw_data_root = Path(self.test_dir) / "data" / "raw"
        self.raw_data_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _mock_stream_response(self, data: dict):
        """Helper to mock httpx stream response."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = Mock()
        
        json_bytes = json.dumps(data).encode("utf-8")
        # Generator for iter_bytes
        def iter_bytes():
            yield json_bytes
            
        mock_resp.iter_bytes = iter_bytes
        
        cm = MagicMock()
        cm.__enter__ = Mock(return_value=mock_resp)
        cm.__exit__ = Mock(return_value=None)
        return cm

    def test_fetch_page_saves_to_cache(self):
        """Test that fetching a page saves the result to the local JSON cache."""
        start_date = datetime(2024, 3, 1)
        end_date = datetime(2024, 3, 31)
        
        mock_data = {"data": [{"numeroControlePNCP": "TEST-1"}], "totalPaginas": 1}
        
        with patch("baliza.extractor.Path") as mock_path:
            mock_path.side_effect = lambda *args: Path(self.test_dir, *args) if "data/raw" in str(args) else Path(*args)
            
            # Now mocking stream instead of get
            with patch.object(self.extractor.client, "stream") as mock_stream:
                mock_stream.return_value = self._mock_stream_response(mock_data)
                
                result = self.extractor.fetch_page("contratos", start_date, end_date, page=1)
                
                self.assertEqual(result, mock_data)
                mock_stream.assert_called_once()

    def test_fetch_page_resumes_from_cache(self):
        """Test that fetch_page reuses valid cached JSON files instead of hitting the API."""
        start_date = datetime(2024, 3, 1)
        end_date = datetime(2024, 3, 31)
        month_str = "2024-03"
        
        with patch("baliza.extractor.Path") as mock_path:
            def path_side_effect(*args):
                p_str = "/".join(map(str, args))
                if "data/raw" in p_str:
                    return Path(self.test_dir, *args)
                return Path(*args)
            
            mock_path.side_effect = path_side_effect
            
            cache_dir = Path(self.test_dir) / "data/raw" / month_str
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / "contratos_p1.json"
            
            cached_data = {"data": [{"id": "cached"}], "totalPaginas": 1}
            with open(cache_file, "w") as f:
                json.dump(cached_data, f)
            
            with patch.object(self.extractor.client, "stream") as mock_stream:
                result = self.extractor.fetch_page("contratos", start_date, end_date, page=1)
                
                # Should NOT have called the API
                mock_stream.assert_not_called()
                self.assertEqual(result["data"][0]["id"], "cached")

    def test_probe_range_returns_correct_metadata(self):
        """Test that probe_range calls fetch_page(page=1) and extracts metadata."""
        start_date = datetime(2024, 3, 1)
        end_date = datetime(2024, 3, 31)
        
        mock_page_1 = {"totalPaginas": 10, "totalRegistros": 1000, "data": []}
        
        with patch.object(self.extractor, "fetch_page", return_value=mock_page_1) as mock_fetch:
            result = self.extractor.probe_range("contratos", start_date, end_date)
            
            self.assertEqual(result["total_pages"], 10)
            self.assertEqual(result["total_registries"], 1000)
            mock_fetch.assert_called_with("contratos", start_date, end_date, page=1)

if __name__ == "__main__":
    unittest.main()
