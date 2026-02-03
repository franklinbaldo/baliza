"""
Tests for parallel extraction functionality.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from baliza.extractor import PNCPExtractor


def test_parallel_extraction_requests(tmp_path):
    """
    Test that parallel extraction makes requests for all dates in range.
    Uses real DuckDB (on temp file) but mocks the API calls.
    """
    db_path = tmp_path / "test_parallel.duckdb"
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 3)  # 3 days: 01, 02, 03

    # Patch the _fetch_page function to avoid real network calls
    with patch("baliza.extractor._fetch_page") as mock_fetch:
        # Configure mock to return empty result (no data)
        # This simplifies the test as we don't need to verify insertion logic,
        # just that the extractor attempts to fetch each date.
        mock_fetch.return_value = {
            "data": [],
            "totalPaginas": 1,
            "totalRegistros": 0,
            "numeroPagina": 1
        }

        with PNCPExtractor(db_path) as extractor:
            # Run with 2 workers to test concurrency
            result = extractor.extract(start_date, end_date, workers=2)

        # Verify results
        assert result["rows_extracted"] == 0
        assert len(result["failed_dates"]) == 0

        # Check that we attempted to fetch all 3 dates
        # Note: Depending on retries or pagination logic, call_count might vary,
        # but with empty data and 1 page, it should be exactly 3.
        assert mock_fetch.call_count == 3

        # Verify that each date was requested
        dates_requested = set()
        for call in mock_fetch.call_args_list:
            # call args are: (client, url, params)
            # We access params which is the 3rd argument
            params = call[0][2]
            dates_requested.add(params["dataInicial"])

        assert "20230101" in dates_requested
        assert "20230102" in dates_requested
        assert "20230103" in dates_requested


def test_worker_failure_handling(tmp_path):
    """
    Test that one worker failing does not stop others.
    """
    db_path = tmp_path / "test_failure.duckdb"
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 3)

    with patch("baliza.extractor._fetch_page") as mock_fetch:
        # Make the second date fail
        def side_effect(client, url, params):
            if params["dataInicial"] == "20230102":
                raise Exception("Simulated API Error")
            return {
                "data": [],
                "totalPaginas": 1,
                "totalRegistros": 0
            }

        mock_fetch.side_effect = side_effect

        with PNCPExtractor(db_path) as extractor:
            result = extractor.extract(start_date, end_date, workers=2)

        # Expect 1 failure
        assert len(result["failed_dates"]) == 1
        assert "2023-01-02" in [str(d) for d in result["failed_dates"]]

        # Other dates should have been processed (we check if they were attempted)
        dates_requested = set()
        for call in mock_fetch.call_args_list:
            params = call[0][2]
            dates_requested.add(params["dataInicial"])

        assert "20230101" in dates_requested
        assert "20230103" in dates_requested
