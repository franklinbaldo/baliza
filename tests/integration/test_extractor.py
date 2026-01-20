"""Integration tests for the PNCPExtractor."""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb
import pytest
from httpx import Response
from pytest_httpx import HTTPXMock

from baliza.extractor import PNCPExtractor

# Mark all tests as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


def get_coverage_status(db_path: str, resource: str, target_date: datetime) -> str | None:
    """Query the coverage table for the status of a specific window."""
    with duckdb.connect(db_path, read_only=True) as con:
        result = con.execute(
            """
            SELECT status FROM baliza_state.coverage
            WHERE resource = ? AND window_start = ?
            """,
            [resource, target_date],
        ).fetchone()
    return result[0] if result else None


import re
import baliza


def test_extractor_success(tmp_path, httpx_mock: HTTPXMock):
    """Verify a successful extraction run records 'complete' status."""
    db_path = tmp_path / "test.duckdb"
    extractor = PNCPExtractor(db_path=db_path)
    target_date = datetime(2024, 1, 1)

    # Mock a successful API response
    url_pattern = re.compile(f"^{re.escape(extractor.base_url)}/contratos.*")
    httpx_mock.add_response(
        url=url_pattern,
        json={"data": [{"numeroControlePNCP": "123"}], "totalPaginas": 1},
    )

    # Run extraction for a single day
    extractor.extract(target_date, target_date)

    # Verify status in coverage table
    status = get_coverage_status(str(db_path), "contratos", target_date)
    assert status == "complete"


def test_extractor_failure(tmp_path, httpx_mock: HTTPXMock):
    """Verify a failed extraction run records 'failed' status."""
    db_path = tmp_path / "test.duckdb"
    extractor = PNCPExtractor(db_path=db_path)
    target_date = datetime(2024, 1, 1)

    # Mock a server error
    url_pattern = re.compile(f"^{re.escape(extractor.base_url)}/contratos.*")
    httpx_mock.add_response(url=url_pattern, status_code=500)

    # Run extraction, expecting an exception
    with pytest.raises(Exception):
        extractor.extract(target_date, target_date)

    # Verify status in coverage table
    status = get_coverage_status(str(db_path), "contratos", target_date)
    assert status == "failed"


def test_get_last_extracted_date(tmp_path):
    """Verify the _get_last_extracted_date method correctly queries the state table."""
    db_path = tmp_path / "test.duckdb"
    resource = "contratos"
    day1 = datetime(2024, 1, 1)
    day2 = datetime(2024, 1, 2)

    with duckdb.connect(str(db_path)) as con:
        extractor = PNCPExtractor(db_path=db_path)
        extractor._ensure_schema(con)
        assert extractor._get_last_extracted_date(con, resource) is None

        # Add a complete record
        con.execute("INSERT INTO baliza_state.coverage VALUES (?, ?, ?, 'complete', 1, 1, NOW())", [resource, day1, day1])
        assert extractor._get_last_extracted_date(con, resource) == day1

        # Add a failed record
        con.execute("INSERT INTO baliza_state.coverage VALUES (?, ?, ?, 'failed', 1, 0, NOW())", [resource, day2, day2])
        assert extractor._get_last_extracted_date(con, resource) == day2


def test_run_windowing(tmp_path, mocker, httpx_mock: HTTPXMock):
    """Verify the run method correctly calculates the processing window."""
    db_path = tmp_path / "test.duckdb"
    resource = "contratos"
    start_date = datetime(2024, 1, 1)

    # Mock the extractor's own methods to isolate the windowing logic
    mocker.patch.object(PNCPExtractor, "_get_last_extracted_date", return_value=start_date)
    mocked_extract = mocker.patch.object(PNCPExtractor, "extract")

    # Mock datetime.now() to control the time window
    mocker.patch("baliza.extractor.datetime")
    baliza.extractor.datetime.now.return_value = start_date + timedelta(days=2)
    baliza.extractor.datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

    extractor = PNCPExtractor(db_path=db_path)
    extractor.run(resource=resource, lookback_days=1)

    # Verify that extract was called for the correct days
    assert mocked_extract.call_count == 2
    mocked_extract.assert_any_call(start_date, start_date.replace(hour=23, minute=59, second=59), resource)
    mocked_extract.assert_any_call(start_date + timedelta(days=1), (start_date + timedelta(days=1)).replace(hour=23, minute=59, second=59), resource)
