"""
Integration tests for Baliza PNCP pipeline using VCR cassettes.

These tests make real HTTP requests to the PNCP API and record the responses
in VCR cassettes. Subsequent test runs replay the recorded responses, making
tests fast and not dependent on API availability.

To re-record cassettes (e.g., after API changes):
    rm -rf tests/cassettes/*.yaml
    pytest tests/integration/
"""

import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from baliza.extractor import PNCPExtractor


def run_extraction(db_path, start_date, end_date, dataset="test_data"):
    """Helper to run the extractor."""
    with PNCPExtractor(db_path, dataset=dataset) as extractor:
        return extractor.extract(start_date, end_date)


@pytest.mark.skip(reason="Quarantined due to persistent timeout issues with VCR/real API calls")
@pytest.mark.vcr()
def test_pncp_extract_real_api_single_day():
    """
    Test real extraction from PNCP API for a single day.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        start_date = datetime(2024, 10, 1)
        end_date = datetime(2024, 10, 1)

        run_extraction(db_path, start_date, end_date)

        with duckdb.connect(str(db_path)) as con:
            count = con.execute("SELECT COUNT(*) FROM test_data.contratos").fetchone()[0]
            assert count > 0

            sample = con.execute(
                "SELECT numeroControlePNCP, valorInicial FROM test_data.contratos LIMIT 1"
            ).fetchone()
            assert isinstance(sample[0], str)
            assert isinstance(sample[1], (int, float))


@pytest.mark.skip(reason="Quarantined due to persistent timeout issues with VCR/real API calls")
@pytest.mark.vcr()
def test_pncp_pagination():
    """
    Test that pagination works correctly over a longer period.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_pagination.duckdb"
        start_date = datetime(2024, 9, 1)
        end_date = datetime(2024, 9, 30)

        run_extraction(db_path, start_date, end_date)

        with duckdb.connect(str(db_path)) as con:
            total = con.execute("SELECT COUNT(*) FROM test_data.contratos").fetchone()[0]
            assert total > 500  # Expect multiple pages for a full month

            unique_count = con.execute(
                "SELECT COUNT(DISTINCT numeroControlePNCP) FROM test_data.contratos"
            ).fetchone()[0]
            assert unique_count == total


@pytest.mark.skip(reason="Quarantined due to persistent timeout issues with VCR/real API calls")
@pytest.mark.vcr()
def test_pncp_api_error_handling():
    """
    Test graceful handling of dates with no data.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_errors.duckdb"
        start_date = datetime(2030, 1, 1)
        end_date = datetime(2030, 1, 1)

        result = run_extraction(db_path, start_date, end_date)
        assert result["rows_extracted"] == 0

        with duckdb.connect(str(db_path)) as con:
            count = con.execute("SELECT COUNT(*) FROM test_data.contratos").fetchone()[0]
            assert count == 0


@pytest.mark.skip(reason="Quarantined due to persistent timeout issues with VCR/real API calls")
@pytest.mark.vcr()
def test_pncp_coverage_tracker():
    """
    Test that coverage tracker records metadata correctly.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_coverage.duckdb"
        start_date = datetime(2024, 10, 15)
        end_date = datetime(2024, 10, 15)

        run_extraction(db_path, start_date, end_date)

        with duckdb.connect(str(db_path)) as con:
            count = con.execute("SELECT COUNT(*) FROM baliza_state.coverage").fetchone()[0]
            assert count > 0
