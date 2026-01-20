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
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from baliza.extractor import PNCPExtractor


@pytest.mark.vcr()
def test_pncp_extract_real_api_single_day():
    """
    Test real extraction from PNCP API for a single day.

    This test makes a real API request (recorded by VCR) for October 1st, 2024.
    It verifies that:
    - API response is successful
    - Data is loaded into DuckDB
    - Records have expected schema
    - numeroControlePNCP follows expected format
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"

        # Extract data for a specific date
        with PNCPExtractor(db_path=db_path, dataset="test_data") as extractor:
            start_date = datetime.fromisoformat("2024-10-01")
            end_date = datetime.fromisoformat("2024-10-01")
            run_info = extractor.extract(start_date=start_date, end_date=end_date)

        # Verify pipeline executed
        assert run_info is not None
        assert run_info["rows_extracted"] > 0

        # Query the database
        con = duckdb.connect(str(db_path))
        result = con.execute("SELECT COUNT(*) as total FROM test_data.contratos").fetchone()

        total_rows = result[0]
        assert total_rows > 0, "Should have extracted at least one contract"

        # Verify schema
        schema = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'test_data' AND table_name = 'contratos'"
        ).fetchall()

        column_names = [row[0] for row in schema]
        # The new extractor does not normalize column names to snake_case
        assert "numeroControlePNCP" in column_names
        assert "valorInicial" in column_names

        # Verify data format
        sample = con.execute(
            "SELECT numeroControlePNCP, valorInicial FROM test_data.contratos LIMIT 1"
        ).fetchone()

        assert sample is not None
        numero_controle = sample[0]
        valor = sample[1]

        # numeroControlePNCP format: CNPJ-2-sequencial/ano
        assert isinstance(numero_controle, str)
        assert "-" in numero_controle
        assert "/" in numero_controle

        # Valor should be numeric
        assert isinstance(valor, (float, int))
        assert valor >= 0

        con.close()


@pytest.mark.vcr()
def test_pncp_extract_with_lookback():
    """
    Test extraction with lookback parameter.

    Verifies that lookback_days parameter correctly retrieves data
    from previous days.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_lookback.duckdb"

        # Extract with 3 days lookback from Oct 5, 2024
        with PNCPExtractor(db_path=db_path, dataset="test_data") as extractor:
            end_date = datetime.fromisoformat("2024-10-05")
            start_date = end_date - timedelta(days=3)  # Simulate lookback
            run_info = extractor.extract(start_date=start_date, end_date=end_date)

        assert run_info is not None

        con = duckdb.connect(str(db_path))
        result = con.execute("SELECT COUNT(*) as total FROM test_data.contratos").fetchone()

        # With lookback, should get data from multiple days
        assert result[0] > 0

        con.close()


@pytest.mark.vcr()
def test_pncp_pagination():
    """
    Test that pagination works correctly.

    Extracts a full month to ensure pagination handles multiple pages.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_pagination.duckdb"

        # Extract full month (will have multiple pages)
        with PNCPExtractor(db_path=db_path, dataset="test_data") as extractor:
            start_date = datetime.fromisoformat("2024-09-01")
            end_date = datetime.fromisoformat("2024-09-30")
            run_info = extractor.extract(start_date=start_date, end_date=end_date)

        assert run_info is not None

        con = duckdb.connect(str(db_path))

        # Should have many records for a full month
        total = con.execute("SELECT COUNT(*) FROM test_data.contratos").fetchone()[0]

        assert total > 500, "Full month should have many contracts (pagination test)"

        # Verify unique numeroControlePNCP (merge should work)
        unique_count = con.execute(
            "SELECT COUNT(DISTINCT numeroControlePNCP) FROM test_data.contratos"
        ).fetchone()[0]

        assert unique_count == total, "All numeroControlePNCP should be unique"

        con.close()


@pytest.mark.vcr()
def test_pncp_api_error_handling():
    """
    Test handling of API errors and edge cases.

    Note: VCR will record the error response, so subsequent runs
    replay the same error.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_errors.duckdb"

        # Try to extract from far future (should handle gracefully)
        # PNCP API returns 204 No Content for dates with no data
        with PNCPExtractor(db_path=db_path, dataset="test_data") as extractor:
            start_date = datetime.fromisoformat("2030-01-01")
            end_date = datetime.fromisoformat("2030-01-01")
            run_info = extractor.extract(start_date=start_date, end_date=end_date)

        # Should complete without error
        assert run_info is not None

        con = duckdb.connect(str(db_path))

        # May have 0 records if no data for that date
        # Check if table exists first
        tables = con.execute(
            "SELECT * FROM information_schema.tables WHERE table_schema='test_data' AND table_name='contratos'"
        ).fetchall()
        if tables:
            result = con.execute("SELECT COUNT(*) FROM test_data.contratos").fetchone()
            assert result[0] >= 0
        else:
            # If table doesn't exist, count is effectively 0
            pass

        con.close()


@pytest.mark.vcr()
def test_pncp_coverage_tracker():
    """
    Test that coverage tracker records metadata correctly.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_coverage.duckdb"

        with PNCPExtractor(db_path=db_path, dataset="test_data") as extractor:
            start_date = datetime.fromisoformat("2024-10-15")
            end_date = datetime.fromisoformat("2024-10-15")
            run_info = extractor.extract(start_date=start_date, end_date=end_date)

        assert run_info is not None

        con = duckdb.connect(str(db_path))

        # Check if coverage tables exist
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'baliza_state'"
        ).fetchall()

        table_names = [row[0] for row in tables]

        # Coverage tracking tables should exist
        assert "coverage" in table_names

        con.close()
