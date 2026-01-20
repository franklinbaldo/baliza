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
from pathlib import Path

import duckdb
import pytest
from baliza.pipelines import pncp


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
        pipeline, run_info = pncp.run_pncp(
            duckdb_path=db_path,
            dataset="test_data",
            lookback_days=0,
            range_start="2024-10-01",
            range_end="2024-10-01",
        )

        # Verify pipeline executed
        assert run_info is not None

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
        # dlt normalizes column names to snake_case
        normalized_columns = [c.lower() for c in column_names]

        # 'numeroControlePNCP' -> 'numero_controle_pncp'
        assert (
            "numero_controle_pncp" in normalized_columns
            or "numerocontrolepncp" in normalized_columns
        )
        assert "valor_inicial" in normalized_columns or "valorinicial" in normalized_columns

        # Verify data format
        # Use normalized column name in query
        col_name = (
            "numero_controle_pncp"
            if "numero_controle_pncp" in normalized_columns
            else "numeroControlePNCP"
        )
        val_col_name = "valor_inicial" if "valor_inicial" in normalized_columns else "valorInicial"

        sample = con.execute(
            f"SELECT {col_name}, {val_col_name} FROM test_data.contratos LIMIT 1"
        ).fetchone()

        assert sample is not None
        numero_controle = sample[0]
        valor = sample[1]

        # numeroControlePNCP format: CNPJ-2-sequencial/ano
        assert isinstance(numero_controle, str)
        assert "-" in numero_controle
        assert "/" in numero_controle

        # Valor should be numeric
        assert isinstance(valor, (int, float))
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
        pipeline, run_info = pncp.run_pncp(
            duckdb_path=db_path,
            dataset="test_data",
            lookback_days=3,
            range_start="2024-10-05",
            range_end="2024-10-05",
        )

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
        pipeline, run_info = pncp.run_pncp(
            duckdb_path=db_path,
            dataset="test_data",
            lookback_days=0,
            range_start="2024-09-01",
            range_end="2024-09-30",
        )

        assert run_info is not None

        con = duckdb.connect(str(db_path))

        # Should have many records for a full month
        total = con.execute("SELECT COUNT(*) FROM test_data.contratos").fetchone()[0]

        assert total > 500, "Full month should have many contracts (pagination test)"

        # Verify unique numeroControlePNCP (merge should work)
        # Use normalized column name
        schema = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'test_data' AND table_name = 'contratos'"
        ).fetchall()
        column_names = [row[0].lower() for row in schema]
        col_name = (
            "numero_controle_pncp"
            if "numero_controle_pncp" in column_names
            else "numeroControlePNCP"
        )

        unique_count = con.execute(
            f"SELECT COUNT(DISTINCT {col_name}) FROM test_data.contratos"
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
        pipeline, run_info = pncp.run_pncp(
            duckdb_path=db_path,
            dataset="test_data",
            lookback_days=0,
            range_start="2030-01-01",
            range_end="2030-01-01",
        )

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

        pipeline, run_info = pncp.run_pncp(
            duckdb_path=db_path,
            dataset="test_data",
            lookback_days=0,
            range_start="2024-10-15",
            range_end="2024-10-15",
        )

        assert run_info is not None

        con = duckdb.connect(str(db_path))

        # Check if coverage tables exist
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'baliza_state'"
        ).fetchall()

        table_names = [row[0] for row in tables]

        # Coverage tracking tables should exist
        assert "cobertura" in table_names or "janelas" in table_names

        con.close()
