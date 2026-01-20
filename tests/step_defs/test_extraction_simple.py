"""BDD step definitions for extraction (simple, no dlt)."""

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner
from unittest.mock import Mock
import httpx

from baliza.cli_simple import app

runner = CliRunner()

# Mark all extraction scenarios as Tier 0 (Critical Path)
pytestmark = pytest.mark.tier0


# =============================================================================
# Scenario 1: Basic extraction works
# =============================================================================


@scenario('../features/extraction.feature', 'Basic extraction works')
def test_basic_extraction_works():
    pass


@given("a clean DuckDB database", target_fixture="db_path")
def clean_db(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@when(parsers.parse('I run "baliza extract --start {start} --end {end}"'), target_fixture="run_result")
def run_extract(db_path, monkeypatch, start, end):
    """Run baliza extract command with mocked HTTP."""

    # Mock HTTP response from PNCP API
    def mock_get(url, params=None):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": [
                {
                    "numeroControlePNCP": f"TEST-{params.get('pagina', 1)}-001",
                    "anoCompra": 2024,
                    "sequencialCompra": 1,
                    "orgaoEntidade": {"cnpj": "12345678000190", "razaoSocial": "Test Org"},
                    "valorInicial": 10000.00,
                    "dataPublicacao": "2024-01-01T10:00:00",
                    "dataAtualizacao": "2024-01-01T10:00:00",
                },
            ],
            "totalPaginas": 1,
        }

        def raise_for_status():
            if response.status_code >= 400:
                raise httpx.HTTPStatusError("Error", request=Mock(), response=response)

        response.raise_for_status = raise_for_status
        return response

    # Patch httpx.Client.get
    monkeypatch.setattr("httpx.Client.get", mock_get)

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            start,
            "--end",
            end,
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )

    if result.exit_code != 0:
        print(f"\n=== COMMAND FAILED ===")
        print(f"Exit code: {result.exit_code}")
        print(f"Output:\n{result.stdout}")
        if result.exception:
            import traceback

            print(f"Exception:\n{''.join(traceback.format_exception(type(result.exception), result.exception, result.exception.__traceback__))}")

    return {"result": result, "db_path": db_path}


@then("the data should be saved to the database")
def check_data_saved(run_result):
    """Verify data was saved to database."""
    db_path = run_result["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert count > 0, "No data found in database"


@then("the command should exit successfully")
def check_exit_success(run_result):
    """Verify command exited with code 0."""
    assert run_result["result"].exit_code == 0, f"Exit code: {run_result['result'].exit_code}"


# =============================================================================
# Scenario 2: Incremental extraction doesn't duplicate data
# =============================================================================


@scenario('../features/extraction.feature', 'Incremental extraction doesn\'t duplicate data')
def test_incremental_no_duplicates():
    pass


@given("I have previously extracted data for 2024-01-01", target_fixture="previous_extraction")
def previous_extraction(tmp_path: Path, monkeypatch):
    """Setup: run initial extraction for 2024-01-01."""
    db_path = tmp_path / "test.duckdb"
    if db_path.exists():
        db_path.unlink()

    # Mock HTTP response
    def mock_get(url, params=None):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": [
                {
                    "numeroControlePNCP": "TEST-DUPLICATE-001",  # Same ID for both extractions
                    "anoCompra": 2024,
                    "sequencialCompra": 1,
                    "orgaoEntidade": {"cnpj": "12345678000190", "razaoSocial": "Test Org"},
                    "valorInicial": 10000.00,
                    "dataPublicacao": "2024-01-01T10:00:00",
                    "dataAtualizacao": "2024-01-01T10:00:00",  # Old timestamp
                },
            ],
            "totalPaginas": 1,
        }
        response.raise_for_status = lambda: None
        return response

    monkeypatch.setattr("httpx.Client.get", mock_get)

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-01",
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )
    assert result.exit_code == 0
    return db_path


@when(parsers.parse('I run "baliza extract --start {start} --end {end}"'), target_fixture="run_result_incremental")
def run_extract_incremental(previous_extraction, monkeypatch, start, end):
    """Run second extraction with updated data."""

    # Mock HTTP response with UPDATED data for same contract
    def mock_get(url, params=None):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": [
                {
                    "numeroControlePNCP": "TEST-DUPLICATE-001",  # SAME ID
                    "anoCompra": 2024,
                    "sequencialCompra": 1,
                    "orgaoEntidade": {"cnpj": "12345678000190", "razaoSocial": "Test Org UPDATED"},
                    "valorInicial": 15000.00,  # UPDATED value
                    "dataPublicacao": "2024-01-01T10:00:00",
                    "dataAtualizacao": "2024-01-02T10:00:00",  # UPDATED timestamp
                },
                {
                    "numeroControlePNCP": "TEST-NEW-002",  # NEW contract
                    "anoCompra": 2024,
                    "sequencialCompra": 2,
                    "orgaoEntidade": {"cnpj": "12345678000190", "razaoSocial": "Test Org"},
                    "valorInicial": 5000.00,
                    "dataPublicacao": "2024-01-02T10:00:00",
                    "dataAtualizacao": "2024-01-02T10:00:00",
                },
            ],
            "totalPaginas": 1,
        }
        response.raise_for_status = lambda: None
        return response

    monkeypatch.setattr("httpx.Client.get", mock_get)

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            start,
            "--end",
            end,
            "--duckdb",
            str(previous_extraction),
            "--dataset",
            "test_dataset",
        ],
    )

    return {"result": result, "db_path": previous_extraction}


@then("the database should not contain duplicate records")
def check_no_duplicates(run_result_incremental):
    """Verify no duplicate primary keys exist."""
    db_path = run_result_incremental["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # Check for duplicate numeroControlePNCP (primary key)
        query = """
            SELECT numeroControlePNCP, COUNT(*) as cnt
            FROM test_dataset.contratos
            GROUP BY numeroControlePNCP
            HAVING COUNT(*) > 1
        """
        duplicates = con.execute(query).fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate primary keys: {duplicates}"


@then("the 2024-01-01 data should be updated, not duplicated")
def check_data_updated(run_result_incremental):
    """Verify data was updated via INSERT OR REPLACE."""
    db_path = run_result_incremental["db_path"]
    with duckdb.connect(str(db_path), read_only=True) as con:
        # Should have exactly 2 rows total (1 updated + 1 new)
        total = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
        assert total == 2, f"Expected 2 rows, found {total}"

        # Check the updated row has new values
        updated_row = con.execute("""
            SELECT orgaoEntidade_razaoSocial, valorInicial
            FROM test_dataset.contratos
            WHERE numeroControlePNCP = 'TEST-DUPLICATE-001'
        """).fetchone()

        assert updated_row[0] == "Test Org UPDATED", "Row was not updated"
        assert float(updated_row[1]) == 15000.00, "Value was not updated"
