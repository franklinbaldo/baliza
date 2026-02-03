"""Step definitions for the end-to-end extraction feature."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


@scenario(
    "../features/end_to_end_extraction.feature",
    "The data pipeline is resumable and idempotent",
)
def test_pipeline_is_resumable_and_idempotent():
    """The data pipeline is resumable and idempotent."""


@given("a clean local data store", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@given(
    "an external data source for a specific date range",
    target_fixture="mock_get",
)
def external_data_source():
    """Mock the external data source for the PNCP API."""

    def mock_pncp_api(url, params=None, **kwargs):
        """Mock the PNCP API, responding based on date range query params."""
        params = params or {}
        start_date = params.get("dataInicial")
        end_date = params.get("dataFinal")

        # Data for day 1
        record_1 = {
            "numeroControlePNCP": "PNCP-1",
            "anoCompra": 2024,
            "orgaoEntidade": {"cnpj": "12345678901234"},
            "dataPublicacao": "2024-01-01T10:00:00",
        }
        # Data for day 2
        record_2 = {
            "numeroControlePNCP": "PNCP-2",
            "anoCompra": 2024,
            "orgaoEntidade": {"cnpj": "56789012345678"},
            "dataPublicacao": "2024-01-02T10:00:00",
        }

        response = MagicMock()
        response.status_code = 200

        # First extraction: only day 1
        if start_date == "20240101" and end_date == "20240101":
            response.json.return_value = {"data": [record_1], "totalPaginas": 1}
        # Second extraction: day 1 and day 2
        elif start_date == "20240101" and end_date == "20240102":
            response.json.return_value = {"data": [record_1, record_2], "totalPaginas": 1}
        else:
            response.json.return_value = {"data": [], "totalPaginas": 0}

        response.raise_for_status = MagicMock()
        return response

    return mock_pncp_api


@when("I extract data for the first half of the date range")
def extract_first_half(db_path, mock_get):
    """Extract data for the first half of the date range."""
    with patch("httpx.Client.get", side_effect=mock_get):
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
    assert result.exit_code == 0, f"Command failed: {result.stdout}"


@when("then I extract data for the full date range")
def extract_full_range(db_path, mock_get):
    """Extract data for the full date range."""
    with patch("httpx.Client.get", side_effect=mock_get):
        result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-02",
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )
    assert result.exit_code == 0, f"Command failed: {result.stdout}"


@then("the final dataset should contain all records for the full date range")
def check_all_records(db_path):
    """Verify that the final dataset contains all records."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM test_dataset.contratos"
        ).fetchone()[0]
        assert count == 2, f"Expected 2 records, but found {count}"


@then("the final dataset should not contain duplicate records")
def check_no_duplicates(db_path):
    """Verify that the final dataset does not contain duplicate records."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        query = """
            SELECT numeroControlePNCP, COUNT(*) as cnt
            FROM test_dataset.contratos
            GROUP BY numeroControlePNCP
            HAVING COUNT(*) > 1
        """
        duplicates = con.execute(query).fetchall()
        assert not duplicates, f"Found duplicate records: {duplicates}"
