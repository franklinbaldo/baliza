"""Step definitions for the end-to-end extraction feature."""

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


@pytest.mark.skip(reason="Quarantined due to persistent timeout issues with pytest-httpx")
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
    target_fixture="external_data_source",
)
def external_data_source(httpx_mock):
    """Mock the external data source for the PNCP API."""

    def mock_pncp_api(request: httpx.Request):
        """Mock the PNCP API, responding based on date range query params."""
        parsed_url = urlparse(str(request.url))
        query_params = parse_qs(parsed_url.query)

        start_date = query_params.get("dataInicial", [None])[0]
        end_date = query_params.get("dataFinal", [None])[0]

        # Data for day 1
        record_1 = {
            "cnpj": "12345678901234",
            "dataAssinatura": "2024-01-01",
            "numeroControlePNCP": "PNCP-1",
        }
        # Data for day 2
        record_2 = {
            "cnpj": "56789012345678",
            "dataAssinatura": "2024-01-02",
            "numeroControlePNCP": "PNCP-2",
        }

        # First extraction: only day 1
        if start_date == "20240101" and end_date == "20240101":
            return httpx.Response(
                200, json={"data": [record_1], "totalPaginas": 1}
            )

        # Second extraction: day 1 and day 2
        if start_date == "20240101" and end_date == "20240102":
            return httpx.Response(
                200, json={"data": [record_1, record_2], "totalPaginas": 1}
            )

        # Fallback for any other request
        return httpx.Response(200, json={"data": [], "totalPaginas": 0})

    # Add the callback and make it reusable for multiple `extract` calls
    httpx_mock.add_callback(mock_pncp_api, is_reusable=True)


@when("I extract data for the first half of the date range")
def extract_first_half(db_path, external_data_source):
    """Extract data for the first half of the date range."""
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
def extract_full_range(db_path, external_data_source):
    """Extract data for the full date range."""
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
