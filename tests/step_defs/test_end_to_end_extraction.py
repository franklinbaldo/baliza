"""Step definitions for core data extraction feature."""

from pathlib import Path

import duckdb
import httpx
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


@scenario(
    "../features/end_to_end_extraction.feature",
    "Simple extraction for a date range",
)
def test_simple_extraction():
    """Test simple extraction for a date range."""


@given("a clean local data store")
def a_clean_local_data_store(db_path: Path):
    """This step now just ensures the db_path fixture is activated."""
    pass


@given(parsers.parse('the PNCP API has {count:d} contracts for "{date_str}"'))
def setup_api_mock(httpx_mock, date_str: str, count: int):
    """Set up the mock API to return a specific number of contracts for a date."""

    def mock_pncp_api(request: httpx.Request):
        """Mock the PNCP API, responding based on date range query params."""
        records = [
            {"numeroControlePNCP": f"PNCP-{i}", "dataPublicacao": f"{date_str}T12:00:00"}
            for i in range(count)
        ]
        return httpx.Response(200, json={"data": records, "totalPaginas": 1})

    httpx_mock.add_callback(mock_pncp_api)


@when(parsers.parse('I run the extract command for "{start_date}" to "{end_date}"'))
def run_extract_command(db_path, start_date: str, end_date: str):
    """Run the extract command for a given date range."""
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            start_date,
            "--end",
            end_date,
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )
    assert result.exit_code == 0, f"Command failed: {result.output}"


@then(parsers.parse("the database should contain {count:d} contracts"))
def check_db_record_count(db_path, count: int):
    """Verify the number of records in the database."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        try:
            actual_count = con.execute(
                "SELECT COUNT(*) FROM test_dataset.contratos"
            ).fetchone()[0]
        except duckdb.CatalogException:
            actual_count = 0

    assert actual_count == count, f"Expected {count} records, but found {actual_count}"
