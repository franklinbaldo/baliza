"""Step definitions for the end-to-end extraction feature."""

from pathlib import Path
import duckdb
import httpx
from pytest_bdd import given, scenario, then, when, parsers
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


# --- Scenario: Extracting data for a specific date range ---


@scenario(
    "../features/end_to_end_extraction.feature",
    "Extracting data for a specific date range",
)
def test_extracting_data_for_a_specific_date_range():
    """Test extracting data for a specific date range."""


@scenario(
    "../features/end_to_end_extraction.feature",
    "Extracting data for a date range with no data",
)
def test_extracting_data_for_a_date_range_with_no_data():
    """Test extracting data for a date range with no data."""


@given("a clean local database", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


@given("a mock PNCP API server", target_fixture="mock_api")
def mock_api(httpx_mock):
    """Mock the PNCP API to return specific data."""
    httpx_mock.add_response(
        url="https://pncp.gov.br/api/consulta/v1/contratos?dataInicial=20240101&dataFinal=20240102&pagina=1&tamanhoPagina=500",
        json={
            "data": [
                {"numeroControlePNCP": "PNCP-1", "valorInicial": 100.0},
                {"numeroControlePNCP": "PNCP-2", "valorInicial": 200.0},
            ],
            "totalPaginas": 1,
        },
    )


@given("a mock PNCP API server that will return no data for a specific date range")
def mock_api_no_data(httpx_mock):
    """Mock the PNCP API to return no data."""
    httpx_mock.add_response(
        url="https://pncp.gov.br/api/consulta/v1/contratos?dataInicial=20240103&dataFinal=20240104&pagina=1&tamanhoPagina=500",
        json={"data": [], "totalPaginas": 0},
    )


@when(parsers.parse('I run the "extract" command with a specific start and end date'))
def run_extract_command(db_path):
    """Run the extract command with a specific date range."""
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


@when(parsers.parse('I run the "extract" command with that date range'))
def run_extract_command_no_data(db_path):
    """Run the extract command with a date range that has no data."""
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            "2024-01-03",
            "--end",
            "2024-01-04",
            "--duckdb",
            str(db_path),
            "--dataset",
            "test_dataset",
        ],
    )
    assert result.exit_code == 0, f"Command failed: {result.stdout}"


@then(parsers.parse("the database should contain the correct number of records for that date range"))
def check_record_count(db_path):
    """Verify the number of records in the database."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM test_dataset.contratos"
        ).fetchone()[0]
        assert count == 2, f"Expected 2 records, but found {count}"


@then("the database should contain records with the expected data")
def check_record_data(db_path):
    """Verify the data of the records in the database."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        records = con.execute(
            "SELECT numeroControlePNCP, valorInicial FROM test_dataset.contratos ORDER BY numeroControlePNCP"
        ).fetchall()
        assert records == [("PNCP-1", 100.0), ("PNCP-2", 200.0)]


@then("the database should contain zero records")
def check_zero_records(db_path):
    """Verify that the database contains zero records."""
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            count = con.execute(
                "SELECT COUNT(*) FROM test_dataset.contratos"
            ).fetchone()[0]
            assert count == 0, f"Expected 0 records, but found {count}"
    except duckdb.CatalogException:
        # If the table doesn't exist, that also means there are no records.
        pass
