"""Step definitions for the resumability feature."""

from pathlib import Path

import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()


@scenario(
    "../features/automatic_retry.feature",
    "The \"extract\" command succeeds after a transient error",
)
def test_command_succeeds_after_transient_error():
    """The extract command succeeds after a transient error."""


@given("a clean local data store", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test.duckdb"
    if db.exists():
        db.unlink()
    return db


class MockPNCPClient:
    """A mock PNCP client that fails on the first call."""

    def __init__(self):
        self.call_count = 0

    def get(self, url, params):
        """Mock the PNCP API, failing on the first call and succeeding on the second."""
        self.call_count += 1
        req = httpx.Request("GET", url, params=params)

        if self.call_count == 1:
            return httpx.Response(500, request=req)

        record = {
            "cnpj": "12345678901234",
            "dataAssinatura": "2024-01-01",
            "numeroControlePNCP": "PNCP-1",
        }
        return httpx.Response(200, json={"data": [record], "totalPaginas": 1}, request=req)


@pytest.fixture
def mock_client():
    """Provides a mock client that fails once, then succeeds."""
    return MockPNCPClient()


@given("an external data source that will fail once before succeeding")
def external_data_source_that_fails(monkeypatch, mock_client):
    """Mock the external data source to fail on the first request."""
    monkeypatch.setattr(httpx.Client, "get", mock_client.get)


@when("I run the \"extract\" command", target_fixture="extract_result")
def run_extract_command(db_path):
    """Run the extract command."""
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
    return result


@then("the command should succeed")
def check_command_succeeds(extract_result):
    """Verify that the command succeeds."""
    assert extract_result.exit_code == 0, extract_result.stdout


@then("the external data source should have been called twice")
def check_call_count(mock_client):
    """Verify that the external data source was called twice."""
    assert mock_client.call_count == 2


@then("the final dataset should contain all records")
def check_final_dataset(db_path):
    """Verify that the final dataset contains all records."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM test_dataset.contratos"
        ).fetchone()[0]
        assert count == 1
