"""Step definitions for historical backfill."""

from pathlib import Path

import duckdb
import httpx
import pytest
from pytest_bdd import given, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

# Mark all scenarios in this file with tier1
pytestmark = pytest.mark.tier1


@scenario("../features/backfill.feature", "Backfill processes full months sequentially")
def test_backfill_sequentially():
    """Backfill processes full months sequentially."""


@scenario("../features/backfill.feature", "Backfill creates a separate pipeline run for each month")
def test_backfill_runs():
    """Backfill creates a separate pipeline run for each month."""


@given("a clean local data store", target_fixture="db_path")
def db_path(tmp_path: Path) -> Path:
    """Create a clean DuckDB database."""
    db = tmp_path / "test_backfill.duckdb"
    if db.exists():
        db.unlink()
    return db


@when('I run the "backfill" command from "2024-01" to "2024-02"', target_fixture="result")
def run_backfill_range(db_path, monkeypatch):
    """Run backfill for a range of months."""

    def mock_get(self, url, params=None, **kwargs):
        # Return dummy data
        return httpx.Response(
            200,
            json={
                "data": [{"numeroControlePNCP": f"test-{params['pagina']}"}],
                "totalPaginas": 1,
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.Client, "get", mock_get)

    result = runner.invoke(
        app, ["backfill", "2024-01", "2024-02", "--duckdb", str(db_path)]
    )
    return result


@when('I run the "backfill" command for "2024-03"', target_fixture="result")
def run_backfill_single(db_path, monkeypatch):
    """Run backfill for a single month."""

    def mock_get(self, url, params=None, **kwargs):
        return httpx.Response(
            200,
            json={
                "data": [{"numeroControlePNCP": "test-3"}],
                "totalPaginas": 1,
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.Client, "get", mock_get)

    result = runner.invoke(app, ["backfill", "2024-03", "--duckdb", str(db_path)])
    return result


@then("the command should extract data for January 2024")
def check_january_data(db_path):
    """Verify data was extracted for January."""
    with duckdb.connect(str(db_path)) as con:
        # Check if coverage recorded it
        count = con.execute(
            "SELECT COUNT(*) FROM baliza_state.coverage WHERE window_start >= '2024-01-01' AND window_end <= '2024-01-31'"
        ).fetchone()[0]
        assert count == 1


@then("the command should extract data for February 2024")
def check_february_data(db_path):
    """Verify data was extracted for February."""
    with duckdb.connect(str(db_path)) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM baliza_state.coverage WHERE window_start >= '2024-02-01' AND window_end <= '2024-02-29'"
        ).fetchone()[0]
        assert count == 1


@then('the coverage table should show both months as "complete"')
def check_coverage(db_path):
    """Verify coverage records."""
    with duckdb.connect(str(db_path)) as con:
        coverage = con.execute(
            "SELECT COUNT(*) FROM baliza_state.coverage WHERE status = 'complete'"
        ).fetchone()[0]
        assert coverage >= 2


@then('the run history should show a completed run for the "backfill" pipeline')
def check_run_history(db_path):
    """Verify run history."""
    with duckdb.connect(str(db_path)) as con:
        runs = con.execute(
            "SELECT status, pipeline_name FROM baliza_state.runs WHERE pipeline_name = 'backfill'"
        ).fetchall()
        assert any(run[0] == "completed" for run in runs)


@then('the resource should be "contratos"')
def check_resource(db_path):
    """Verify resource name in run history."""
    with duckdb.connect(str(db_path)) as con:
        resource = con.execute("SELECT DISTINCT resource FROM baliza_state.runs").fetchone()[0]
        assert resource == "contratos"
