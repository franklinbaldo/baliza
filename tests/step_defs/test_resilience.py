from __future__ import annotations

import os
from datetime import datetime, timedelta

import duckdb
import pytest
from httpx import Response
from pytest_bdd import given, parsers, scenarios, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app
from baliza.extractor import PNCPExtractor

scenarios("../features/resilience.feature")


@pytest.fixture
def db_path(tmp_path):
    """Provides a temporary path for the DuckDB database."""
    return tmp_path / "test.duckdb"


@pytest.fixture
def runner():
    """Provides a Typer CliRunner for invoking the CLI."""
    return CliRunner()


@given("a clean DuckDB database")
def clean_db(db_path):
    """Ensures the database file does not exist before a test."""
    if db_path.exists():
        db_path.unlink()


@given(parsers.parse('the PNCP API will fail for dates after "{fail_date_str}"'))
def mock_api_failure(httpx_mock, fail_date_str):
    """Mocks the PNCP API to return a 500 error for dates after the specified date."""
    fail_date = datetime.strptime(fail_date_str, "%Y-%m-%d").strftime("%Y%m%d")

    def handle_request(request):
        params = {p.split("=")[0]: p.split("=")[1] for p in request.url.query.decode().split("&")}
        if params["dataInicial"] > fail_date:
            return Response(500, text="Internal Server Error")
        return Response(
            200,
            json={
                "data": [
                    {
                        "numeroControlePNCP": f"12345678000195-1-{params['dataInicial']}",
                        "dataAtualizacao": "2024-01-01T12:00:00",
                    }
                ],
                "totalPaginas": 1,
            },
        )

    httpx_mock.add_callback(handle_request, is_reusable=True)


@when(parsers.parse('I run "{command}"'))
def run_command(runner, command, db_path):
    """Runs a CLI command and stores the result."""
    command_args = command.split()[1:] + ["--duckdb", str(db_path)]
    result = runner.invoke(app, command_args)
    # Store the result in the context for later assertions
    runner.result = result


@then("the command should fail")
def check_command_fails(runner):
    """Asserts that the last command exited with a non-zero status code."""
    assert runner.result.exit_code != 0, "Command should have failed but it succeeded."


@then(parsers.parse('the database should contain data for "{date_str}"'))
def check_data_exists(db_path, date_str):
    """Asserts that the database contains data for the specified date."""
    # Ensure the extractor has created the schema
    _ = PNCPExtractor(db_path)
    with duckdb.connect(str(db_path)) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM baliza_raw.contratos WHERE numeroControlePNCP LIKE ?",
            [f"%-{date_str.replace('-', '')}"],
        ).fetchone()[0]
    assert count > 0, f"No data found for {date_str}"


@then(parsers.parse('the database should not contain data for "{date_str}"'))
def check_data_not_exists(db_path, date_str):
    """Asserts that the database does not contain data for the specified date."""
    # Ensure the extractor has created the schema
    _ = PNCPExtractor(db_path)
    with duckdb.connect(str(db_path)) as con:
        count = con.execute(
            "SELECT COUNT(*) FROM baliza_raw.contratos WHERE numeroControlePNCP LIKE ?",
            [f"%-{date_str.replace('-', '')}"],
        ).fetchone()[0]
    assert count == 0, f"Data found for {date_str} when it should not have been."


@then(parsers.parse('the coverage table should mark "{date_str}" as complete'))
def check_coverage_complete(db_path, date_str):
    """Asserts that the coverage table marks the specified date as 'complete'."""
    start_date = datetime.strptime(date_str, "%Y-%m-%d")
    with duckdb.connect(str(db_path)) as con:
        status = con.execute(
            "SELECT status FROM baliza_state.coverage WHERE window_start = ?", [start_date]
        ).fetchone()[0]
    assert status == "completed", f"Expected coverage for {date_str} to be 'complete' but was '{status}'"


@then(parsers.parse('the coverage table should mark "{date_str}" as failed'))
def check_coverage_failed(db_path, date_str):
    """Asserts that the coverage table marks the specified date as 'failed'."""
    start_date = datetime.strptime(date_str, "%Y-%m-%d")
    with duckdb.connect(str(db_path)) as con:
        status = con.execute(
            "SELECT status FROM baliza_state.coverage WHERE window_start = ?", [start_date]
        ).fetchone()[0]
    assert status == "failed", f"Expected coverage for {date_str} to be 'failed' but was '{status}'"


@given(parsers.parse('the previous extraction failed for "{date_str}"'))
def previous_extraction_failed(db_path, date_str):
    """Sets up the database to reflect a failed extraction for the specified date."""
    # Ensure the extractor has created the schema
    _ = PNCPExtractor(db_path)
    start_date = datetime.strptime(date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1) - timedelta(seconds=1)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO baliza_state.coverage
            (resource, window_start, window_end, status, extracted_at)
            VALUES ('contratos', ?, ?, 'failed', NOW())
            """,
            [start_date, end_date],
        )


@given("the PNCP API is now working correctly")
def mock_api_success(httpx_mock):
    """Mocks the PNCP API to return a successful response for all requests."""

    def handle_request(request):
        params = {p.split("=")[0]: p.split("=")[1] for p in request.url.query.decode().split("&")}
        return Response(
            200,
            json={
                "data": [
                    {
                        "numeroControlePNCP": f"98765432000195-1-{params['dataInicial']}",
                        "dataAtualizacao": "2024-01-03T12:00:00",
                    }
                ],
                "totalPaginas": 1,
            },
        )

    httpx_mock.add_callback(handle_request, is_reusable=True)


@then("the command should succeed")
def check_command_succeeds(runner):
    """Asserts that the last command exited with a zero status code."""
    assert runner.result.exit_code == 0, f"Command failed with output: {runner.result.output}"
