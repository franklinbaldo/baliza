"""BDD step definitions for verification.feature (Tier 1: Core Features)."""

from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

pytestmark = pytest.mark.tier1

runner = CliRunner()

# Mark all verification scenarios as Tier 1 (Core Features)


# =============================================================================
# Scenario: Verify command detects gaps
# =============================================================================


@scenario("../features/verification.feature", "Verify command detects gaps")
def test_verify_detects_gaps():
    pass


@given("I have extracted data for 2024-01-01 to 2024-01-10", target_fixture="partial_coverage")
def partial_coverage(tmp_path: Path) -> Path:
    """Setup: Create database with partial coverage (gaps from 01-05 to 01-07)."""
    db_path = tmp_path / "test.duckdb"

    with duckdb.connect(str(db_path)) as con:
        # Create schemas
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("CREATE SCHEMA IF NOT EXISTS test_dataset")

        # Create coverage table
        con.execute("""
            CREATE TABLE baliza_state.coverage (
                resource VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                status VARCHAR,
                total_paginas INTEGER,
                rows_extracted INTEGER,
                extracted_at TIMESTAMP
            )
        """)

        # Insert coverage for 2024-01-01 to 2024-01-04 (complete)
        for day in range(1, 5):
            con.execute(
                """
                INSERT INTO baliza_state.coverage VALUES (
                    'contratos',
                    ?,
                    ?,
                    'complete',
                    10,
                    100,
                    NOW()
                )
            """,
                [datetime(2024, 1, day), datetime(2024, 1, day, 23, 59, 59)],
            )

        # GAP: 2024-01-05 to 2024-01-07 (missing)

        # Insert coverage for 2024-01-08 to 2024-01-10 (complete)
        for day in range(8, 11):
            con.execute(
                """
                INSERT INTO baliza_state.coverage VALUES (
                    'contratos',
                    ?,
                    ?,
                    'complete',
                    10,
                    100,
                    NOW()
                )
            """,
                [datetime(2024, 1, day), datetime(2024, 1, day, 23, 59, 59)],
            )

    return db_path


@given("2024-01-05 to 2024-01-07 are missing")
def missing_dates():
    """This is handled by the partial_coverage fixture."""
    pass


@when(
    parsers.parse('I run "baliza verify --resource {resource} --start {start} --end {end}"'),
    target_fixture="verify_result",
)
def run_verify(partial_coverage, resource, start, end):
    """Run baliza verify command."""
    result = runner.invoke(
        app,
        [
            "verify",
            "--resource",
            resource,
            "--start",
            start,
            "--end",
            end,
            "--duckdb",
            str(partial_coverage),
        ],
    )

    if result.exit_code != 0:
        print("\n=== VERIFY FAILED ===")
        print(f"Exit code: {result.exit_code}")
        print(f"Output:\n{result.stdout}")
        if result.exception:
            import traceback

            traceback.print_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )

    return {"result": result, "db_path": partial_coverage}


@then("the output should show gaps for 2024-01-05 to 2024-01-07")
def check_gaps_shown(verify_result):
    """Verify gaps are shown in output."""
    output = verify_result["result"].stdout
    # Check that the gap dates appear in the output
    assert "2024-01-05" in output or "01-05" in output, "Gap start date not found in output"
    assert "2024-01-07" in output or "01-07" in output, "Gap end date not found in output"
    # Check for gap indicator
    assert "gap" in output.lower() or "⚠" in output, "No gap indicator found"


@then("the command should exit successfully")
def check_verify_success(verify_result):
    """Verify command exited with code 0 (gaps are not errors, just info)."""
    assert verify_result["result"].exit_code == 0, f"Exit code: {verify_result['result'].exit_code}"
