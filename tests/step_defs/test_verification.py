"""BDD step definitions for verification.feature (Tier 1: Core Features)."""

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, when, then, scenario, parsers
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()

# Mark all verification scenarios as Tier 1 (Core Features)
pytestmark = pytest.mark.tier1


# =============================================================================
# Scenario: Verify command detects gaps
# =============================================================================


@pytest.mark.skip(reason="Requires state/coverage table setup")
@scenario('../features/verification.feature', 'Verify command detects gaps')
def test_verify_detects_gaps():
    pass


@given("I have extracted data for 2024-01-01 to 2024-01-10", target_fixture="partial_data")
def partial_data(tmp_path: Path) -> Path:
    """Setup: Create database with some extracted data."""
    db_path = tmp_path / "test.duckdb"

    with duckdb.connect(str(db_path)) as con:
        # Create state schema and coverage table
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE baliza_state.coverage (
                resource VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                status VARCHAR,
                total_paginas INTEGER,
                extracted_at TIMESTAMP
            )
        """)

        # Insert coverage for 2024-01-01 to 2024-01-04 (complete)
        for day in range(1, 5):
            con.execute(f"""
                INSERT INTO baliza_state.coverage VALUES (
                    'contratos',
                    '2024-01-{day:02d}T00:00:00',
                    '2024-01-{day:02d}T23:59:59',
                    'complete',
                    10,
                    '2024-01-15T10:00:00'
                )
            """)

        # Gap: 2024-01-05 to 2024-01-07 (missing)

        # Insert coverage for 2024-01-08 to 2024-01-10 (complete)
        for day in range(8, 11):
            con.execute(f"""
                INSERT INTO baliza_state.coverage VALUES (
                    'contratos',
                    '2024-01-{day:02d}T00:00:00',
                    '2024-01-{day:02d}T23:59:59',
                    'complete',
                    10,
                    '2024-01-15T10:00:00'
                )
            """)

    return db_path


@given("2024-01-05 to 2024-01-07 are missing")
def missing_dates():
    """This is handled by the partial_data fixture."""
    pass


@when(parsers.parse('I run "baliza verify --resource {resource} --start {start} --end {end}"'), target_fixture="verify_result")
def run_verify(partial_data, resource, start, end):
    """Run baliza verify command."""
    result = runner.invoke(
        app,
        [
            "verify",
            "--resource",
            resource,
            "--desde",
            start,
            "--hasta",
            end,
            "--duckdb",
            str(partial_data),
        ],
    )
    return {"result": result, "db_path": partial_data}


@then("the output should show gaps for 2024-01-05 to 2024-01-07")
def check_gaps_shown(verify_result):
    """Verify gaps are shown in output."""
    output = verify_result["result"].stdout
    # Check that the gap dates appear in the output
    assert "2024-01-05" in output or "01-05" in output, "Gap start date not found in output"
    assert "2024-01-07" in output or "01-07" in output, "Gap end date not found in output"
