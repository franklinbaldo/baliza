"""BDD step definitions for verification.feature.

Seam: patches `baliza.ia_uploader.try_read_manifest_from_ia` via the
`mock_ia_manifest` conftest fixture so the CLI never hits archive.org.
"""

from __future__ import annotations

import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

pytestmark = pytest.mark.tier1


@scenario(
    "../features/verification.feature",
    "Verify reports complete coverage when manifest lists every month",
)
def test_verify_complete_coverage():
    pass


@scenario(
    "../features/verification.feature",
    "Verify reports gaps when manifest is missing months",
)
def test_verify_gaps():
    pass


@scenario(
    "../features/verification.feature",
    "Verify tolerates a manifest read failure",
)
def test_verify_manifest_failure():
    pass


@given("the IA manifest already lists every month from 2023-01 to 2023-12")
def manifest_full_2023(mock_ia_manifest):
    # The verify CLI iterates from --start through the previous calendar
    # month (not --end), so to simulate "complete coverage" we stock the
    # manifest with every month from 2023-01 up to the current year.
    from datetime import date

    today = date.today()
    rows: list[dict] = []
    for year in range(2023, today.year + 1):
        last_month = 12 if year < today.year else today.month
        for m in range(1, last_month + 1):
            rows.append({"data_particao": f"{year}-{m:02d}", "table_name": "contratos"})
    mock_ia_manifest(rows)


@given("the IA manifest lists only 2023-01 and 2023-02")
def manifest_partial_2023(mock_ia_manifest):
    mock_ia_manifest(
        [
            {"data_particao": "2023-01", "table_name": "contratos"},
            {"data_particao": "2023-02", "table_name": "contratos"},
        ]
    )


@given("the IA manifest read fails")
def manifest_read_fails(mock_ia_manifest):
    mock_ia_manifest(None)


@when(
    parsers.parse('I run "baliza verify --resource {resource} --start {start} --end {end}"'),
    target_fixture="verify_result",
)
def run_verify(resource: str, start: str, end: str):
    result = runner.invoke(
        app,
        ["verify", "--resource", resource, "--start", start, "--end", end],
    )
    return result


@then("the command should exit successfully")
def check_verify_success(verify_result):
    assert verify_result.exit_code == 0, (
        f"exit={verify_result.exit_code}\noutput:\n{verify_result.stdout}"
    )


@then(parsers.parse('the output should mention "{fragment}"'))
def check_output_contains(verify_result, fragment: str):
    assert fragment in verify_result.stdout, (
        f"fragment {fragment!r} not in output:\n{verify_result.stdout}"
    )


@then(parsers.parse('the output should mention a missing month "{month}"'))
def check_missing_month(verify_result, month: str):
    assert month in verify_result.stdout, (
        f"missing month {month!r} not listed:\n{verify_result.stdout}"
    )
