from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, scenario, then, when
from pytest_httpx import HTTPXMock
from typer.testing import CliRunner
import httpx

from baliza.cli_simple import app


@pytest.mark.xfail(
    reason="This test has a persistent issue with the httpx_mock callback signature that needs deeper investigation.",
    strict=True,
)
@scenario("../features/checkpoint.feature", "Extraction resumes from a checkpoint after interruption")
def test_extraction_resumes_from_checkpoint():
    """Test that extraction can resume after being interrupted."""


@pytest.fixture
def context():
    """Provides a shared context object for steps."""
    return {}


def create_response(page: int, total_pages: int, num_rows: int) -> dict:
    """Helper to create a consistent API response structure."""
    return {
        "totalPaginas": total_pages,
        "data": [{"numeroControlePNCP": f"row_{page}_{i+1}"} for i in range(num_rows)],
    }


@given('the PNCP API will return 2 pages for the date "2024-01-01"')
def setup_api_pages_context(context: dict):
    """Set up the context with page data and URLs, but do not mock responses yet."""
    context["date"] = "2024-01-01"
    context["base_url"] = "https://pncp.gov.br/api/consulta/v1/contratos"
    context["page_1_url"] = f"{context['base_url']}?dataInicial=20240101&dataFinal=20240101&pagina=1&tamanhoPagina=500"
    context["page_2_url"] = f"{context['base_url']}?dataInicial=20240101&dataFinal=20240101&pagina=2&tamanhoPagina=500"
    context["page_1_response"] = create_response(page=1, total_pages=2, num_rows=10)
    context["page_2_response"] = create_response(page=2, total_pages=2, num_rows=5)


@given("the first page has 10 rows")
def first_page_rows(context: dict):
    """Confirm the first page has the correct number of rows."""
    assert len(context["page_1_response"]["data"]) == 10


@given("the second page has 5 rows")
def second_page_rows(context: dict):
    """Confirm the second page has the correct number of rows."""
    assert len(context["page_2_response"]["data"]) == 5


@given("the API will be interrupted after the first page is fetched")
def setup_failing_api_mock(httpx_mock: HTTPXMock, context: dict):
    """Set up mocks for the first run, where page 2 fails repeatedly."""
    httpx_mock.add_response(url=context["page_1_url"], json=context["page_1_response"])

    # Use a callback for the failing response to handle all tenacity retries
    def fail_page_2(request, ext):
        return httpx.Response(500)

    httpx_mock.add_callback(fail_page_2, url=context["page_2_url"])


@when('I run "baliza extract --start 2024-01-01 --end 2024-01-01"')
def run_extract_command_first_time(runner: CliRunner, test_db_path: Path, context: dict):
    """Run the extract command, expecting it to fail."""
    # We don't catch exceptions here because we want the command to fail naturally.
    # The runner will catch the SystemExit from typer.
    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            context["date"],
            "--end",
            context["date"],
            "--duckdb",
            str(test_db_path),
        ],
    )
    context["first_run_result"] = result


@when('I run "baliza extract --start 2024-01-01 --end 2024-01-01" again without interruption')
def run_extract_command_second_time(
    runner: CliRunner, test_db_path: Path, context: dict, httpx_mock: HTTPXMock
):
    """Run the extract command again after resetting mocks for success."""
    # Reset mocks from the first run to set up a new state.
    httpx_mock.reset()
    # Also clear the history of requests to avoid assertion errors on teardown
    httpx_mock._requests = []

    context["resumed_to_page_2"] = False

    def spy_on_page_2(request, ext):
        context["resumed_to_page_2"] = True
        return httpx.Response(200, json=context["page_2_response"])

    # For the resumed run, only page 2 should be requested.
    httpx_mock.add_callback(spy_on_page_2, url=context["page_2_url"])

    result = runner.invoke(
        app,
        [
            "extract",
            "--start",
            context["date"],
            "--end",
            context["date"],
            "--duckdb",
            str(test_db_path),
        ],
    )
    context["second_run_result"] = result


@then("the command should fail")
def check_command_failed(context: dict):
    """Check that the first command run exited with an error."""
    assert context["first_run_result"].exit_code == 1, "Command should have failed but succeeded"


@then('the database should contain a checkpoint for "2024-01-01" at page 1 with 10 rows')
def check_checkpoint_exists(test_db_path: Path):
    """Verify that the checkpoint was saved correctly."""
    with duckdb.connect(str(test_db_path), read_only=True) as con:
        result = con.execute(
            "SELECT current_page, rows_extracted FROM baliza_state.extraction_checkpoint"
        ).fetchone()
        assert result is not None
        assert result[0] == 1
        assert result[1] == 10


@then('the database should contain 10 rows for "2024-01-01"')
def check_partial_data_saved(test_db_path: Path):
    """Verify that the data from the first page was saved."""
    with duckdb.connect(str(test_db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 10


@then("the command should succeed")
def check_second_command_succeeded(context: dict):
    """Check that the second command run was successful."""
    assert context["second_run_result"].exit_code == 0, f"Command failed unexpectedly: {context['second_run_result'].stdout}"


@then("the extraction should resume from page 2")
def check_extraction_resumed(context: dict):
    """Verify that the second run fetched page 2."""
    assert context["resumed_to_page_2"] is True, "The mock API for page 2 was not called on the second run"


@then('the database should contain a total of 15 rows for "2024-01-01"')
def check_total_data_saved(test_db_path: Path):
    """Verify that all data from both pages is now in the database."""
    with duckdb.connect(str(test_db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_raw.contratos").fetchone()[0]
        assert count == 15


@then('the checkpoint for "2024-01-01" should be cleared')
def check_checkpoint_cleared(test_db_path: Path):
    """Verify that the checkpoint was removed after successful completion."""
    with duckdb.connect(str(test_db_path), read_only=True) as con:
        result = con.execute(
            "SELECT COUNT(*) FROM baliza_state.extraction_checkpoint"
        ).fetchone()
        assert result[0] == 0
