import pytest
from typer.testing import CliRunner
from baliza.cli import app
from unittest.mock import patch, MagicMock

runner = CliRunner()

@pytest.fixture
def mock_manager():
    with patch("baliza.cli.StateManager") as MockStateManager:
        manager = MockStateManager.return_value
        # Default behavior: tracker returns empty list (no state)
        manager.tracker.fetch_window_statuses.return_value = []
        # Default behavior: no last run
        manager.get_last_successful_run.return_value = None
        yield manager

def test_state_show_empty_state(mock_manager):
    """
    Test that 'baliza state show' displays a helpful empty state Panel
    when no extraction statuses are found.
    """
    # Setup: tracker returns empty list for statuses (default from fixture)
    mock_manager.tracker.fetch_window_statuses.return_value = []

    result = runner.invoke(app, ["state", "show", "--resource", "contratos", "--duckdb", ":memory:"])

    assert result.exit_code == 0
    # Check for the Panel title
    assert "No State Found" in result.stdout
    # Check for the helpful message
    assert "No extraction state found for resource" in result.stdout
    assert "baliza extract --resource contratos" in result.stdout

def test_state_show_with_data(mock_manager):
    """
    Test that 'baliza state show' displays the table when data exists.
    """
    # Setup: tracker returns some statuses
    mock_manager.tracker.fetch_window_statuses.return_value = [
        {"status": "ok"}, {"status": "ok"}, {"status": "incompleto"}
    ]

    # Setup: last_run returns a mock object with string attributes to satisfy Rich
    mock_run = MagicMock()
    mock_run.run_id = "run-123"
    mock_run.completed_at = "2023-01-01"
    mock_run.windows_completed = 10
    mock_run.rows_extracted = 1000
    mock_manager.get_last_successful_run.return_value = mock_run

    result = runner.invoke(app, ["state", "show", "--resource", "contratos", "--duckdb", ":memory:"])

    if result.exit_code != 0:
        print(result.stdout) # For debugging if it fails

    assert result.exit_code == 0
    # Should NOT show the empty state panel
    assert "No State Found" not in result.stdout
    # Should show the table content
    assert "State Summary for 'contratos'" in result.stdout
    assert "Complete windows" in result.stdout
