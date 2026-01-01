from __future__ import annotations

from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from rich.console import Console
from baliza import cli
from datetime import datetime, timezone

runner = CliRunner()

def test_state_show_empty_state() -> None:
    """Test that state show displays a helpful panel when no statuses exist."""
    with patch("baliza.cli.StateManager") as MockStateManager:
        manager_instance = MockStateManager.return_value
        # Setup mock to return empty status list
        manager_instance.tracker.fetch_window_statuses.return_value = []
        # Setup mock to return no last run
        manager_instance.get_last_successful_run.return_value = None

        result = runner.invoke(cli.app, ["state", "show", "--resource", "test_resource"])

        assert result.exit_code == 0
        # Check for key phrases in the helpful empty state
        assert "No extraction state found" in result.stdout
        assert "baliza extract --resource test_resource" in result.stdout

def test_state_show_populated_state() -> None:
    """Test that state show displays the table with percentages and humanized time."""
    with patch("baliza.cli.StateManager") as MockStateManager:
        manager_instance = MockStateManager.return_value

        # Mock statuses: 5 ok, 5 incompleto (50% each)
        statuses = [{"status": "ok"}] * 5 + [{"status": "incompleto"}] * 5
        manager_instance.tracker.fetch_window_statuses.return_value = statuses

        # Mock last run
        last_run = MagicMock()
        last_run.run_id = "run_123"
        last_run.completed_at = datetime.now(timezone.utc) # Just now
        last_run.windows_completed = 10
        last_run.rows_extracted = 1000
        manager_instance.get_last_successful_run.return_value = last_run

        result = runner.invoke(cli.app, ["state", "show", "--resource", "test_resource"])

        assert result.exit_code == 0

        # Check for table headers
        assert "Status" in result.stdout
        assert "Count" in result.stdout
        assert "Percentage" in result.stdout

        # Check for counts
        assert "5" in result.stdout

        # Check for percentage bars (assuming implementation uses specific characters)
        # We'll look for the percentage text first
        assert "50.0%" in result.stdout

        # Check for humanized time (just now)
        assert "just now" in result.stdout
