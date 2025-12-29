from typer.testing import CliRunner
from baliza.cli import app
from rich.console import Console

runner = CliRunner()

def test_state_show_empty_state(mocker):
    """Test that state show displays a helpful panel when no state is found."""
    # Mock StateManager and its tracker to return empty statuses
    mocker.patch("baliza.cli.StateManager")
    mock_tracker = mocker.Mock()
    mock_tracker.fetch_window_statuses.return_value = []

    # We need to mock StateManager to return our mock tracker
    mock_manager_instance = mocker.Mock()
    mock_manager_instance.tracker = mock_tracker
    mock_manager_instance.get_last_successful_run.return_value = None

    mocker.patch("baliza.cli.StateManager", return_value=mock_manager_instance)

    # Run the command
    result = runner.invoke(app, ["state", "show", "--resource", "contratos"])

    assert result.exit_code == 0
    # Check for parts of the panel text
    assert "No extraction state found for resource 'contratos'" in result.stdout
    assert "To start a new extraction, run:" in result.stdout
    assert "baliza extract --resource contratos" in result.stdout
    assert "No State Found" in result.stdout
