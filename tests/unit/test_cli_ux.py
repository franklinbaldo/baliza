from typer.testing import CliRunner
from baliza.cli import app
from rich.console import Console
import shutil
import pytest

runner = CliRunner()

def test_state_show_empty_ux(tmp_path):
    """Test that state show displays a helpful panel when no data exists."""
    db_path = tmp_path / "test_ux.duckdb"

    result = runner.invoke(app, ["state", "show", "--resource", "test_resource", "--duckdb", str(db_path)])

    assert result.exit_code == 0
    assert "No State Data Found" in result.stdout
    assert "No state data found for resource 'test_resource'" in result.stdout
    assert "baliza extract --resource test_resource" in result.stdout
    # Ensure no table borders are printed (indicating table was replaced by panel)
    assert "Percentage" not in result.stdout

def test_state_history_empty_ux(tmp_path):
    """Test that state history displays a helpful panel when no history exists."""
    db_path = tmp_path / "test_ux.duckdb"

    result = runner.invoke(app, ["state", "history", "--resource", "test_resource", "--duckdb", str(db_path)])

    assert result.exit_code == 0
    assert "No History Found" in result.stdout
    assert "No run history found for resource 'test_resource'" in result.stdout
    assert "baliza extract --resource test_resource" in result.stdout
