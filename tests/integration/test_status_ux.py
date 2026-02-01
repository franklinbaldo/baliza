import os
from datetime import datetime, timedelta

from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

def test_status_empty_state_ux():
    """Verify the helpful empty state UX when no database exists."""
    # Ensure the file doesn't exist
    db_path = "nonexistent_test.duckdb"
    if os.path.exists(db_path):
        os.remove(db_path)

    result = runner.invoke(app, ["status", "--duckdb", db_path])

    assert result.exit_code == 0
    assert "Welcome to Baliza!" in result.stdout
    assert f"No database found at {db_path}" in result.stdout

    today = datetime.now().date()
    start_date = today - timedelta(days=3)
    # Check for the dynamic date in the suggestion
    assert str(today) in result.stdout
    assert str(start_date) in result.stdout
    assert "baliza extract --start" in result.stdout
