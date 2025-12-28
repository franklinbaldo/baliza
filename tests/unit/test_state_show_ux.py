from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()


def test_state_show_empty_ux(tmp_path):
    """Test that state show displays a helpful empty state when no data exists."""
    db_path = tmp_path / "test.duckdb"

    # Run the command
    result = runner.invoke(
        app, ["state", "show", "--resource", "contratos", "--duckdb", str(db_path)]
    )

    # Should exit successfully
    assert result.exit_code == 0

    # Should NOT show the zeroed-out table
    assert "Complete windows" not in result.stdout
    assert "0.0%" not in result.stdout

    # Should show the helpful empty state panel
    assert "No state found" in result.stdout
    assert "baliza extract" in result.stdout
