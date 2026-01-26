import duckdb
import pytest
from pathlib import Path
from typer.testing import CliRunner
from baliza.cli_simple import app

runner = CliRunner()

def test_sql_injection_export(tmp_path):
    """Test that SQL injection in the output path is prevented."""
    db_path = tmp_path / "test.duckdb"

    # Setup database with a table to be targeted
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE baliza_raw.target_table (id INTEGER)")
        con.execute("INSERT INTO baliza_raw.target_table VALUES (1)")
        con.execute("CREATE TABLE baliza_raw.innocent_table (id INTEGER)")
        con.execute("INSERT INTO baliza_raw.innocent_table VALUES (1)")

    # Malicious output path attempting to drop 'target_table'
    # Use a path relative to tmp_path to avoid creating files in repo root
    malicious_dir_name = "safe_dir'; DROP TABLE baliza_raw.target_table; --"
    malicious_output = tmp_path / malicious_dir_name

    # Run the export command
    result = runner.invoke(app, [
        "export",
        "--table", "innocent_table",
        "--output", str(malicious_output),
        "--duckdb", str(db_path),
        "--dataset", "baliza_raw"
    ])

    # Check if the target table still exists
    with duckdb.connect(str(db_path)) as con:
        # Check specifically for the table in the correct schema
        try:
            con.execute("SELECT * FROM baliza_raw.target_table")
            target_exists = True
        except duckdb.CatalogException:
            target_exists = False

    if not target_exists:
        pytest.fail("SQL Injection successful! 'target_table' was dropped.")

    # Assert that the injection failed (table exists)
    assert target_exists

    # Assert that the command actually succeeded (it should treat the path literally)
    # If the fix caused a syntax error, exit_code would be non-zero (likely 1)
    if result.exit_code != 0:
        print(f"Command failed with output: {result.stdout}")

    assert result.exit_code == 0
    assert "Exported baliza_raw.innocent_table to" in result.stdout
