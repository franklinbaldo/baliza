import os
import shutil
from pathlib import Path
from typer.testing import CliRunner
import duckdb
import pytest
from baliza.cli_simple import app

runner = CliRunner()

@pytest.fixture
def clean_db():
    db_path = "test_security.duckdb"
    if os.path.exists(db_path):
        os.remove(db_path)

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
    con.execute("CREATE TABLE baliza_raw.test_table (id INTEGER)")
    con.execute("INSERT INTO baliza_raw.test_table VALUES (1)")
    con.close()

    yield db_path

    if os.path.exists(db_path):
        os.remove(db_path)

def test_export_sql_injection_protection(clean_db):
    """Test that SQL injection via --output argument is prevented."""
    db_path = clean_db

    # Payload trying to inject a CREATE TABLE command
    # This corresponds to: COPY ... TO 'exploit'; CREATE TABLE baliza_raw.hacked (id INTEGER); --/test_table.parquet'
    exploit_dir = "exploit'; CREATE TABLE baliza_raw.hacked (id INTEGER); --"

    result = runner.invoke(app, [
        "export",
        "--table", "test_table",
        "--output", exploit_dir,
        "--duckdb", db_path,
        "--dataset", "baliza_raw"
    ])

    # The command should succeed (it will try to create the weirdly named directory/file)
    # or fail with OS error if the path is invalid, but it must NOT execute the injected SQL.
    # In our verify script it succeeded because it's a valid path on Linux.

    assert result.exit_code == 0 or "Export failed" in result.stdout

    # Verify the injected table was NOT created
    con = duckdb.connect(db_path, read_only=True)
    with pytest.raises(duckdb.CatalogException, match="Table with name hacked does not exist"):
        con.execute("SELECT * FROM baliza_raw.hacked")
    con.close()

    # Clean up the weird directory if it was created
    if os.path.exists(exploit_dir):
        shutil.rmtree(exploit_dir)
    # Also check for 'exploit' directory if the injection worked partially as a file write
    if os.path.exists("exploit"):
         # It might be a file if the COPY TO 'exploit' part executed
         if os.path.isdir("exploit"):
             shutil.rmtree("exploit")
         else:
             os.remove("exploit")

def test_export_legitimate(clean_db):
    """Test that legitimate export still works."""
    db_path = clean_db
    output_dir = "test_output"

    result = runner.invoke(app, [
        "export",
        "--table", "test_table",
        "--output", output_dir,
        "--duckdb", db_path,
        "--dataset", "baliza_raw"
    ])

    assert result.exit_code == 0
    assert "Exported baliza_raw.test_table" in result.stdout

    expected_file = Path(output_dir) / "test_table.parquet"
    assert expected_file.exists()

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
