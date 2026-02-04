import duckdb
from typer.testing import CliRunner

from baliza.cli_simple import app
from baliza.utils import escape_sql_literal

runner = CliRunner()

def test_status_dataset_injection(tmp_path):
    """Test that 'status' command rejects invalid dataset names."""
    db_path = tmp_path / "test.duckdb"
    # Create a dummy DB so it doesn't fail on file not found
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (a INTEGER)")

    # Try injection
    result = runner.invoke(app, ["status", "--duckdb", str(db_path), "--dataset", "baliza_raw --"])

    assert result.exit_code != 0
    assert "Invalid identifier" in result.stdout

def test_export_path_injection(tmp_path):
    """Test that 'export' command handles paths with quotes safely."""
    db_path = tmp_path / "test.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA baliza_raw")
        con.execute("CREATE TABLE baliza_raw.contratos (a INTEGER)")
        con.execute("INSERT INTO baliza_raw.contratos VALUES (1)")

    output_dir = tmp_path / "bad'dir"
    output_dir.mkdir()

    result = runner.invoke(app, [
        "export",
        "--table", "contratos",
        "--output", str(output_dir),
        "--duckdb", str(db_path)
    ])

    # It should succeed now that we handle quotes
    assert result.exit_code == 0
    assert "Exported" in result.stdout
    assert (output_dir / "contratos.parquet").exists()

def test_escape_sql_literal():
    """Unit test for escape_sql_literal."""
    assert escape_sql_literal("test") == "test"
    assert escape_sql_literal("test's") == "test''s"
    assert escape_sql_literal("test''s") == "test''''s"
