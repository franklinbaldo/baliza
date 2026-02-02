

import duckdb
from typer.testing import CliRunner

from src.baliza.cli_simple import app

runner = CliRunner()

def test_export_sqli_vulnerability(tmp_path):
    """
    Test that SQL injection in --output parameter is prevented.
    """
    db_path = tmp_path / "test.duckdb"

    # Create a dummy database with data
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
        con.execute("CREATE TABLE IF NOT EXISTS baliza_raw.contratos (id INTEGER)")
        con.execute("INSERT INTO baliza_raw.contratos VALUES (1)")

    # Malicious output path attempting to inject SQL
    # This payload attempts to create a table named 'injected'
    # The vulnerability constructs: parquet_file = output / "contratos.parquet"
    # Query: COPY ... TO '{parquet_file}' ...

    # Payload: test.parquet' (FORMAT PARQUET); CREATE TABLE injected (id INTEGER); --
    # This will be interpreted as the output directory.
    # The full path passed to COPY will be:
    # "test.parquet' (FORMAT PARQUET); CREATE TABLE injected (id INTEGER); --/contratos.parquet"

    malicious_output = "test.parquet' (FORMAT PARQUET); CREATE TABLE injected (id INTEGER); --"

    # We need to run this test in a directory where we can create this weird directory name
    # tmp_path is perfect.

    # We pass the absolute path to malicious output to ensure it works regardless of CWD,
    # but the injection relies on the string being used in SQL.
    # If we pass absolute path, it works same way.
    # But typically one passes relative path.

    with runner.isolated_filesystem(temp_dir=tmp_path):
        # Run the export command
        result = runner.invoke(app, [
            "export",
            "--table", "contratos",
            "--output", malicious_output,
            "--duckdb", str(db_path),
            "--dataset", "baliza_raw"
        ])

        # Check if the command ran
        # It might fail if the path is too long or invalid on some OS, but on Linux it should be fine.
        # Even if it fails, we care about the side effect.

        print(result.stdout)

        # Verify if 'injected' table exists in the database
        with duckdb.connect(str(db_path)) as con:
            # We need to switch to default schema or check all tables
            # 'injected' table would likely be created in default schema 'main' or current schema 'baliza_raw'
            # depending on how DuckDB handles it. The payload didn't specify schema.
            tables = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = [t[0] for t in tables]

            print(f"Tables found: {table_names}")

            assert "injected" not in table_names, "CRITICAL: SQL Injection successful! 'injected' table was created."
