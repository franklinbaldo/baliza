import duckdb
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

def test_export_sqli_vulnerability(tmp_path):
    """
    Test that the export command is safe against SQL injection in the output path.
    """
    # Setup a dummy database
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA baliza_raw")
    con.execute("CREATE TABLE baliza_raw.contratos (id INTEGER)")
    con.execute("INSERT INTO baliza_raw.contratos VALUES (1)")
    con.close()

    # Malicious output path containing a single quote
    # We use a path that would definitely cause a syntax error if injected
    # e.g. "out'put" -> TO 'out'put/contratos.parquet' -> Syntax error at 'put'
    malicious_output_dir = tmp_path / "insec'ure_dir"
    # malicious_output_dir.mkdir() # We don't create it, so we might get IO error or 'directory not found', which is fine.

    # Run export command
    result = runner.invoke(app, [
        "export",
        "--table", "contratos",
        "--output", str(malicious_output_dir),
        "--duckdb", str(db_path)
    ])

    print(f"Result exit code: {result.exit_code}")
    print(f"Result output: {result.stdout}")

    # Check for Parser Error (SQL Injection symptom)
    # The vulnerability causes a "Parser Error" because the quote breaks the string literal.
    # The fix should result in either success (if dir existed) or IO Error (file/dir not found),
    # but NEVER a Parser Error.
    assert "Parser Error" not in result.stdout, "SQL Injection detected: Parser Error found in output"
