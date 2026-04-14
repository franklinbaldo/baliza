"""BDD step definitions for export.feature (Tier 0: Critical Path)."""

from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, parsers, scenario, then, when
from typer.testing import CliRunner

from baliza.cli_simple import app

runner = CliRunner()

# Mark all export scenarios as Tier 0 (Critical Path)
pytestmark = pytest.mark.tier0


# =============================================================================
# Scenario: Export creates valid parquet
# =============================================================================


@scenario("../features/export.feature", "Export creates valid parquet")
def test_export_creates_valid_parquet():
    pass


@given(parsers.parse("a DuckDB database with {count:d} contracts"), target_fixture="db_with_data")
def db_with_data(tmp_path: Path, count: int) -> dict:
    """Create a DuckDB database with test data."""
    db_file = tmp_path / "test.duckdb"
    output_dir = tmp_path / "exports"

    with duckdb.connect(str(db_file)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS test_dataset")
        con.execute("""
            CREATE TABLE test_dataset.contratos (
                id INTEGER,
                numero_controle_pncp VARCHAR,
                data_publicacao TIMESTAMP,
                data_atualizacao TIMESTAMP,
                valor_inicial DECIMAL(15,2)
            )
        """)

        # Insert test data
        for i in range(count):
            con.execute(f"""
                INSERT INTO test_dataset.contratos VALUES (
                    {i},
                    'CTRL-{i:06d}',
                    '2024-01-15T10:00:00',
                    '2024-01-15T10:00:00',
                    {1000.00 + i}
                )
            """)

    return {"db_path": db_file, "output_dir": output_dir, "expected_count": count}


@when(
    parsers.parse('I run "baliza export --table {table} --output {output}"'),
    target_fixture="export_result",
)
def run_export(db_with_data, table, output):
    """Run baliza export command."""
    db_path = db_with_data["db_path"]
    output_dir = db_with_data["output_dir"]

    result = runner.invoke(
        app,
        [
            "export",
            "--duckdb",
            str(db_path),
            "--table",
            table,
            "--dataset",
            "test_dataset",
            "--output",
            str(output_dir),
            "--date-col",
            "dataPublicacao",
        ],
    )

    if result.exit_code != 0:
        print("\n=== EXPORT FAILED ===")
        print(f"Exit code: {result.exit_code}")
        print(f"Output:\n{result.stdout}")
        if result.exception:
            import traceback

            traceback.print_exception(
                type(result.exception), result.exception, result.exception.__traceback__
            )

    return {
        "result": result,
        "output_dir": output_dir,
        "expected_count": db_with_data["expected_count"],
    }


@then("Parquet files should be created")
def check_parquet_created(export_result):
    """Verify Parquet files were created."""
    output_dir = export_result["output_dir"]
    parquet_files = list(output_dir.glob("**/*.parquet"))
    assert len(parquet_files) > 0, f"No parquet files found in {output_dir}"


@then(parsers.parse("the Parquet files should contain all {count:d} contracts"))
def check_parquet_content(export_result, count):
    """Verify Parquet files contain expected number of records."""
    output_dir = export_result["output_dir"]
    parquet_files = list(output_dir.glob("**/*.parquet"))

    # Use DuckDB to read and count parquet files
    total_rows = 0
    with duckdb.connect(":memory:") as con:
        for parquet_file in parquet_files:
            result = con.execute(f"SELECT COUNT(*) FROM '{parquet_file}'").fetchone()
            total_rows += result[0]

    assert total_rows == count, f"Expected {count} rows, found {total_rows}"


@then("the command should exit successfully")
def check_export_success(export_result):
    """Verify export command exited with code 0."""
    assert export_result["result"].exit_code == 0, f"Exit code: {export_result['result'].exit_code}"
