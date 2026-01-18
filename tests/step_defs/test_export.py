import duckdb
from pathlib import Path
from pytest_bdd import given, when, then, scenario
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()


@scenario('../features/export.feature', 'Exporting data to Parquet')
def test_exporting_data_to_parquet():
    pass


@given("a DuckDB database with existing data", target_fixture="db_path")
def db_path_with_data(tmp_path: Path) -> Path:
    db_file = tmp_path / "test.duckdb"
    with duckdb.connect(str(db_file)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS test_dataset")
        con.execute("CREATE TABLE test_dataset.contratos (id INTEGER, dataAtualizacao TIMESTAMP, numeroControlePNCP VARCHAR)")
        con.execute("INSERT INTO test_dataset.contratos VALUES (1, '2024-01-01T12:00:00Z', '123')")
    return db_file


@when("I run the baliza export command", target_fixture="export_result")
def run_export_command(db_path, tmp_path):
    output_dir = tmp_path / "output"
    result = runner.invoke(
        app,
        [
            "export",
            "--duckdb", str(db_path),
            "--table", "contratos",
            "--dataset", "test_dataset",
            "--out", str(output_dir),
            "--date-col", "dataAtualizacao"
        ],
    )
    assert result.exit_code == 0
    return output_dir


@then("Parquet files should be created in the output directory")
def check_parquet_files(export_result):
    output_dir = export_result
    parquet_files = list(output_dir.glob("**/*.parquet"))
    assert len(parquet_files) > 0


@scenario('../features/export.feature', 'Exporting data with year and month partitioning')
def test_exporting_data_with_partitioning():
    pass


@given("a DuckDB database with data spanning multiple months", target_fixture="db_path")
def db_path_with_multi_month_data(tmp_path: Path) -> Path:
    db_file = tmp_path / "test.duckdb"
    with duckdb.connect(str(db_file)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS test_dataset")
        con.execute("CREATE TABLE test_dataset.contratos (id INTEGER, dataAtualizacao TIMESTAMP, numeroControlePNCP VARCHAR)")
        con.execute("INSERT INTO test_dataset.contratos VALUES (1, '2024-01-15T12:00:00Z', '123')")
        con.execute("INSERT INTO test_dataset.contratos VALUES (2, '2024-02-20T12:00:00Z', '456')")
        con.execute("INSERT INTO test_dataset.contratos VALUES (3, '2023-12-10T12:00:00Z', '789')")
    return db_file


@when("I run the baliza export command with partitioning enabled", target_fixture="export_result")
def run_export_command_with_partitioning(db_path, tmp_path):
    output_dir = tmp_path / "output_partitioned"
    result = runner.invoke(
        app,
        [
            "export",
            "--duckdb", str(db_path),
            "--table", "contratos",
            "--dataset", "test_dataset",
            "--out", str(output_dir),
            "--date-col", "dataAtualizacao"
        ],
    )
    assert result.exit_code == 0
    return output_dir


@then("the output directory should contain subdirectories for each year and month")
def check_partitioned_directories(export_result):
    output_dir = export_result
    assert (output_dir / "ano=2024" / "mes=1").is_dir()
    assert (output_dir / "ano=2024" / "mes=2").is_dir()
    assert (output_dir / "ano=2023" / "mes=12").is_dir()


@then("each subdirectory should contain at least one Parquet file")
def check_parquet_files_in_partitions(export_result):
    output_dir = export_result
    assert len(list((output_dir / "ano=2024" / "mes=1").glob("*.parquet"))) > 0
    assert len(list((output_dir / "ano=2024" / "mes=2").glob("*.parquet"))) > 0
    assert len(list((output_dir / "ano=2023" / "mes=12").glob("*.parquet"))) > 0
