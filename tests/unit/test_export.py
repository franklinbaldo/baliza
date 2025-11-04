from __future__ import annotations

import json
from pathlib import Path

import duckdb
from typer.testing import CliRunner

from baliza import cli
from baliza.utils import export_parquet

runner = CliRunner()


def _prepare_duckdb(tmp_path: Path) -> tuple[Path, str, str]:
    db_path = tmp_path / "test.duckdb"
    schema = "baliza_raw"
    table = "contratos"

    with duckdb.connect(str(db_path)) as con:
        con.execute(f"CREATE SCHEMA {schema}")
        con.execute(
            f"""
            CREATE TABLE {schema}.{table} (
                id INTEGER,
                data DATE,
                dataAtualizacao DATE
            )
            """
        )
        con.execute(
            f"""
            INSERT INTO {schema}.{table}
            VALUES
                (1, DATE '2024-01-15', DATE '2024-01-16'),
                (2, DATE '2024-02-20', DATE '2024-02-21'),
                (3, DATE '2024-02-25', DATE '2024-02-26')
            """
        )

    return db_path, schema, table


def test_export_writes_year_month_partitions(tmp_path: Path) -> None:
    db_path, schema, table = _prepare_duckdb(tmp_path)
    out_dir = tmp_path / "out"

    metadata = export_parquet(
        duckdb_path=db_path,
        dataset=schema,
        table=table,
        out_dir=out_dir,
        date_col="data",
        fallback_date_cols=["dataAtualizacao"],
    )

    assert metadata.rows_exported == 3
    assert metadata.partition_count == 2
    partition_paths = {
        p.relative_to(out_dir).as_posix() for p in out_dir.glob("ano=*/mes=*") if p.is_dir()
    }
    assert partition_paths == {"ano=2024/mes=1", "ano=2024/mes=2"}


def test_export_uses_fallback_column(tmp_path: Path) -> None:
    db_path, schema, table = _prepare_duckdb(tmp_path)
    out_dir = tmp_path / "out_fallback"

    metadata = export_parquet(
        duckdb_path=db_path,
        dataset=schema,
        table=table,
        out_dir=out_dir,
        date_col="dataInexistente",
        fallback_date_cols=["dataAtualizacao"],
    )

    assert metadata.date_column == "dataAtualizacao"
    assert metadata.rows_exported == 3


def test_cli_export_command(tmp_path: Path) -> None:
    db_path, schema, table = _prepare_duckdb(tmp_path)
    out_dir = tmp_path / "cli_out"

    result = runner.invoke(
        cli.app,
        [
            "export",
            "--duckdb",
            str(db_path),
            "--dataset",
            schema,
            "--table",
            table,
            "--out",
            str(out_dir),
            "--date-col",
            "data",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-02-28",
        ],
    )

    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["rows_exported"] == 3
    assert payload["partitions"] == [
        {"ano": 2024, "mes": 1},
        {"ano": 2024, "mes": 2},
    ]
