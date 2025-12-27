from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
from typer.testing import CliRunner

from baliza import cli

runner = CliRunner()


def _prepare_duckdb(tmp_path: Path) -> tuple[Path, str, str]:
    db_path = tmp_path / "test.duckdb"
    schema = "baliza_raw"
    table = "contratos"

    with duckdb.connect(str(db_path)) as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
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

def test_export_ia_link(tmp_path: Path, monkeypatch) -> None:
    db_path, schema, table = _prepare_duckdb(tmp_path)
    out_dir = tmp_path / "cli_out"

    # Mock IA session
    mock_session = MagicMock()
    mock_item = MagicMock()
    mock_session.get_item.return_value = mock_item

    mock_get_session = MagicMock(return_value=mock_session)
    monkeypatch.setattr("baliza.cli.get_session", mock_get_session)

    # Use a temporary path for the metadata file to avoid polluting the repo
    metadata_path = tmp_path / "ia-summary.json"

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
            "--ia-identifier",
            "test-id-123",
            "--ia-access-key",
            "fake-key",
            "--ia-secret-key",
            "fake-secret",
            "--ia-metadata-path",
            str(metadata_path)
        ],
    )

    # Check that the IA link is present in stderr
    assert result.exit_code == 0
    assert "Successfully uploaded to Internet Archive:" in result.stderr
    assert "https://archive.org/details/test-id-123" in result.stderr

    print("Stderr output:")
    print(result.stderr)

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
