from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from baliza import cli
from baliza.state import CoverageTracker

runner = CliRunner()


@pytest.fixture()
def tracker_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "state.duckdb"
    tracker = CoverageTracker(db_path, dataset="baliza_raw")
    records = [
        {
            "numeroControlePNCP": "X-1",
            "contratante": {"cnpj": "99887766000111"},
            "sequencialCompra": 5,
            "dataPublicacaoPncp": "2024-01-01T12:00:00Z",
        }
    ]
    tracker.record_page(
        "contratos",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=1,
        registros=records,
    )
    tracker.conn.execute('CREATE SCHEMA IF NOT EXISTS "baliza_raw"')
    tracker.conn.execute(
        'CREATE TABLE "baliza_raw"."contratos" (numeroControlePNCP TEXT, dataPublicacaoPncp TIMESTAMP)'
    )
    tracker.conn.execute(
        'INSERT INTO "baliza_raw"."contratos" VALUES (?, ?)',
        ("X-1", datetime(2024, 1, 1, 12, 0)),
    )
    tracker.conn.execute(
        'INSERT INTO "baliza_raw"."contratos" VALUES (?, ?)',
        ("X-2", datetime(2024, 1, 2, 12, 0)),
    )
    tracker.close()
    return db_path


def test_verify_reports_missing_pages_and_windows(tracker_db: Path, httpx_mock) -> None:
    httpx_mock.add_response(
        url=re.compile(r"https://pncp\.gov\.br/api/consulta/v1/contratos.*"),
        json={"paginaAtual": 1, "totalPaginas": 3, "data": []},
    )

    result = runner.invoke(
        cli.app,
        [
            "verify",
            "--resource",
            "contratos",
            "--duckdb",
            str(tracker_db),
            "--dataset",
            "baliza_raw",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)

    assert payload["paginas_pendentes"], "Expected pending pages report"
    period_label, pages = next(iter(payload["paginas_pendentes"].items()))
    assert pages == [2, 3]

    lacuna_statuses = {entry["status"] for entry in payload["lacunas"]}
    assert "incompleto" in lacuna_statuses
    assert "nao_processado" in lacuna_statuses

    tracker = CoverageTracker(tracker_db, dataset="baliza_raw")
    try:
        statuses = tracker.fetch_window_statuses("contratos")
    finally:
        tracker.close()
    status_map = {row["status"] for row in statuses}
    assert "incompleto" in status_map
    assert "nao_processado" in status_map
