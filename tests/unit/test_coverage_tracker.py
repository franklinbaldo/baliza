from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from baliza.state import CoverageTracker


@pytest.fixture()
def temp_tracker(tmp_path: Path) -> CoverageTracker:
    db_path = tmp_path / "tracker.duckdb"
    tracker = CoverageTracker(db_path, dataset="baliza_raw")
    yield tracker
    tracker.close()


def test_record_page_creates_manifest_and_hash(temp_tracker: CoverageTracker) -> None:
    records = [
        {
            "numeroControlePNCP": "A-1",
            "contratante": {"cnpj": "12345678000195"},
            "sequencialCompra": 1,
            "dataPublicacaoPncp": "2024-01-01T10:00:00Z",
        },
        {
            "numeroControlePNCP": "A-2",
            "contratante": {"cnpj": "12345678000195"},
            "sequencialCompra": 3,
            "dataPublicacaoPncp": "2024-01-01T12:00:00Z",
        },
    ]

    temp_tracker.record_page(
        "contratos",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=2,
        registros=records,
    )

    rows = temp_tracker.conn.execute(
        "SELECT recurso, pagina, total_paginas_observado, hash_ids FROM baliza_state.cobertura"
    ).fetchall()
    assert rows == [("contratos", 1, 2, temp_tracker.hash_registros(records))]

    status_rows = temp_tracker.conn.execute(
        "SELECT status, motivo FROM baliza_state.janelas"
    ).fetchall()
    assert status_rows and status_rows[0][0] == "suspeito"
    assert "salto" in status_rows[0][1]


def test_summarize_windows_detects_missing_periods(
    temp_tracker: CoverageTracker,
) -> None:
    # Prepare sample coverage
    records = [
        {
            "numeroControlePNCP": "B-1",
            "contratante": {"cnpj": "44556677000100"},
            "sequencialCompra": 10,
            "dataPublicacaoPncp": "2024-01-03T09:00:00Z",
        }
    ]
    start = "2024-01-03T00:00:00Z"
    end = "2024-01-04T00:00:00Z"
    temp_tracker.record_page(
        "contratos", start, end, pagina=1, total_paginas=1, registros=records
    )

    # Populate raw dataset with two daily entries (one missing from coverage)
    temp_tracker.conn.execute('CREATE SCHEMA IF NOT EXISTS "baliza_raw"')
    temp_tracker.conn.execute(
        'CREATE TABLE "baliza_raw"."contratos" (numeroControlePNCP TEXT, dataPublicacaoPncp TIMESTAMP)'
    )
    temp_tracker.conn.execute(
        'INSERT INTO "baliza_raw"."contratos" VALUES (?, ?)',
        ("B-1", datetime(2024, 1, 3, 9, 0)),
    )
    temp_tracker.conn.execute(
        'INSERT INTO "baliza_raw"."contratos" VALUES (?, ?)',
        ("B-2", datetime(2024, 1, 4, 9, 0)),
    )

    summary = temp_tracker.summarize_windows("contratos", "contratos")
    assert summary["windows"]
    missing_labels = summary["missing"]
    assert len(missing_labels) == 1
    # The missing window should correspond to 2024-01-04
    assert "2024-01-04" in missing_labels[0]
