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

    hash_value = temp_tracker.hash_registros(records)
    assert hash_value is not None
    algoritmo, digest = CoverageTracker.parse_hash_value(hash_value)
    assert algoritmo in {"xxh64", "sha256"}
    assert digest

    rows = temp_tracker.conn.execute(
        "SELECT recurso, pagina, total_paginas_observado, hash_ids FROM baliza_state.cobertura"
    ).fetchall()
    assert rows == [("contratos", 1, 2, hash_value)]

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
    temp_tracker.record_page("contratos", start, end, pagina=1, total_paginas=1, registros=records)

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


def test_parse_hash_value_legacy_formats() -> None:
    algoritmo, digest = CoverageTracker.parse_hash_value("0123456789abcdef")
    assert algoritmo == "xxh64"
    assert digest == "0123456789abcdef"

    algoritmo, digest = CoverageTracker.parse_hash_value("a" * 64)
    assert algoritmo == "sha256"
    assert digest == "a" * 64


def test_hash_registros_requires_xxhash_for_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    registros = [{"numeroControlePNCP": "LEG-1"}]
    monkeypatch.setattr("baliza.state.coverage.xxhash", None)

    fallback_hash = CoverageTracker.hash_registros(registros)
    assert fallback_hash and fallback_hash.startswith("sha256:")

    with pytest.raises(RuntimeError):
        CoverageTracker.hash_registros(registros, algorithm="xxh64", include_algorithm=False)
