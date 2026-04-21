"""Verify upgrade path from the pre-flatten camelCase `main.contratos`
schema to the current snake_case shape.

Older BALIZA versions wrote the raw Pydantic dump (camelCase keys, nested
structs) straight to `main.contratos`. The post-flatten path upserts
snake_case scalars with PK `numero_controle_pncp`, which raises
IbisTypeError against a legacy table.

`_archive_legacy_contratos_table` preserves legacy rows under a
timestamp-suffixed archive name (rather than dropping them) because
successful sync runs wipe `data/raw/`, leaving the local DuckDB file as
the only durable copy of months that already shipped to IA.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from baliza.engine import BalizaEngine
from baliza.extractor import PNCPExtractor


def _seed_legacy_contratos(engine: BalizaEngine, pk: str = "LEGACY-001") -> None:
    engine.con.raw_sql(
        """
        CREATE TABLE IF NOT EXISTS main.contratos (
            "numeroControlePNCP" VARCHAR PRIMARY KEY,
            "orgaoEntidade" STRUCT(cnpj VARCHAR, "razaoSocial" VARCHAR),
            "valorInicial" DOUBLE
        )
        """
    )
    engine.con.raw_sql(
        f"""
        INSERT INTO main.contratos VALUES (
            '{pk}', {{'cnpj': '00000000000001', 'razaoSocial': 'Legacy Org'}}, 999.0
        )
        """
    )


def _legacy_archive_tables(engine: BalizaEngine) -> list[str]:
    return [t for t in engine.con.list_tables(database="main") if t.startswith("contratos_legacy_")]


def test_archive_legacy_preserves_rows_under_new_name(tmp_path: Path):
    db = tmp_path / "legacy.duckdb"
    engine = BalizaEngine(db)
    try:
        _seed_legacy_contratos(engine)
        assert "contratos" in engine.con.list_tables(database="main")

        extractor = PNCPExtractor(engine)
        extractor._archive_legacy_contratos_table()

        tables = set(engine.con.list_tables(database="main"))
        assert "contratos" not in tables

        archives = _legacy_archive_tables(engine)
        assert len(archives) == 1, f"expected one archive, got: {archives}"

        count = engine.con.raw_sql(
            f"SELECT COUNT(*) FROM main.{archives[0]}"
        ).fetchone()[0]
        assert count == 1, "legacy rows must survive the archive step"
    finally:
        engine.con.disconnect()


def test_archive_is_noop_for_snake_case_table(tmp_path: Path):
    db = tmp_path / "current.duckdb"
    engine = BalizaEngine(db)
    try:
        engine.con.raw_sql(
            """
            CREATE TABLE main.contratos (
                numero_controle_pncp VARCHAR PRIMARY KEY,
                cnpj_orgao VARCHAR,
                valor_inicial DOUBLE
            )
            """
        )
        engine.con.raw_sql(
            "INSERT INTO main.contratos VALUES ('CURRENT-001', '00000000000001', 42.0)"
        )

        extractor = PNCPExtractor(engine)
        extractor._archive_legacy_contratos_table()

        # Table + row survive, no archive sidecar created.
        tables = set(engine.con.list_tables(database="main"))
        assert "contratos" in tables
        assert _legacy_archive_tables(engine) == []
        count = engine.con.raw_sql("SELECT COUNT(*) FROM main.contratos").fetchone()[0]
        assert count == 1
    finally:
        engine.con.disconnect()


def test_archive_is_noop_when_table_missing(tmp_path: Path):
    db = tmp_path / "empty.duckdb"
    engine = BalizaEngine(db)
    try:
        extractor = PNCPExtractor(engine)
        # Must not raise when the table doesn't exist yet.
        extractor._archive_legacy_contratos_table()
        assert "contratos" not in engine.con.list_tables(database="main")
        assert _legacy_archive_tables(engine) == []
    finally:
        engine.con.disconnect()


def test_archive_tolerates_race_with_another_worker(tmp_path: Path):
    """Simulates the P2 race: this worker decides to archive, then a
    concurrent worker already handled it before our RENAME fires. The
    post-condition (no legacy-shaped contratos) holds; we must not
    raise."""
    db = tmp_path / "raced.duckdb"
    engine = BalizaEngine(db)
    try:
        _seed_legacy_contratos(engine)
        extractor = PNCPExtractor(engine)

        original_raw_sql = engine.con.raw_sql

        def racing_raw_sql(sql, *args, **kwargs):
            # Only intercept the ALTER we're testing; pass everything
            # else (list_tables, schema reads, etc.) through unchanged.
            if isinstance(sql, str) and sql.startswith("ALTER TABLE main.contratos RENAME"):
                original_raw_sql("ALTER TABLE main.contratos RENAME TO contratos_legacy_raced")
                raise RuntimeError("simulated concurrent rename collision")
            return original_raw_sql(sql, *args, **kwargs)

        engine.con.raw_sql = racing_raw_sql  # type: ignore[assignment]

        # Must not raise; post-condition (no legacy shape) holds.
        extractor._archive_legacy_contratos_table()

        tables = set(engine.con.list_tables(database="main"))
        assert "contratos_legacy_raced" in tables
        assert not extractor._has_legacy_contratos_shape()
    finally:
        engine.con.disconnect()


def test_archive_propagates_when_postcondition_fails(tmp_path: Path):
    """If the RENAME fails AND the legacy shape still sits at
    main.contratos, the error must propagate — we haven't solved the
    upgrade problem and silent ignoring would then trip IbisTypeError
    later in ingest_range.
    """
    db = tmp_path / "unrecoverable.duckdb"
    engine = BalizaEngine(db)
    try:
        _seed_legacy_contratos(engine)
        extractor = PNCPExtractor(engine)

        original_raw_sql = engine.con.raw_sql

        def permanently_broken_rename(sql, *args, **kwargs):
            # Fail only on the RENAME so reads in _has_legacy_contratos_shape
            # still work and can correctly assert the post-condition.
            if isinstance(sql, str) and sql.startswith("ALTER TABLE main.contratos RENAME"):
                raise RuntimeError("simulated permanent rename failure")
            return original_raw_sql(sql, *args, **kwargs)

        engine.con.raw_sql = permanently_broken_rename  # type: ignore[assignment]

        with pytest.raises(RuntimeError, match="simulated permanent rename failure"):
            extractor._archive_legacy_contratos_table()
    finally:
        engine.con.disconnect()


@pytest.mark.filterwarnings("ignore")
def test_ingest_range_recovers_from_legacy_schema(tmp_path: Path, monkeypatch):
    """Full upgrade path: legacy table present, then ingest_range runs.

    Historical rows must survive under the archive; new rows land in
    the re-created snake_case contratos table.
    """
    monkeypatch.chdir(tmp_path)
    db = tmp_path / "upgrade.duckdb"
    engine = BalizaEngine(db)
    try:
        _seed_legacy_contratos(engine)

        raw_dir = tmp_path / "data" / "raw" / "2023-01"
        raw_dir.mkdir(parents=True)
        (raw_dir / "contratos_p1.json").write_text(
            json.dumps(
                {
                    "data": [
                        {
                            "numeroControlePNCP": "NEW-001",
                            "orgaoEntidade": {"cnpj": "00000000000002"},
                            "valorInicial": 1.0,
                        }
                    ]
                }
            )
        )

        extractor = PNCPExtractor(engine)
        stats = extractor.ingest_range(datetime(2023, 1, 1))

        assert stats["valid"] == 1
        columns = set(engine.con.table("contratos", database="main").schema().names)
        assert "numero_controle_pncp" in columns
        assert "numeroControlePNCP" not in columns

        archives = _legacy_archive_tables(engine)
        assert len(archives) == 1
        legacy_count = engine.con.raw_sql(
            f"SELECT COUNT(*) FROM main.{archives[0]}"
        ).fetchone()[0]
        assert legacy_count == 1, "legacy rows must not be lost across the upgrade"
    finally:
        engine.con.disconnect()
