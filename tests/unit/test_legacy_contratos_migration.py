"""Verify upgrade path from the pre-flatten camelCase `main.contratos`
schema to the current snake_case shape.

Older BALIZA versions wrote the raw Pydantic dump (camelCase keys, nested
structs) straight to `main.contratos`. The post-flatten path upserts
snake_case scalars with PK `numero_controle_pncp`, which raises
IbisTypeError against a legacy table. `_drop_legacy_contratos_table`
detects that case and drops the stale table so the next upsert recreates
it in the new shape.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from baliza.engine import BalizaEngine
from baliza.extractor import PNCPExtractor


def _seed_legacy_contratos(engine: BalizaEngine) -> None:
    engine.con.raw_sql(
        """
        CREATE TABLE main.contratos (
            "numeroControlePNCP" VARCHAR PRIMARY KEY,
            "orgaoEntidade" STRUCT(cnpj VARCHAR, "razaoSocial" VARCHAR),
            "valorInicial" DOUBLE
        )
        """
    )
    engine.con.raw_sql(
        """
        INSERT INTO main.contratos VALUES (
            'LEGACY-001', {'cnpj': '00000000000001', 'razaoSocial': 'Legacy Org'}, 999.0
        )
        """
    )


def test_drop_legacy_contratos_removes_camelcase_table(tmp_path: Path):
    db = tmp_path / "legacy.duckdb"
    engine = BalizaEngine(db)
    try:
        _seed_legacy_contratos(engine)
        assert "contratos" in engine.con.list_tables(database="main")

        extractor = PNCPExtractor(engine)
        extractor._drop_legacy_contratos_table()

        assert "contratos" not in engine.con.list_tables(database="main")
    finally:
        engine.con.disconnect()


def test_drop_legacy_is_noop_for_snake_case_table(tmp_path: Path):
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
        extractor._drop_legacy_contratos_table()

        # Table + row survive.
        assert "contratos" in engine.con.list_tables(database="main")
        count = engine.con.raw_sql("SELECT COUNT(*) FROM main.contratos").fetchone()[0]
        assert count == 1
    finally:
        engine.con.disconnect()


def test_drop_legacy_is_noop_when_table_missing(tmp_path: Path):
    db = tmp_path / "empty.duckdb"
    engine = BalizaEngine(db)
    try:
        extractor = PNCPExtractor(engine)
        # Must not raise when the table doesn't exist yet.
        extractor._drop_legacy_contratos_table()
        assert "contratos" not in engine.con.list_tables(database="main")
    finally:
        engine.con.disconnect()


@pytest.mark.filterwarnings("ignore")
def test_ingest_range_recovers_from_legacy_schema(tmp_path: Path, monkeypatch):
    """Full upgrade path: legacy table present, then ingest_range runs."""
    import json

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
    finally:
        engine.con.disconnect()
