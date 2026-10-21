"""Exercise the real Ibis/DuckDB boundary without PNCP or archive access."""

from datetime import datetime
from pathlib import Path

from baliza.engine import BalizaEngine


def test_file_engine_reopens_without_losing_state(tmp_path: Path) -> None:
    """Schema discovery must work on both new and existing DuckDB files (#704)."""
    database = tmp_path / "state.duckdb"
    when = datetime(2026, 1, 1)
    with BalizaEngine(database) as engine:
        assert "quarantine" in engine.con.list_tables(database="baliza_state")
        engine.quarantine_record("contratos", when, "test error", {"id": "test"})
        engine.upsert_rows([{"numeroControlePNCP": "test", "valor": 10}], "contracts")

    with BalizaEngine(database) as engine:
        assert "contracts" in engine.con.list_tables(database="main")
        # Exercise Ibis overwrite SQL as well as schema discovery.
        engine.upsert_rows([{"numeroControlePNCP": "test", "valor": 20}], "contracts")
        contracts = engine.get_table("contracts").execute()
        assert len(contracts) == 1
        assert contracts.iloc[0]["valor"] == 20
        quarantine = engine.get_table("quarantine", schema="baliza_state").execute()
        assert len(quarantine) == 1
        assert quarantine.iloc[0]["error"] == "test error"
