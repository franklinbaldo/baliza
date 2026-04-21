"""BDD step definitions for consolidate.feature.

Seams:
- `IAConsolidator._get_daily_urls_for_year` → patched to return local
  parquet paths (DuckDB reads them without httpfs).
- `internetarchive.upload` → patched via `mock_ia_upload`.
- `internetarchive.get_item` → patched to control the "consolidated file
  already exists" branch at `consolidator.py:83-90`.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from pytest_bdd import given, parsers, scenario, then, when

from baliza.consolidator import IAConsolidator
from baliza.daily_exporter import DailyExporter


@scenario(
    "../features/consolidate.feature",
    "Consolidate produces an annual parquet from daily files",
)
def test_consolidate_happy_path():
    pass


@scenario(
    "../features/consolidate.feature",
    "Consolidate skips an already-consolidated frozen year",
)
def test_consolidate_skips_frozen():
    pass


# ── Given ────────────────────────────────────────────────────────────────────


@given("monthly parquet fixtures exist for 2023", target_fixture="daily_parquets")
def build_daily_parquets(make_db_with_contracts, tmp_path: Path) -> list[str]:
    """Materialise one daily parquet via DailyExporter and point the consolidator at it."""
    ctx = make_db_with_contracts("2023-01-15", count=10)
    output_dir = tmp_path / "daily"
    exporter = DailyExporter(ctx["db_path"], "baliza_raw")
    exporter.export(date(2023, 1, 15), output_dir)
    daily_path = output_dir / "2023-01-15" / "contratos.parquet"
    assert daily_path.exists()
    return [str(daily_path)]


@given("IA credentials are configured")
def set_ia_keys(monkeypatch):
    monkeypatch.setenv("IA_ACCESS_KEY", "fake-access-key")
    monkeypatch.setenv("IA_SECRET_KEY", "fake-secret-key")


@given("the consolidated 2023 file already exists on Internet Archive")
def stub_existing_consolidated(monkeypatch):
    stub_item = MagicMock()
    stub_item.files = [{"name": "contratos-2023.parquet"}]
    monkeypatch.setattr("baliza.consolidator.ia.get_item", lambda _item_id: stub_item)


# ── When ─────────────────────────────────────────────────────────────────────


@when(
    parsers.parse("I run IAConsolidator.consolidate_year for {year:d} with force=True"),
    target_fixture="consolidate_result",
)
def run_consolidate_force(
    monkeypatch,
    mock_ia_upload,
    daily_parquets,
    year: int,
):
    consolidator = IAConsolidator()
    monkeypatch.setattr(
        consolidator,
        "_get_daily_urls_for_year",
        lambda _year: daily_parquets,
    )
    # Ensure the frozen-check never short-circuits (scenario passes force=True anyway).
    monkeypatch.setattr("baliza.consolidator._is_frozen", lambda _year: False)
    ok = consolidator.consolidate_year(year, "fake-access", "fake-secret", force=True)
    return {"ok": ok, "uploads": mock_ia_upload}


@when(
    parsers.parse("I run IAConsolidator.consolidate_year for {year:d} without force"),
    target_fixture="consolidate_result",
)
def run_consolidate_noforce(
    monkeypatch,
    mock_ia_upload,
    year: int,
):
    consolidator = IAConsolidator()
    monkeypatch.setattr("baliza.consolidator._is_frozen", lambda _year: True)
    ok = consolidator.consolidate_year(year, "fake-access", "fake-secret", force=False)
    return {"ok": ok, "uploads": mock_ia_upload}


# ── Then ─────────────────────────────────────────────────────────────────────


@then("the consolidator should report success")
def check_consolidator_ok(consolidate_result):
    assert consolidate_result["ok"] is True


@then("the consolidator should report no-op")
def check_consolidator_noop(consolidate_result):
    assert consolidate_result["ok"] is False


@then(parsers.parse('the IA upload mock should have been called for item "{item_id}"'))
def check_upload_called(consolidate_result, item_id: str):
    matching = [u for u in consolidate_result["uploads"] if u["item_id"] == item_id]
    assert matching, (
        f"no upload for {item_id!r}; got {[u['item_id'] for u in consolidate_result['uploads']]}"
    )


@then("the IA upload mock should not have been called")
def check_no_upload(consolidate_result):
    assert consolidate_result["uploads"] == [], (
        f"expected zero uploads, got {[u['item_id'] for u in consolidate_result['uploads']]}"
    )


@then(parsers.parse('the uploaded files should include "{filename}"'))
def check_file_uploaded(consolidate_result, filename: str):
    all_files = []
    for u in consolidate_result["uploads"]:
        all_files.extend(u["files"].keys())
    assert filename in all_files, f"{filename!r} not in uploaded files: {all_files}"
