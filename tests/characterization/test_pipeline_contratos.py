import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from baliza.builder import _pending_build_months
from baliza.engine import BalizaEngine
from baliza.ia_uploader import IAUploader


# 1. builder.py uses hardcoded '%Y-%m' to parse partitions
def test_builder_parses_month_partition():
    # Setup mock manifest
    manifest = [
        {
            "data_particao": "2024-01",
            "raw_zip_url": "url1",
            "table_name": "contratos",
            "mirror_uploaded_at": "123",
            "parquet_uploaded_at": "",
        },
        {
            "data_particao": "2024-02",
            "raw_zip_url": "url2",
            "table_name": "contratos",
            "mirror_uploaded_at": "123",
            "parquet_uploaded_at": "",
        },
        {
            "data_particao": "invalid-format",
            "raw_zip_url": "url3",
            "table_name": "contratos",
            "mirror_uploaded_at": "123",
            "parquet_uploaded_at": "",
        },
    ]

    # We set start_date far back enough to include 2024
    start_date = date(2023, 1, 1)

    pending = _pending_build_months(
        start_date=start_date, batch_size=None, manifest=manifest, backfill=False
    )

    # Only the standard "YYYY-MM" parsed correctly, invalid-format skipped
    assert date(2024, 2, 1) in pending
    assert date(2024, 1, 1) in pending

    # Prove that the exact format string "%Y-%m" is used. If we pass "2024-01-01", it won't parse properly in the loop.
    # Actually datetime.strptime("2024-01-01", "%Y-%m") throws ValueError
    manifest.append(
        {
            "data_particao": "2024-03-01",
            "raw_zip_url": "url4",
            "table_name": "contratos",
            "mirror_uploaded_at": "123",
            "parquet_uploaded_at": "",
        }
    )

    pending_with_day = _pending_build_months(
        start_date=start_date, batch_size=None, manifest=manifest, backfill=False
    )
    # The list is the same, 2024-03-01 is skipped because of ValueError
    assert len(pending_with_day) == 2


# 2. engine.py upsert_rows deduplicates by PK. Drift-A made ``pk``
#    accept ``str | list[str]``: simple PKs keep the cheap ``isin`` path,
#    composite PKs go through an anti-join. Both shapes are pinned here.
def test_engine_upsert_single_pk():
    engine = BalizaEngine()
    data1 = [{"numeroControlePNCP": "A1", "valor": 10}, {"numeroControlePNCP": "A2", "valor": 20}]

    # Initial insert
    engine.upsert_rows(data1, "test_table", pk="numeroControlePNCP")

    # Read back
    res1 = engine.con.table("test_table").execute()
    assert len(res1) == 2

    # Upsert with duplicate PK "A1" - should overwrite or ignore, keeping row count same,
    # but new rows not in existing PK are added
    data2 = [{"numeroControlePNCP": "A1", "valor": 15}, {"numeroControlePNCP": "A3", "valor": 30}]
    engine.upsert_rows(data2, "test_table", pk="numeroControlePNCP")

    res2 = engine.con.table("test_table").execute()
    assert len(res2) == 3


def test_engine_upsert_composite_pk():
    """PCA uses ``(id_pca_pncp, numero_item)`` — verify the composite
    path dedups on the full key and not on either column alone."""
    engine = BalizaEngine()
    data1 = [
        {"id_pca": "P1", "item": 1, "valor": 10},
        {"id_pca": "P1", "item": 2, "valor": 20},
        {"id_pca": "P2", "item": 1, "valor": 30},
    ]
    engine.upsert_rows(data1, "pca_table", pk=["id_pca", "item"])
    assert len(engine.con.table("pca_table").execute()) == 3

    # Update P1/item=1 (collides), insert P2/item=2 (new). Critically,
    # P1/item=2 must survive — a single-column anti-join on id_pca alone
    # would have dropped it.
    data2 = [
        {"id_pca": "P1", "item": 1, "valor": 99},
        {"id_pca": "P2", "item": 2, "valor": 40},
    ]
    engine.upsert_rows(data2, "pca_table", pk=["id_pca", "item"])
    res = engine.con.table("pca_table").execute()
    assert len(res) == 4

    rows = {(r["id_pca"], r["item"]): r["valor"] for _, r in res.iterrows()}
    assert rows[("P1", 1)] == 99   # updated
    assert rows[("P1", 2)] == 20   # untouched — would die under single-PK
    assert rows[("P2", 1)] == 30
    assert rows[("P2", 2)] == 40


def test_engine_upsert_empty_composite_pk_rejected():
    engine = BalizaEngine()
    engine.upsert_rows([{"k": 1}], "tbl", pk="k")
    with pytest.raises(ValueError, match="pk list"):
        engine.upsert_rows([{"k": 2}], "tbl", pk=[])


# 3. ia_uploader.py hardcodes the written fields
@patch("baliza.ia_uploader.read_manifest_from_ia", return_value=[])
@patch("baliza.ia_uploader.write_manifest_to_ia")
def test_manifest_fields_are_fixed(mock_write, mock_read):
    engine = MagicMock()
    uploader = IAUploader(engine)

    start_dt = date(2024, 1, 1)
    # Call internal _update_remote_manifest with mock values
    uploader._update_remote_manifest(
        start_date=start_dt,
        item_id="item-id",
        uploaded_files={},  # empty dict, no parquet found
        access_key="fake",
        secret_key="fake",
        q_stats={"valid": 100, "quarantine": 2},
    )

    # verify write_manifest_to_ia was called with specific hardcoded fields
    assert mock_write.called
    written_manifest = mock_write.call_args[0][0]

    assert len(written_manifest) == 1
    row = written_manifest[0]

    expected_keys = {
        "data_particao",
        "table_name",
        "row_count",
        "quarantine_count",
        "ia_item_id",
        "raw_zip_url",
        "parquet_url",
        "quarantine_url",
        "uploaded_at",
        "file_type",
        "uf_sigla",
        "sort_key",
        "row_group_size",
        "bloom_filter_columns",
        "sha256",
        "file_size_bytes",
    }

    # Verify that all expected keys are in the row
    for k in expected_keys:
        assert k in row

    assert row["table_name"] == "contratos"  # hardcoded
    assert row["file_type"] == "monthly_canonical"  # hardcoded
    assert row["data_particao"] == "2024-01"


# 4. Frontend schema.ts exactly matches hardcoded string array + dynamic exposures
def test_frontend_archived_tables_includes_contratos():
    schema_path = Path("web/src/lib/archive/schema.ts")
    content = schema_path.read_text()

    # Use regex to find the ARCHIVED_TABLES array
    match = re.search(r"export const ARCHIVED_TABLES = \[([\s\S]*?)\] as const;", content)
    assert match is not None, "Could not find ARCHIVED_TABLES array in schema.ts"

    array_content = match.group(1)

    # We now expect dynamic mapping + static fallback tables
    assert "...FRONTEND_EXPOSURES.map((e) => e.table_alias)" in array_content
    assert "'orgaos'" in array_content
    assert "'unidades'" in array_content
    assert "'fornecedores'" in array_content


# 5. build-data.mjs uses real manifest loading (no hardcoded stub)
def test_build_data_stub_shape():
    build_data_path = Path("web/scripts/build-data.mjs")
    content = build_data_path.read_text()

    # Real manifest loading: BALIZA_MANIFEST_FIXTURE for CI fixture mode
    assert "BALIZA_MANIFEST_FIXTURE" in content
    assert "csv-parse/sync" in content  # switched from DuckDB read_csv_auto

    # httpfs loaded conditionally (not unconditionally installed)
    assert "LOAD httpfs" in content
    assert "INSTALL httpfs" in content

    # Old hardcoded stub is gone
    assert "SELECT '2024-04-01'" not in content
