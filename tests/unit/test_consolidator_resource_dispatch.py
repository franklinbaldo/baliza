"""Per-resource consolidator behavior tests.

Locks two invariants the multi-resource consolidator depends on:

  * Resources whose canonical schema lacks ``uf_sigla`` (atas) skip
    the per-UF shard step entirely — building shards on a UF-less
    file would fail at the ``SELECT DISTINCT uf_sigla`` query, and
    creating an empty manifest entry would mislead the web resolver.
  * Annual consolidated filenames are derived from the resource's
    canonical-table name, not hardcoded to ``contratos-{year}.parquet``.
"""

from __future__ import annotations

from baliza.consolidator import _consolidated_file_name, _resource_has_uf_shards


def test_resource_has_uf_shards_contratos():
    """Contratos's canonical row has uf_sigla. The spec must declare
    partition_by_uf=True so consolidate_year keeps emitting monthly_uf
    shards (the public Journey 6 contract relies on those URLs)."""
    from baliza.resources import CONTRATOS

    assert CONTRATOS.canonical_tables[0].partition_by_uf is True
    assert _resource_has_uf_shards("contratos") is True


def test_resource_has_uf_shards_atas_skipped():
    """Atas has no uf_sigla — partition_by_uf must stay False so the
    consolidator skips _build_per_uf_shards (which would crash on the
    SELECT DISTINCT uf_sigla query) and the manifest stays free of
    bogus monthly_uf rows."""
    from baliza.resources import ATAS

    assert ATAS.canonical_tables[0].partition_by_uf is False
    assert _resource_has_uf_shards("atas") is False


def test_consolidated_file_name_uses_canonical_table():
    """Annual file basename per resource — preserves contratos's
    historical name and gives atas its own slot in the same IA item."""
    assert _consolidated_file_name(2024) == "contratos-2024.parquet"
    assert _consolidated_file_name(2024, resource="contratos") == "contratos-2024.parquet"
    assert _consolidated_file_name(2024, resource="atas") == "atas-2024.parquet"
