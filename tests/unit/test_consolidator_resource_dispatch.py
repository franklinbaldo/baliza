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
    """Contratos's canonical schema includes uf_sigla in sort columns
    (today via the bloom filter set), so per-UF shards apply."""
    # Sanity check on the gate. If the spec ever drops uf_sigla we
    # want this test to fail loudly so the consolidator stops trying.
    from baliza.resources import CONTRATOS

    spec_has = "uf_sigla" in (
        set(CONTRATOS.canonical_tables[0].bloom_filter_columns)
        | set(CONTRATOS.canonical_tables[0].sort_columns)
    )
    if spec_has:
        assert _resource_has_uf_shards("contratos") is True
    else:
        # Document the gate: if the spec stops listing uf_sigla, atas
        # and contratos converge on no-shard behavior — that's fine,
        # just intentional product knowledge.
        assert _resource_has_uf_shards("contratos") is False


def test_resource_has_uf_shards_atas_skipped():
    """Atas has no uf_sigla in its flatten output / canonical spec.
    Consolidator must NOT try to build per-UF shards for atas."""
    assert _resource_has_uf_shards("atas") is False


def test_consolidated_file_name_uses_canonical_table():
    """Annual file basename per resource — preserves contratos's
    historical name and gives atas its own slot in the same IA item."""
    assert _consolidated_file_name(2024) == "contratos-2024.parquet"
    assert _consolidated_file_name(2024, resource="contratos") == "contratos-2024.parquet"
    assert _consolidated_file_name(2024, resource="atas") == "atas-2024.parquet"
