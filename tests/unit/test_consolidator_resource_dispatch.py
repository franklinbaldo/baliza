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

import datetime as _dt

from baliza.consolidator import (
    _consolidated_file_name,
    _parse_iso_mtime,
    _resource_has_uf_shards,
)


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


# --------------------------------------------------------------------------
# Codex P1/P2 hotfix coverage — comparison guarantees in the non-UF
# current-year freshness gate. See PR #555 review threads
# r3198163232 (timezone TypeError) and r3198163240 (max([None]) TypeError).
# --------------------------------------------------------------------------


def test_parse_iso_mtime_normalizes_naive_to_utc():
    """Manifest writers historically used datetime.now().isoformat() (naive).
    Comparing those against IA's UTC-aware mtimes would TypeError. The
    parser must promote naive datetimes to UTC."""
    parsed = _parse_iso_mtime("2024-04-01T12:34:56")
    assert parsed is not None
    assert parsed.tzinfo is not None, "naive timestamp must be promoted to UTC-aware"
    assert parsed.tzinfo.utcoffset(parsed) == _dt.timedelta(0)


def test_parse_iso_mtime_preserves_aware_offset():
    """Aware timestamps round-trip with their original offset."""
    parsed = _parse_iso_mtime("2024-04-01T12:34:56+00:00")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.tzinfo.utcoffset(parsed) == _dt.timedelta(0)


def test_parse_iso_mtime_does_not_corrupt_non_utc_offset():
    """Pins the safety property Kilo's CRITICAL on PR #557 worried about:
    for an aware timestamp with a non-UTC offset, the parser MUST
    preserve the absolute moment — not silently relabel it as UTC while
    keeping the wall-clock time. The function only mutates tzinfo when
    the input was naive (the ``if parsed.tzinfo is None`` gate)."""
    parsed = _parse_iso_mtime("2024-04-01T12:34:56+02:00")
    assert parsed is not None
    assert (parsed.year, parsed.month, parsed.day) == (2024, 4, 1)
    assert (parsed.hour, parsed.minute, parsed.second) == (12, 34, 56)
    # Crucially, the absolute moment is +02:00 — must NOT have been
    # silently relabelled as UTC (which would shift the moment by 2h).
    assert parsed.utcoffset() == _dt.timedelta(hours=2)
    # Convert to UTC and check the absolute instant matches what +02:00
    # implies (12:34:56 +02:00 == 10:34:56 UTC).
    in_utc = parsed.astimezone(_dt.UTC)
    assert (in_utc.hour, in_utc.minute) == (10, 34)


def test_parse_iso_mtime_returns_none_on_garbage():
    assert _parse_iso_mtime("") is None
    assert _parse_iso_mtime("not-a-date") is None
    assert _parse_iso_mtime("2024-13-01") is None


def test_consolidated_mtime_compare_against_naive_manifest_succeeds():
    """End-to-end smoke for the non-UF freshness gate: an aware mtime
    from the IA item must compare safely against parsed naive manifest
    timestamps. Reproduces the TypeError from PR #555 r3198163232."""
    ia_mtime = _dt.datetime(2024, 5, 1, tzinfo=_dt.UTC)
    parsed = _parse_iso_mtime("2024-04-30T10:00:00")  # naive in the manifest
    assert parsed is not None
    # The actual comparison the consolidator does — must not TypeError.
    assert ia_mtime >= parsed
