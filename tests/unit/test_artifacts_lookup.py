"""Coverage for the ``baliza.artifacts`` lookup helpers (Drift-B wiring).

The helpers replace hardcoded ``file_type`` literals across the
runtime publish paths. Tests pin:

* lookup semantics (matched / missing / required) for the artifacts
  contratos and atas declare today,
* the ``resolve_export_params`` fallback chain (artifact override >
  CanonicalTableSpec > derived from sort + pk).
"""
from __future__ import annotations

import pytest

from baliza.artifacts import (
    get_artifact,
    require_artifact,
    resolve_export_params,
)
from baliza.resources import ATAS, CONTRATOS


def test_get_artifact_returns_none_for_missing():
    """Atas doesn't publish monthly_uf shards (partition_by_uf=False).
    Lookup must return None, not raise — IAConsolidator uses this as
    a feature gate."""
    assert get_artifact(ATAS, "monthly_uf") is None
    assert get_artifact(ATAS, "monthly_canonical") is not None


def test_get_artifact_finds_each_declared_artifact():
    """Contratos publishes monthly_canonical + monthly_uf +
    annual_canonical; the lookup hits all three."""
    for file_type in ("monthly_canonical", "monthly_uf", "annual_canonical"):
        artifact = get_artifact(CONTRATOS, file_type)
        assert artifact is not None
        assert artifact.file_type == file_type


def test_require_artifact_raises_on_missing():
    """Required-path callers must surface a misconfiguration immediately
    rather than silently emit a wrong manifest row."""
    with pytest.raises(LookupError, match="monthly_uf"):
        require_artifact(ATAS, "monthly_uf")


def test_resolve_export_params_inherits_from_canonical_table():
    """Today no artifact overrides sort/bloom/order_by_sql, so the
    helper returns canonical_tables[0]'s values verbatim. Pin this so
    a future override flows through without a regression on contratos."""
    sort, bloom, order_by = resolve_export_params(CONTRATOS, "monthly_canonical")
    table_spec = CONTRATOS.canonical_tables[0]
    assert sort == tuple(table_spec.sort_columns)
    assert bloom == tuple(table_spec.bloom_filter_columns)
    assert order_by == table_spec.order_by_sql  # contratos sets it explicitly


def test_resolve_export_params_atas_falls_back_to_derived_order_by():
    """Atas leaves order_by_sql blank on its canonical table; the
    helper derives ``sort_columns + pk`` exactly like the legacy
    MonthlyExporter did."""
    sort, _, order_by = resolve_export_params(ATAS, "monthly_canonical")
    table_spec = ATAS.canonical_tables[0]
    pk_cols = (table_spec.pk,) if isinstance(table_spec.pk, str) else tuple(table_spec.pk)
    assert order_by == ", ".join((*sort, *pk_cols))


def test_resolve_export_params_artifact_override_wins(monkeypatch):
    """If an ArtifactSpec overrides sort/bloom/order_by_sql, the helper
    prefers those over the canonical table defaults."""
    from baliza.resources.specs import ArtifactSpec
    overridden = ArtifactSpec(
        file_type="monthly_canonical",
        ia_item_id="baliza-pncp-{partition}",
        sort_columns=("override_col",),
        bloom_filter_columns=("override_bloom",),
        order_by_sql="override_order DESC",
    )
    # Build a fake resource with the override artifact in front of
    # the real one. PNCPResource coerces tuple/list at __post_init__.
    fake = type(CONTRATOS)(
        resource_name=CONTRATOS.resource_name,
        fetch=CONTRATOS.fetch,
        raw_dataset=CONTRATOS.raw_dataset,
        entities=CONTRATOS.entities,
        canonical_tables=CONTRATOS.canonical_tables,
        entity_model=CONTRATOS.entity_model,
        data_start=CONTRATOS.data_start,
        artifacts=(overridden,),
    )
    sort, bloom, order_by = resolve_export_params(fake, "monthly_canonical")
    assert sort == ("override_col",)
    assert bloom == ("override_bloom",)
    assert order_by == "override_order DESC"
