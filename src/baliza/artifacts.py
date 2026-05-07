"""Lookup helpers for ``PNCPResource.artifacts`` (Drift-B wiring).

Drift-B prep added the typed ``ArtifactSpec`` dataclass and populated
``PNCPResource.artifacts`` for contratos/atas. The runtime publish
paths (``IAUploader``, ``MonthlyExporter``, ``IAConsolidator``)
historically hardcoded the same facts (``file_type``, ``ia_item_id``,
sort/bloom columns, ORDER BY) at every call site.

This module is the typed entry point those callers consume so the
spec becomes the source of truth: an override on
``CONTRATOS.artifacts[0].order_by_sql`` actually changes what the
exporter writes, and adding a new artifact (e.g. PCA's
``annual_canonical`` rollup) is a single resource-module edit
instead of patches across three runtime files.

For contratos / atas the lookups produce identical values to the
previous hardcoded literals — this PR is intentionally
behavior-preserving for the registered resources, with the migration
proven by the existing characterization test suite passing unchanged.
"""

from __future__ import annotations

from baliza.resources.specs import (
    ArtifactSpec,
    CanonicalTableSpec,
    PNCPResource,
)


def get_artifact(resource: PNCPResource, file_type: str) -> ArtifactSpec | None:
    """The artifact with the matching ``file_type``, or ``None``.

    ``None`` is the right answer for "this resource doesn't publish a
    ``monthly_uf`` shard" rather than raising — callers like
    ``IAConsolidator._build_per_uf_shards`` use this as a feature gate
    that's strictly per-resource (atas opts out by leaving the
    artifact off its registry entry).
    """
    for artifact in resource.artifacts:
        if artifact.file_type == file_type:
            return artifact
    return None


def require_artifact(resource: PNCPResource, file_type: str) -> ArtifactSpec:
    """Like ``get_artifact`` but raises when missing.

    Use for paths that *must* find the artifact (the canonical export
    on a registered resource — every resource in ``RESOURCES`` carries
    ``monthly_canonical``). Mismatch surfaces immediately instead of
    silently emitting an empty manifest row.
    """
    artifact = get_artifact(resource, file_type)
    if artifact is None:
        raise LookupError(
            f"resource {resource.resource_name!r} declares no artifact "
            f"with file_type={file_type!r}; "
            f"available: {[a.file_type for a in resource.artifacts]}"
        )
    return artifact


def resolve_export_params(
    resource: PNCPResource,
    file_type: str,
    table_spec: CanonicalTableSpec | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    """Return ``(sort_columns, bloom_filter_columns, order_by_sql)``
    for an artifact, layering its overrides on top of the resource's
    canonical table defaults.

    ``ArtifactSpec`` ships empty defaults for the column tuples and
    ``None`` for ``order_by_sql`` precisely so contratos/atas can keep
    their declarations terse while leaving the door open for per-
    artifact tweaks (e.g. an annual rollup sorted by year then UF).
    The fallback chain is:

      artifact.<field> if set
      → ``CanonicalTableSpec.<field>``
      → derived from sort_columns + pk for ``order_by_sql``.

    ``table_spec`` defaults to ``resource.canonical_tables[0]`` because
    every ``PNCPResource`` registered today has exactly one canonical
    table; callers with a derived table can still pass it explicitly.
    """
    artifact = require_artifact(resource, file_type)
    if table_spec is None:
        table_spec = resource.canonical_tables[0]

    sort_columns = artifact.sort_columns or tuple(table_spec.sort_columns)
    bloom_filter_columns = (
        artifact.bloom_filter_columns or tuple(table_spec.bloom_filter_columns)
    )

    if artifact.order_by_sql:
        order_by_sql = artifact.order_by_sql
    elif table_spec.order_by_sql:
        order_by_sql = table_spec.order_by_sql
    else:
        pk_cols = (
            (table_spec.pk,) if isinstance(table_spec.pk, str) else tuple(table_spec.pk)
        )
        order_by_sql = ", ".join((*sort_columns, *pk_cols))

    return sort_columns, bloom_filter_columns, order_by_sql
