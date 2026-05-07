"""Pins the ArtifactSpec shape introduced in Drift-B (prep).

The runtime paths (uploader, consolidator, exporter) still hardcode
artifact-axis facts today; this test fixes the declarative target
shape so the wiring PR can be a behavior-preserving substitution
rather than a re-design.
"""
from __future__ import annotations

from baliza.resources import ATAS, CONTRATOS, ArtifactSpec


def _file_types(resource) -> list[str]:
    return [a.file_type for a in resource.artifacts]


def test_contratos_publishes_three_artifacts():
    """Monthly canonical + per-UF shards (gated by partition_by_uf=True)
    + annual rollup. Drops or additions here are runtime-visible
    changes — bump the test deliberately."""
    assert _file_types(CONTRATOS) == [
        "monthly_canonical",
        "monthly_uf",
        "annual_canonical",
    ]


def test_atas_publishes_two_artifacts():
    """No monthly_uf — atas canonical has partition_by_uf=False (the
    PNCP atas payload doesn't carry buyer UF)."""
    assert _file_types(ATAS) == ["monthly_canonical", "annual_canonical"]


def test_atas_artifact_set_matches_canonical_partition_by_uf():
    """Cross-check: a resource that says ``partition_by_uf=False`` on
    its canonical table must NOT advertise a monthly_uf artifact, and
    vice versa. Catches drift between the two declarations."""
    for resource in (CONTRATOS, ATAS):
        canonical = resource.canonical_tables[0]
        has_uf_artifact = "monthly_uf" in _file_types(resource)
        assert has_uf_artifact == canonical.partition_by_uf, (
            f"{resource.resource_name}: partition_by_uf="
            f"{canonical.partition_by_uf} but monthly_uf in artifacts="
            f"{has_uf_artifact}"
        )


def test_artifact_spec_is_frozen():
    """Mutability would let a runtime caller poison the registry for
    every subsequent caller in the same process. Pin frozen=True."""
    spec = ArtifactSpec(file_type="x", ia_item_id="y")
    try:
        spec.file_type = "z"  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("ArtifactSpec must be frozen")


def test_artifact_spec_collection_fields_are_immutable():
    """Codex P2: ``frozen=True`` only blocks attribute reassignment.
    A ``list`` field would still let a caller append/mutate the
    registry-owned object. Tuples enforce real immutability."""
    spec = ArtifactSpec(file_type="x", ia_item_id="y")
    assert isinstance(spec.sort_columns, tuple)
    assert isinstance(spec.bloom_filter_columns, tuple)
    # And declared instances on real resources too.
    for resource in (CONTRATOS, ATAS):
        for artifact in resource.artifacts:
            assert isinstance(artifact.sort_columns, tuple)
            assert isinstance(artifact.bloom_filter_columns, tuple)


def test_every_artifact_has_a_known_file_type():
    """Whitelist of file_type strings the runtime knows how to handle.
    Adding a new value here is a deliberate cross-cutting change
    (manifest schema, isCanonicalRow, consolidator branch)."""
    known = {"monthly_canonical", "monthly_uf", "annual_canonical"}
    for resource in (CONTRATOS, ATAS):
        for artifact in resource.artifacts:
            assert artifact.file_type in known, (
                f"unknown file_type {artifact.file_type!r} in "
                f"{resource.resource_name}.artifacts"
            )
