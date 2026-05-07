"""End-to-end coverage of the Drift-D wiring in mirror/builder.

Drift-D made ``_pending_mirror_months`` and ``_pending_build_months``
iterate via ``iter_partitions`` and parse ``data_particao`` via
``parse_partition_label``. Existing characterization tests in
``tests/characterization/test_pipeline_contratos.py`` already pin the
monthly path against contratos. This module adds the annual path so
the migration is verified end-to-end before PCA is promoted.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from baliza.builder import _pending_build_months
from baliza.daily_exporter import SCHEMA_VERSION
from baliza.mirror import _pending_mirror_months
from baliza.resources import CONTRATOS as CONTRATOS_RESOURCE
from baliza.resources import PCA, PLANNED_RESOURCES, RESOURCES


def _annual_resource_fixture() -> None:
    """No-op: PCA is now an active resource (#569 promotion).

    Earlier versions of this test promoted PCA into RESOURCES for the
    duration of the test and popped it after — needed because PCA
    lived in PLANNED_RESOURCES. After the promotion the helpers are
    no-ops, kept so the test bodies read the same as the pre-promotion
    versions.
    """


def _annual_resource_unpromote() -> None:
    """No-op — see _annual_resource_fixture."""


def test_pending_mirror_months_walks_annual_partitions(monkeypatch):
    """For an annual resource, _pending_mirror_months should yield one
    Jan-1 date per missing year — not 12 month-starts per year."""
    _annual_resource_fixture()
    try:
        manifest = [
            # 2022 is mirrored, 2023 isn't; 2024 (current year, in
            # this test's date freeze) is always included.
            {"data_particao": "2022", "raw_zip_url": "ia://...", "table_name": "pca"},
        ]
        # Freeze "today" to mid-2024 so current_partition_start is 2024.
        with patch("baliza.partitioning.date") as mock_date:
            mock_date.today.return_value = date(2024, 7, 15)
            mock_date.side_effect = date
            with patch("baliza.mirror.read_manifest_from_ia", return_value=manifest):
                pending = _pending_mirror_months(
                    start_date=date(2022, 1, 1),
                    batch_size=None,
                    resource="pca",
                )
        # Newest-first: current year (2024), then 2023 (missing).
        # 2022 is mirrored → skipped.
        assert pending == [date(2024, 1, 1), date(2023, 1, 1)]
    finally:
        _annual_resource_unpromote()


def test_pending_build_months_parses_annual_labels(monkeypatch):
    """For an annual resource, builder should parse YYYY labels (not
    YYYY-MM) and only return partitions whose raw_zip_url is set and
    whose schema is missing/outdated under backfill."""
    _annual_resource_fixture()
    try:
        manifest = [
            # 2022 ZIP exists, no parquet → pending under backfill.
            {
                "data_particao": "2022",
                "raw_zip_url": "ia://...",
                "table_name": "pca",
                "mirror_uploaded_at": "2024-01-01",
                "parquet_uploaded_at": "",
                "parquet_schema_version": "",
            },
            # 2023 already built with current schema → skipped under backfill.
            {
                "data_particao": "2023",
                "raw_zip_url": "ia://...",
                "table_name": "pca",
                "mirror_uploaded_at": "2024-01-01",
                "parquet_uploaded_at": "2024-01-02",
                "parquet_schema_version": SCHEMA_VERSION,
            },
            # YYYY-MM row from a previous monthly schema must NOT be
            # treated as a valid annual partition. parse_partition_label
            # returns None for this shape; the row is skipped.
            {
                "data_particao": "2022-06",
                "raw_zip_url": "ia://...",
                "table_name": "pca",
                "parquet_schema_version": "",
            },
        ]
        with patch("baliza.partitioning.date") as mock_date:
            mock_date.today.return_value = date(2024, 7, 15)
            mock_date.side_effect = date
            pending = _pending_build_months(
                start_date=date(2022, 1, 1),
                batch_size=None,
                resource="pca",
                backfill=True,
                manifest=manifest,
            )
        assert pending == [date(2022, 1, 1)]
    finally:
        _annual_resource_unpromote()


def test_writer_reader_label_round_trip_for_annual():
    """Codex P1 on PR #570: IAUploader writers historically emitted
    labels via ``start_date.strftime("%Y-%m")``, which would never
    match the reader-side ``parse_partition_label`` for annual
    resources (expects YYYY). Pin that both sides agree by routing
    the writer through ``partition_label`` too — round-tripping any
    anchor date through the writer's label format and back through
    ``parse_partition_label`` must land on the same partition start.
    """
    from baliza.partitioning import parse_partition_label, partition_label
    for anchor in (date(2022, 1, 1), date(2024, 6, 15), date(2024, 12, 31)):
        label = partition_label(PCA, anchor)
        parsed = parse_partition_label(PCA, label)
        assert parsed == date(anchor.year, 1, 1), (
            f"annual round-trip broke for {anchor}: "
            f"label={label!r}, parsed={parsed}"
        )
    # Monthly stays at byte-identical "%Y-%m" semantics.
    for anchor in (date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)):
        label = partition_label(CONTRATOS_RESOURCE, anchor)
        parsed = parse_partition_label(CONTRATOS_RESOURCE, label)
        assert parsed == date(anchor.year, anchor.month, 1)


def test_planned_resources_partition_strategies_round_trip():
    """Sanity: every planned resource declares a partition_strategy that
    iter_partitions / partition_label / parse_partition_label all
    handle. A misconfigured planned resource would fail this."""
    from baliza.partitioning import (
        iter_partitions,
        parse_partition_label,
        partition_label,
    )
    anchor = date(2024, 6, 15)
    for resource in {**RESOURCES, **PLANNED_RESOURCES}.values():
        label = partition_label(resource, anchor)
        assert parse_partition_label(resource, label) is not None
        # iter_partitions on a single-day window yields one period.
        periods = list(iter_partitions(resource, anchor, anchor))
        assert len(periods) == 1


def test_pending_build_months_includes_current_year_for_annual_resource():
    """Annual resources must include the current-year partition in normal build mode.

    Codex P1 on PR #576: ``previous_partition_start`` for an ANNUAL resource
    returns ``date(year-1, 1, 1)``, which would exclude the current year's
    partition from ``_pending_build_months``. The builder now uses
    ``current_partition_start`` as the ceiling for annual resources so
    weekly PCA builds snapshot the current year rather than lagging a
    full calendar year.
    """
    _annual_resource_fixture()
    try:
        manifest = [
            # Current-year PCA: mirrored but not yet built → must be pending.
            {
                "data_particao": "2026",
                "raw_zip_url": "ia://...",
                "table_name": "pca",
                "mirror_uploaded_at": "2026-05-01T10:00:00",
                "parquet_uploaded_at": "",
                "parquet_schema_version": "",
            },
            # Past year already built → not pending.
            {
                "data_particao": "2025",
                "raw_zip_url": "ia://...",
                "table_name": "pca",
                "mirror_uploaded_at": "2026-01-01T10:00:00",
                "parquet_uploaded_at": "2026-01-02T10:00:00",
                "parquet_schema_version": SCHEMA_VERSION,
            },
        ]
        with patch("baliza.partitioning.date") as mock_date:
            mock_date.today.return_value = date(2026, 5, 7)
            mock_date.side_effect = date
            pending = _pending_build_months(
                start_date=date(2024, 1, 1),
                batch_size=None,
                resource="pca",
                backfill=False,
                manifest=manifest,
            )
        # 2026 is the current year — it must appear in the pending list.
        assert date(2026, 1, 1) in pending, (
            "current-year PCA partition was not included in _pending_build_months; "
            "check the PartitionStrategy.ANNUAL ceiling in builder.py"
        )
        # 2025 is already built — must not re-appear.
        assert date(2025, 1, 1) not in pending
    finally:
        _annual_resource_unpromote()
