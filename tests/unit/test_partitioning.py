"""Unit coverage for ``iter_partitions`` / ``partition_label`` (Drift-D).

Pins the contract so PCA promotion (annual) can wire through without
re-deriving the iteration shape; also pins the existing monthly shape
so a refactor of mirror/builder to consume these helpers is a no-op
for contratos / atas.
"""
from __future__ import annotations

from datetime import date

import pytest

from baliza.partitioning import (
    clamp_to_data_start,
    current_partition_start,
    iter_partitions,
    parse_partition_label,
    partition_for,
    partition_label,
    previous_partition_start,
)
from baliza.resources import CONTRATOS, PCA, PLANNED_RESOURCES, RESOURCES


def test_monthly_yields_one_period_per_calendar_month():
    periods = list(iter_partitions(CONTRATOS, date(2024, 1, 15), date(2024, 3, 5)))
    assert [p.label for p in periods] == ["2024-01", "2024-02", "2024-03"]
    assert periods[0].start == date(2024, 1, 1)
    assert periods[0].end == date(2024, 1, 31)
    assert periods[1].end == date(2024, 2, 29)  # leap year — pinning
    assert periods[2].end == date(2024, 3, 31)


def test_monthly_crosses_year_boundary():
    periods = list(iter_partitions(CONTRATOS, date(2023, 11, 1), date(2024, 2, 1)))
    assert [p.label for p in periods] == [
        "2023-11", "2023-12", "2024-01", "2024-02",
    ]


def test_monthly_single_month():
    periods = list(iter_partitions(CONTRATOS, date(2024, 6, 10), date(2024, 6, 20)))
    assert len(periods) == 1
    assert periods[0].label == "2024-06"


def test_annual_yields_one_period_per_calendar_year():
    periods = list(iter_partitions(PCA, date(2023, 5, 1), date(2025, 8, 15)))
    assert [p.label for p in periods] == ["2023", "2024", "2025"]
    assert periods[0].start == date(2023, 1, 1)
    assert periods[0].end == date(2023, 12, 31)


def test_annual_single_year():
    periods = list(iter_partitions(PCA, date(2024, 6, 10), date(2024, 6, 20)))
    assert len(periods) == 1
    assert periods[0].label == "2024"
    assert periods[0].start == date(2024, 1, 1)
    assert periods[0].end == date(2024, 12, 31)


def test_partition_label_monthly_and_annual():
    assert partition_label(CONTRATOS, date(2024, 7, 15)) == "2024-07"
    assert partition_label(PCA, date(2024, 7, 15)) == "2024"


def test_reversed_range_raises():
    """Codex P2: a swapped range silently yielded one period inside the
    same month/year because both branches normalize boundaries. Surface
    the caller bug instead."""
    with pytest.raises(ValueError, match="after end"):
        list(iter_partitions(CONTRATOS, date(2024, 6, 20), date(2024, 6, 10)))
    with pytest.raises(ValueError, match="after end"):
        list(iter_partitions(PCA, date(2025, 1, 1), date(2024, 12, 31)))


def test_unsupported_strategy_raises():
    bogus = type(CONTRATOS.raw_dataset)(
        ia_item_id=CONTRATOS.raw_dataset.ia_item_id,
        filename_fn=CONTRATOS.raw_dataset.filename_fn,
        partition_strategy="weekly",
        retention_policy=CONTRATOS.raw_dataset.retention_policy,
    )
    fake = type(CONTRATOS)(
        resource_name=CONTRATOS.resource_name,
        fetch=CONTRATOS.fetch,
        raw_dataset=bogus,
        entities=CONTRATOS.entities,
        canonical_tables=CONTRATOS.canonical_tables,
        entity_model=CONTRATOS.entity_model,
        data_start=CONTRATOS.data_start,
    )
    with pytest.raises(ValueError, match="weekly"):
        list(iter_partitions(fake, date(2024, 1, 1), date(2024, 1, 31)))
    with pytest.raises(ValueError, match="weekly"):
        partition_label(fake, date(2024, 1, 1))


def test_parse_partition_label_round_trips_through_partition_label():
    """``parse_partition_label`` is the inverse of ``partition_label``."""
    anchor = date(2024, 7, 15)
    assert parse_partition_label(CONTRATOS, partition_label(CONTRATOS, anchor)) == date(2024, 7, 1)
    assert parse_partition_label(PCA, partition_label(PCA, anchor)) == date(2024, 1, 1)


def test_parse_partition_label_rejects_wrong_shape():
    """A YYYY-MM label on an annual resource (or vice versa) returns
    None so callers can skip rather than misinterpret the row."""
    assert parse_partition_label(PCA, "2024-07") is None
    assert parse_partition_label(CONTRATOS, "2024") is None
    assert parse_partition_label(CONTRATOS, "garbage") is None
    assert parse_partition_label(PCA, "garbage") is None


def test_current_and_previous_partition_start():
    anchor = date(2024, 7, 15)
    assert current_partition_start(CONTRATOS, anchor) == date(2024, 7, 1)
    assert previous_partition_start(CONTRATOS, anchor) == date(2024, 6, 1)
    assert current_partition_start(PCA, anchor) == date(2024, 1, 1)
    assert previous_partition_start(PCA, anchor) == date(2023, 1, 1)
    # Year-boundary: previous of January monthly = December of previous year.
    assert previous_partition_start(CONTRATOS, date(2024, 1, 10)) == date(2023, 12, 1)


def test_clamp_to_data_start_respects_resource_floor():
    # CONTRATOS data_start = 2021-09-06: pre-floor request clamps to
    # the floor's partition start; post-floor request normalizes to
    # its own partition start.
    assert clamp_to_data_start(CONTRATOS, date(2020, 1, 15)) == date(2021, 9, 1)
    assert clamp_to_data_start(CONTRATOS, date(2024, 7, 15)) == date(2024, 7, 1)
    # PCA's data_start is None → no floor; request normalizes to
    # partition start (annual = Jan 1).
    assert clamp_to_data_start(PCA, date(2024, 7, 15)) == date(2024, 1, 1)


def test_partition_for_returns_period_containing_anchor():
    monthly = partition_for(CONTRATOS, date(2024, 2, 17))
    assert (monthly.start, monthly.end, monthly.label) == (
        date(2024, 2, 1), date(2024, 2, 29), "2024-02",
    )
    annual = partition_for(PCA, date(2024, 2, 17))
    assert (annual.start, annual.end, annual.label) == (
        date(2024, 1, 1), date(2024, 12, 31), "2024",
    )


@pytest.mark.parametrize(
    "resource", list({**RESOURCES, **PLANNED_RESOURCES}.values()),
    ids=lambda r: r.resource_name,
)
def test_every_declared_resource_has_a_supported_strategy(resource):
    """Smoke test: every resource (registered or planned) must round-trip
    through the helper. Catches future ``partition_strategy`` typos at
    import + test time instead of at the first PCA-style promotion."""
    anchor = date(2024, 6, 15)
    label = partition_label(resource, anchor)
    assert label  # non-empty
    periods = list(iter_partitions(resource, anchor, anchor))
    assert len(periods) == 1
    assert periods[0].label == label
