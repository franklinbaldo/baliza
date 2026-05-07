"""Unit coverage for the partition_period helper introduced in Drift-D.

Pins the contract so PCA promotion (annual) can wire through without
re-deriving the iteration shape; also pins the existing monthly shape
so a refactor of mirror/builder to consume this helper is a no-op for
contratos / atas.
"""
from __future__ import annotations

from datetime import date

import pytest

from baliza.partitioning import iter_partitions, partition_label
from baliza.resources import CONTRATOS, PCA


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
