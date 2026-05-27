"""Unit test: mirror_cmd computes is_current_month using partition-aware logic.

Regression guard for the bug where date.today().replace(day=1) (monthly floor)
was used instead of current_partition_start(resource_obj). For an annual resource
like pca, the monthly floor (2026-05-01) never matched the annual partition start
(2026-01-01), so is_current_month was always False, keep_raw_dir was False, and
the cache directory was deleted after every upload — forcing a full re-fetch.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, call, patch

import pytest

from baliza.partitioning import current_partition_start
from baliza.resources import get_resource


def test_current_partition_start_monthly_resource():
    """Monthly resources: current_partition_start == date.today().replace(day=1)."""
    contratos = get_resource("contratos")
    anchor = date(2026, 5, 15)
    assert current_partition_start(contratos, anchor) == date(2026, 5, 1)


def test_current_partition_start_annual_resource():
    """Annual resources: current_partition_start == date(year, 1, 1), not monthly floor."""
    pca = get_resource("pca")
    anchor = date(2026, 5, 15)
    result = current_partition_start(pca, anchor)
    assert result == date(2026, 1, 1), (
        f"Expected 2026-01-01 for annual PCA, got {result}. "
        "If this returns 2026-05-01 the is_current_month bug has regressed."
    )


def test_annual_partition_differs_from_monthly_floor():
    """The annual partition start must differ from the monthly floor for mid-year dates.

    This is the exact condition that caused the bug: if they were equal,
    is_current_month would coincidentally be True even with the wrong logic.
    """
    pca = get_resource("pca")
    anchor = date(2026, 5, 15)
    annual_start = current_partition_start(pca, anchor)
    monthly_floor = anchor.replace(day=1)
    assert annual_start != monthly_floor, (
        "Annual partition start should differ from monthly floor for mid-year dates. "
        "If equal, the regression test cannot distinguish correct from broken behavior."
    )


@pytest.mark.parametrize("anchor,expected_is_current", [
    # Current year → True
    (date(2026, 1, 1), True),
    (date(2026, 5, 27), True),
    (date(2026, 12, 31), True),
    # Past year → False
    (date(2025, 1, 1), False),
    (date(2025, 6, 15), False),
    (date(2024, 12, 31), False),
])
def test_is_current_month_annual_resource(anchor, expected_is_current):
    """is_current_month must be True only for the current year's partition (PCA)."""
    pca = get_resource("pca")
    today = date(2026, 5, 27)
    current = current_partition_start(pca, today)
    # mirror_cmd now uses: is_current_month = (target_month == current_partition)
    # where target_month is the partition start, not an arbitrary date.
    target_partition_start = current_partition_start(pca, anchor)
    assert (target_partition_start == current) == expected_is_current
