from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from baliza.utils.dates import humanize_naturaltime, to_pncp_window


def test_to_pncp_window_from_datetime_with_timezone() -> None:
    value = datetime(2024, 2, 10, 15, 30, tzinfo=timezone.utc)
    assert to_pncp_window(value) == "20240210"


def test_to_pncp_window_from_naive_datetime() -> None:
    value = datetime(2024, 7, 1, 12, 0)  # naive
    assert to_pncp_window(value) == "20240701"


def test_to_pncp_window_from_date() -> None:
    value = date(2023, 12, 31)
    assert to_pncp_window(value) == "20231231"


def test_to_pncp_window_keeps_compact_strings() -> None:
    assert to_pncp_window("20240115") == "20240115"


def test_to_pncp_window_parses_iso_strings() -> None:
    assert to_pncp_window("2024-05-03T08:45:00Z") == "20240503"


def test_to_pncp_window_rejects_invalid_format() -> None:
    with pytest.raises(ValueError):
        to_pncp_window("31/01/2024")

def test_humanize_naturaltime_just_now() -> None:
    now = datetime.now(timezone.utc)
    dt = now - timedelta(seconds=30)
    assert humanize_naturaltime(dt) == "just now"


def test_humanize_naturaltime_minutes_ago() -> None:
    now = datetime.now(timezone.utc)
    dt = now - timedelta(minutes=5)
    assert humanize_naturaltime(dt) == "5m ago"


def test_humanize_naturaltime_hours_ago() -> None:
    now = datetime.now(timezone.utc)
    dt = now - timedelta(hours=3)
    assert humanize_naturaltime(dt) == "3h ago"


def test_humanize_naturaltime_days_ago() -> None:
    now = datetime.now(timezone.utc)
    dt = now - timedelta(days=2)
    assert humanize_naturaltime(dt) == "2d ago"


def test_humanize_naturaltime_fallback() -> None:
    now = datetime.now(timezone.utc)
    dt = now - timedelta(days=8)
    assert humanize_naturaltime(dt) == dt.strftime("%Y-%m-%d %H:%M")


def test_humanize_naturaltime_future_date() -> None:
    now = datetime.now(timezone.utc)
    dt = now + timedelta(hours=1)
    assert humanize_naturaltime(dt) == dt.strftime("%Y-%m-%d %H:%M")
