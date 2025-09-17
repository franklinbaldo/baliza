from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from baliza.utils.dates import to_pncp_window


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
