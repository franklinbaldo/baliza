from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any


def _coerce_datetime(value: str) -> datetime:
    candidates = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Unsupported datetime format: {value}") from exc


def to_pncp_window(value: Any) -> str:
    """Convert incremental cursor values into PNCP date window parameters."""
    if value is None:
        dt = datetime.now(UTC)
    elif isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    elif isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            dt = datetime.now(UTC)
        elif len(cleaned) == 8 and cleaned.isdigit():
            return cleaned
        else:
            normalized = cleaned.replace("Z", "+00:00")
            dt = _coerce_datetime(normalized)
    else:
        return str(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)

    return dt.strftime("%Y%m%d")
