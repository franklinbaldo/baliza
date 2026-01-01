from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any


def _coerce_datetime(value: str) -> datetime:
    # Try the fastest ISO parsing first (Python 3.11+ optimized path)
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass

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

    raise ValueError(f"Unsupported datetime format: {value}")


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
            # Optimization: Python 3.11+ handles 'Z' in fromisoformat natively.
            dt = _coerce_datetime(cleaned)
    else:
        return str(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)

    # Optimization: f-string is ~2.15x faster than strftime("%Y%m%d")
    return f"{dt.year:04d}{dt.month:02d}{dt.day:02d}"


def humanize_duration(seconds: float) -> str:
    """Convert seconds to a human-readable duration string (e.g., '1h 30m')."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"

    minutes, seconds_rem = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds_rem > 0 and days == 0:
        parts.append(f"{seconds_rem}s")

    # Return at most 2 significant parts for compactness
    return " ".join(parts[:2])


def humanize_naturaltime(dt: datetime) -> str:
    """
    Convert a datetime to a human-readable relative time string (e.g., '2h ago').
    Falls back to YYYY-MM-DD HH:MM for dates older than 7 days.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    diff = now - dt
    seconds = diff.total_seconds()

    if seconds < 0:
        return dt.strftime("%Y-%m-%d %H:%M")

    if seconds < 60:
        return "just now"

    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes}m ago"

    hours = int(minutes // 60)
    if hours < 24:
        return f"{hours}h ago"

    days = int(hours // 24)
    if days < 7:
        return f"{days}d ago"

    return dt.strftime("%Y-%m-%d %H:%M")
