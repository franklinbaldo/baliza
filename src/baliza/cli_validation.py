"""CLI input validation and parsing utilities."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import typer

from .pipelines.pncp import default_config_path
from .utils.dates import to_pncp_window


def resolve_config_path(config: Path | None) -> Path:
    """Resolve configuration file path, using default if not provided."""
    if config is None:
        return default_config_path()
    return config


def month_windows(start_month: str, end_month: str) -> Iterable[tuple[datetime, datetime]]:
    """Generate inclusive month windows between two YYYY-MM strings."""

    try:
        start = datetime.strptime(start_month, "%Y-%m").replace(tzinfo=UTC, day=1)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter("start_month must follow YYYY-MM format") from exc

    try:
        end = datetime.strptime(end_month, "%Y-%m").replace(tzinfo=UTC, day=1)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter("end_month must follow YYYY-MM format") from exc

    if start > end:
        raise typer.BadParameter("start_month must be before or equal to end_month")

    current = start
    while current <= end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        yield current, next_month
        current = next_month


def parse_day(value: str | None, param_name: str) -> datetime | None:
    """Parse YYYY-MM-DD string to datetime, or return None."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=None)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter(f"{param_name} must follow YYYY-MM-DD format") from exc


def pncp_date_param(value: datetime | None) -> str | None:
    """Convert datetime to PNCP AAAAMMDD format, or return None."""
    if value is None:
        return None
    return to_pncp_window(value)


def filter_dict_none(values: dict[str, Any]) -> dict[str, Any]:
    """Filter out None values from dictionary."""
    return {k: v for k, v in values.items() if v is not None}


def parse_optional_date(value: str | None, *, option_name: str) -> date | None:
    """Parse optional ISO date string (YYYY-MM-DD) to date object."""
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter(f"{option_name} must follow YYYY-MM-DD format") from exc
