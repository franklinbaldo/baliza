from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Tuple

import typer

from .pipelines.pncp import (
    BACKFILL_PIPELINE_NAME,
    DEFAULT_PIPELINE_NAME,
    default_config_path,
    run_pncp,
)

app = typer.Typer(help="Declarative PNCP pipeline runner")


def _resolve_config_path(config: Optional[Path]) -> Path:
    if config is None:
        return default_config_path()
    return config


def _month_windows(start_month: str, end_month: str) -> Iterable[Tuple[datetime, datetime]]:
    """Generate inclusive month windows between two YYYY-MM strings."""

    try:
        start = datetime.strptime(start_month, "%Y-%m").replace(tzinfo=timezone.utc, day=1)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter("start_month must follow YYYY-MM format") from exc

    try:
        end = datetime.strptime(end_month, "%Y-%m").replace(tzinfo=timezone.utc, day=1)
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


@app.command("extract")
def extract(
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the declarative source configuration",
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to the DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Destination dataset name inside DuckDB",
    ),
    lookback_days: int = typer.Option(
        3,
        "--lookback-days",
        "-l",
        min=0,
        help="Number of days to subtract from the last cursor when building the request window.",
    ),
) -> None:
    """Run the PNCP extraction pipeline once using dlt."""
    config_path = _resolve_config_path(config)
    _, run_info = run_pncp(
        config_path=config_path,
        dataset=dataset,
        duckdb_path=duckdb,
        lookback_days=lookback_days,
        pipeline_name=DEFAULT_PIPELINE_NAME,
    )
    typer.echo(json.dumps(run_info.asdict(), indent=2, default=str))


@app.command("backfill")
def backfill(
    start_month: str,
    end_month: str,
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the declarative source configuration",
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to the DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Destination dataset name inside DuckDB",
    ),
) -> None:
    """Run stateless monthly backfills between two months (inclusive)."""

    config_path = _resolve_config_path(config)
    windows = list(_month_windows(start_month, end_month))
    results = []

    for window_start, window_end in windows:
        _, run_info = run_pncp(
            config_path=config_path,
            dataset=dataset,
            duckdb_path=duckdb,
            lookback_days=0,
            range_start=window_start,
            range_end=window_end,
            pipeline_name=BACKFILL_PIPELINE_NAME,
        )
        results.append(
            {
                "start": window_start.isoformat(),
                "end": window_end.isoformat(),
                "run": run_info.asdict(),
            }
        )

    typer.echo(json.dumps({"windows": results}, indent=2, default=str))


if __name__ == "__main__":
    app()
