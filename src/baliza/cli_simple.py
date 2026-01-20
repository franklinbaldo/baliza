"""Simplified CLI using direct extraction (no dlt)."""

from __future__ import annotations

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import typer
from rich.console import Console

from .extractor import PNCPExtractor

app = typer.Typer(help="Baliza - Simple PNCP extraction tool")
console = Console()


@app.command("extract")
def extract(
    start: Optional[str] = typer.Option(
        None,
        "--start",
        help="Start date (YYYY-MM-DD). Defaults to lookback period.",
    ),
    end: Optional[str] = typer.Option(
        None,
        "--end",
        help="End date (YYYY-MM-DD). Defaults to today.",
    ),
    lookback_days: int = typer.Option(
        30,
        "--lookback-days",
        help="Number of days to look back for missing data if start/end are not provided.",
    ),
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name in DuckDB",
    ),
    resource: str = typer.Option(
        "contratos",
        "--resource",
        "-r",
        help="Resource to extract (contratos, etc.)",
    ),
) -> None:
    """Extract data from PNCP API to DuckDB using a stateful, resumable pipeline."""
    # Determine date range
    if end:
        end_date = datetime.strptime(end, "%Y-%m-%d")
    else:
        end_date = datetime.now()

    if start:
        start_date = datetime.strptime(start, "%Y-%m-%d")
    else:
        start_date = end_date - timedelta(days=lookback_days)

    console.print(f"Analyzing coverage from {start_date.date()} to {end_date.date()}...")

    with PNCPExtractor(db_path, dataset) as extractor:
        result = extractor.run(start_date, end_date, resource)

    if result["windows_failed"] > 0:
        console.print(
            f"\n[yellow]⚠ Extraction finished with {result['windows_failed']} failed window(s)."
        )
        raise typer.Exit(1) from None


@app.command("verify")
def verify(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
) -> None:
    """Verify data coverage and detect gaps."""
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    with duckdb.connect(str(db_path), read_only=True) as con:
        # Get all completed windows
        completed_dates = {
            row[0].date()
            for row in con.execute(
                """
                SELECT window_start FROM baliza_state.coverage
                WHERE resource = ? AND status = 'completed' AND window_start >= ? AND window_start <= ?
                """,
                [resource, start_date, end_date],
            ).fetchall()
        }

    # Find missing dates
    gaps = []
    current_date = start_date
    while current_date <= end_date:
        if current_date.date() not in completed_dates:
            gaps.append(current_date.date())
        current_date += timedelta(days=1)

    # Display results
    if gaps:
        console.print(f"[yellow]⚠ Found {len(gaps)} gap(s) from {start} to {end}:")
        for gap_date in gaps:
            console.print(f"  • {gap_date}")
    else:
        console.print(f"[green]✓ Complete coverage from {start} to {end}")


@app.command("export")
def export(
    table: str = typer.Option(..., "--table", help="Table name to export"),
    output: Path = typer.Option(..., "--output", "-o", help="Output directory"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name"),
    date_col: str = typer.Option(
        "dataPublicacao", "--date-col", help="Date column for partitioning"
    ),
) -> None:
    """Export DuckDB table to partitioned Parquet files."""
    output.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute(f"""
            COPY (SELECT * FROM {dataset}.{table})
            TO '{output}'
            (FORMAT PARQUET, PARTITION_BY (ano, mes), OVERWRITE_OR_IGNORE)
        """)

    console.print(f"[green]✓ Exported {dataset}.{table} to {output}")


if __name__ == "__main__":
    app()
