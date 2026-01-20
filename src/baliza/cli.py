"""Simplified CLI using direct extraction (no dlt)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import typer
from rich.console import Console

from .extractor import PNCPExtractor

app = typer.Typer(help="Baliza - Simple PNCP extraction tool")
console = Console()


@app.command("extract")
def extract(
    start: str = typer.Option(
        None,
        "--start",
        help="Start date (YYYY-MM-DD). If not provided, runs in automatic mode.",
    ),
    end: str = typer.Option(
        None,
        "--end",
        help="End date (YYYY-MM-DD). If not provided, runs in automatic mode.",
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
    lookback_days: int = typer.Option(
        3,
        "--lookback-days",
        help="Number of days to look back for updates in automatic mode.",
    ),
) -> None:
    """Extract data from PNCP API to DuckDB.

    Can run in two modes:
    1. Manual: Provide --start and --end to extract a specific date range.
    2. Automatic: Omit --start and --end to run the resumable, state-aware
       pipeline, which finds and processes missing or incomplete days.
    """
    try:
        with PNCPExtractor(db_path, dataset) as extractor:
            if start and end:
                # Manual mode
                start_date = datetime.strptime(start, "%Y-%m-%d")
                end_date = datetime.strptime(end, "%Y-%m-%d")
                result = extractor.extract(start_date, end_date, resource)
                console.print("\n[green]✓ Manual extraction complete!")
                console.print(f"  - Rows: {result['rows_extracted']}")
                console.print(f"  - Pages: {result['pages']}")
            elif start or end:
                # Invalid combination
                console.print("[red]✗ Error: --start and --end must be provided together.")
                raise typer.Exit(1)
            else:
                # Automatic mode
                result = extractor.run(resource=resource, lookback_days=lookback_days)
                console.print("\n[green]✓ Automatic extraction complete!")
                console.print(f"  - Windows processed: {result['windows_processed']}")
                console.print(f"  - Total rows: {result['total_rows']}")

    except Exception as e:
        console.print(f"[red]✗ Extraction failed: {e}")
        raise typer.Exit(1) from None


@app.command("verify")
def verify(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
) -> None:
    """Verify data coverage and detect gaps."""
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

        with duckdb.connect(str(db_path), read_only=True) as con:
            # Get coverage records
            coverage = con.execute(
                """
                SELECT window_start, window_end, status
                FROM baliza_state.coverage
                WHERE resource = ?
                AND window_start >= ?
                AND window_end <= ?
                ORDER BY window_start
            """,
                [resource, start_date, end_date],
            ).fetchall()

            if not coverage:
                console.print(f"[yellow]⚠ No coverage found for {resource} from {start} to {end}")
                return

            # Find gaps (with 1-day tolerance for adjacent windows)
            gaps = []
            current = start_date
            one_day = timedelta(days=1)

            for window_start, window_end, _status in coverage:
                # Check if there's a significant gap (more than 1 day)
                gap_duration = (window_start - current).total_seconds()
                if gap_duration > one_day.total_seconds():
                    gaps.append((current, window_start))
                current = max(current, window_end)

            # Check final gap
            gap_duration = (end_date - current).total_seconds()
            if gap_duration > one_day.total_seconds():
                gaps.append((current, end_date))

            # Display results
            if gaps:
                console.print(f"[yellow]⚠ Found {len(gaps)} gap(s):")
                for gap_start, gap_end in gaps:
                    # Show the first missing day to last missing day
                    first_missing = (gap_start + one_day).date()
                    last_missing = (gap_end - one_day).date()
                    console.print(f"  • {first_missing} to {last_missing}")
            else:
                console.print(f"[green]✓ Complete coverage from {start} to {end}")

    except Exception as e:
        console.print(f"[red]✗ Verify failed: {e}")
        raise typer.Exit(1) from None


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
    """Export DuckDB table to Parquet files."""
    try:
        output.mkdir(parents=True, exist_ok=True)

        with duckdb.connect(str(db_path)) as con:
            # Simple export - dump everything to parquet
            parquet_file = output / f"{table}.parquet"
            con.execute(f"""
                COPY {dataset}.{table} TO '{parquet_file}' (FORMAT PARQUET)
            """)

        console.print(f"[green]✓ Exported {dataset}.{table} to {parquet_file}")

    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}")
        raise typer.Exit(1) from None


@app.command("verify")
def verify(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
) -> None:
    """Verify data coverage and detect gaps."""
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

        with duckdb.connect(str(db_path), read_only=True) as con:
            coverage = con.execute(
                """
                SELECT window_start, window_end, status
                FROM baliza_state.coverage
                WHERE resource = ? AND window_start >= ? AND window_end <= ?
                ORDER BY window_start
                """,
                [resource, start_date, end_date],
            ).fetchall()

            if not coverage:
                console.print(f"[yellow]⚠ No coverage found for {resource} from {start} to {end}")
                return

            gaps = []
            current = start_date
            one_day = timedelta(days=1)

            for window_start, window_end, _status in coverage:
                if (window_start - current).total_seconds() > one_day.total_seconds():
                    gaps.append((current, window_start))
                current = max(current, window_end)

            if (end_date - current).total_seconds() > one_day.total_seconds():
                gaps.append((current, end_date))

            if gaps:
                console.print(f"[yellow]⚠ Found {len(gaps)} gap(s):")
                for gap_start, gap_end in gaps:
                    first_missing = (gap_start + one_day).date()
                    last_missing = (gap_end - one_day).date()
                    console.print(f"  • {first_missing} to {last_missing}")
            else:
                console.print(f"[green]✓ Complete coverage from {start} to {end}")

    except Exception as e:
        console.print(f"[red]✗ Verify failed: {e}")
        raise typer.Exit(1) from None


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
    """Export DuckDB table to Parquet files."""
    try:
        output.mkdir(parents=True, exist_ok=True)
        parquet_file = output / f"{table}.parquet"

        with duckdb.connect(str(db_path)) as con:
            con.execute(f"COPY {dataset}.{table} TO '{parquet_file}' (FORMAT PARQUET)")

        console.print(f"[green]✓ Exported {dataset}.{table} to {parquet_file}")

    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}")
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
