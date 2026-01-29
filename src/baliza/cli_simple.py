"""Simplified CLI using direct extraction (no dlt)."""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .daily_exporter import DailyExporter
from .extractor import PNCPExtractor
from .utils import validate_identifier, validate_resource_path

app = typer.Typer(help="Baliza - Simple PNCP extraction tool")
state_app = typer.Typer(name="state", help="Manage and inspect extraction state.")
app.add_typer(state_app)
console = Console()


@app.command("extract")
def extract(
    start: str = typer.Option(
        ...,
        "--start",
        help="Start date (YYYY-MM-DD)",
    ),
    end: str = typer.Option(
        ...,
        "--end",
        help="End date (YYYY-MM-DD)",
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
    """Extract data from PNCP API to DuckDB.

    Simple extraction command without complex gap detection or resumability.
    Just fetches data from start to end date and saves to DuckDB.
    """
    try:
        # Validate resource
        validate_resource_path(resource)

        # Parse dates
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

        # Extract data
        start_time = time.time()
        with PNCPExtractor(db_path, dataset) as extractor:
            result = extractor.extract(start_date, end_date, resource)
        duration = time.time() - start_time

        # Create summary
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="bold")
        grid.add_column(justify="right")

        grid.add_row("Rows Extracted:", f"{result['rows_extracted']:,}")
        grid.add_row("Pages:", f"{result['pages']:,}")
        grid.add_row("Date Range:", f"{start} to {end}")
        grid.add_row("Duration:", f"{duration:.1f}s")

        console.print(
            Panel(
                grid,
                title="[green]✓ Extraction Complete[/green]",
                border_style="green",
                expand=False,
            )
        )

    except Exception as e:
        console.print(f"[red]✗ Extraction failed: {e}")
        raise typer.Exit(1) from None


@state_app.command("gaps")
def gaps(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
) -> None:
    """Verify data coverage and detect gaps."""
    try:
        # Validate resource
        validate_resource_path(resource)

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
                extract_cmd = f"baliza extract --start {start} --end {end} --resource {resource}"
                if str(db_path) != "baliza.duckdb":
                    extract_cmd += f" --duckdb {db_path}"

                msg = (
                    f"No extraction data found for [bold]{resource}[/bold] "
                    f"between [bold]{start}[/bold] and [bold]{end}[/bold].\n\n"
                    f"[white]To fix this, run:[/white]\n"
                    f"[cyan]{extract_cmd}[/cyan]"
                )
                console.print(
                    Panel(
                        msg,
                        title="[yellow]⚠ No Coverage Found[/yellow]",
                        border_style="yellow",
                        padding=(1, 2),
                    )
                )
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
        # Validate inputs used in SQL construction
        validate_identifier(table)
        validate_identifier(dataset)

        output.mkdir(parents=True, exist_ok=True)

        with console.status(
            f"[bold green]Exporting {dataset}.{table} to parquet...[/bold green]",
            spinner="dots",
        ):
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


@app.command("export-daily")
def export_daily(
    date_str: str = typer.Option(
        ...,
        "--date",
        "-d",
        help="Date to export (YYYY-MM-DD)",
    ),
    output: Path = typer.Option(
        Path("data/daily"),
        "--output",
        "-o",
        help="Output directory (date subdirectory will be created)",
    ),
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        help="DuckDB file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name",
    ),
) -> None:
    """Export daily self-contained parquet package.

    Creates a date-specific directory with:
    - contratos.parquet (main contracts table)
    - orgaos.parquet (deduplicated organizations)
    - unidades.parquet (organizational units)
    - _metadata.json (schema version and stats)
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        exporter = DailyExporter(db_path, dataset)

        with console.status(
            f"[bold green]Exporting daily package for {date_str}...[/bold green]",
            spinner="dots",
        ):
            stats = exporter.export(target_date, output)

        # Show summary table
        table = Table(title=f"Daily Export: {date_str}")
        table.add_column("Table", style="cyan")
        table.add_column("Rows", justify="right")
        table.add_column("Size", justify="right")

        for name, info in stats["tables"].items():
            size_kb = info["file_size_bytes"] / 1024
            table.add_row(name, str(info["row_count"]), f"{size_kb:.1f} KB")

        console.print(table)
        console.print(f"\n[green]✓ Output: {output / date_str}/")

    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}")
        raise typer.Exit(1) from None


@app.command("buffer-stats")
def buffer_stats(
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="DuckDB file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name",
    ),
) -> None:
    """Show buffer statistics for monitoring."""
    try:
        with PNCPExtractor(db_path, dataset) as extractor:
            stats = extractor.get_buffer_stats()

        console.print(Panel("[bold]Buffer Statistics[/bold]"))
        console.print(f"  Total rows in buffer: [cyan]{stats['total_rows']:,}[/cyan]")
        console.print(f"  Dates in buffer: [cyan]{stats['dates_in_buffer']}[/cyan]")
        console.print(f"  Dates uploaded to IA: [cyan]{stats['dates_uploaded_to_ia']}[/cyan]")
        console.print(f"  Pending checkpoints: [yellow]{stats['pending_checkpoints']}[/yellow]")

        if stats["rows_by_date"]:
            console.print("\n[bold]Rows by Date:[/bold]")
            table = Table()
            table.add_column("Date", style="cyan")
            table.add_column("Rows", justify="right")

            for dt, count in sorted(stats["rows_by_date"].items()):
                table.add_row(dt, f"{count:,}")

            console.print(table)

    except Exception as e:
        console.print(f"[red]✗ Failed to get stats: {e}")
        raise typer.Exit(1) from None


@state_app.command("show")
def show(
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="DuckDB file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name",
    ),
) -> None:
    """Show overall extraction status."""
    try:
        if not db_path.exists():
            console.print("[yellow]No database found. Run extraction first.[/yellow]")
            raise typer.Exit(0)

        with duckdb.connect(str(db_path), read_only=True) as con:
            # Total contracts
            total = con.execute(f"SELECT COUNT(*) FROM {dataset}.contratos").fetchone()[0]

            # Date range
            date_range = con.execute(f"""
                SELECT MIN(CAST(dataPublicacao AS DATE)), MAX(CAST(dataPublicacao AS DATE))
                FROM {dataset}.contratos
            """).fetchone()

            # Days with data
            days_count = con.execute(f"""
                SELECT COUNT(DISTINCT CAST(dataPublicacao AS DATE))
                FROM {dataset}.contratos
            """).fetchone()[0]

            # Uploaded to IA
            try:
                uploaded = con.execute(
                    "SELECT COUNT(*) FROM baliza_state.uploaded_to_ia"
                ).fetchone()[0]
            except Exception:
                uploaded = 0

            # Pending checkpoints
            try:
                checkpoints = con.execute(
                    "SELECT COUNT(*) FROM baliza_state.extraction_checkpoint"
                ).fetchone()[0]
            except Exception:
                checkpoints = 0

        # Display
        console.print(Panel("[bold]Baliza PNCP Status[/bold]", style="blue"))
        console.print()

        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="dim")
        table.add_column("Value", style="cyan")

        table.add_row("Total contracts", f"{total:,}")
        table.add_row("Date range", f"{date_range[0]} to {date_range[1]}" if date_range[0] else "-")
        table.add_row("Days with data", str(days_count))
        table.add_row("Days on Internet Archive", str(uploaded))
        table.add_row("Pending extractions", str(checkpoints))

        console.print(table)

        # Warnings
        if checkpoints > 0:
            console.print(f"\n[yellow]⚠ {checkpoints} extraction(s) incomplete - will resume on next run[/yellow]")

    except Exception as e:
        console.print(f"[red]✗ Failed to get status: {e}")
        raise typer.Exit(1) from None


@state_app.command("history")
def history(
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to DuckDB database file",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        "-l",
        help="Limit number of runs to display",
    ),
) -> None:
    """Show history of extraction runs."""
    try:
        if not db_path.exists():
            console.print("[yellow]No database found. Run extraction first.[/yellow]")
            raise typer.Exit(0)

        with duckdb.connect(str(db_path), read_only=True) as con:
            runs = con.execute(
                f"""
                SELECT run_id, start_time, end_time, status, windows_processed, rows_extracted
                FROM baliza_state.extraction_runs
                ORDER BY start_time DESC
                LIMIT {limit}
            """
            ).fetchall()

        table = Table(title="Extraction History")
        table.add_column("Run ID")
        table.add_column("Start Time")
        table.add_column("Duration (s)")
        table.add_column("Status")
        table.add_column("Windows")
        table.add_column("Rows")

        for run_id, start_time, end_time, status, windows, rows in runs:
            duration = (end_time - start_time).total_seconds() if end_time else "N/A"
            status_color = "green" if status == "completed" else "red"
            table.add_row(
                run_id,
                str(start_time),
                f"{duration:.2f}" if isinstance(duration, float) else duration,
                f"[{status_color}]{status}[/{status_color}]",
                str(windows),
                f"{rows:,}",
            )

        console.print(table)

    except Exception as e:
        console.print(f"[red]✗ Failed to get history: {e}")
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
