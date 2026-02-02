from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ...tiers import tier2
from ...utils import validate_resource_path

state_app = typer.Typer(help="Manage and inspect extraction state")
console = Console()


@state_app.command("show")
@tier2
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
    """Show overall extraction status (previously 'status')."""
    try:
        if not db_path.exists():
            console.print("[yellow]No database found. Run extraction first.[/yellow]")
            return

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


@state_app.command("gaps")
@tier2
def gaps(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
) -> None:
    """Verify data coverage and detect gaps (previously 'verify')."""
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
            found_gaps = []
            current = start_date
            one_day = timedelta(days=1)

            for window_start, window_end, _status in coverage:
                # Check if there's a significant gap (more than 1 day)
                gap_duration = (window_start - current).total_seconds()
                if gap_duration > one_day.total_seconds():
                    found_gaps.append((current, window_start))
                current = max(current, window_end)

            # Check final gap
            gap_duration = (end_date - current).total_seconds()
            if gap_duration > one_day.total_seconds():
                found_gaps.append((current, end_date))

            # Display results
            if found_gaps:
                console.print(f"[yellow]⚠ Found {len(found_gaps)} gap(s):")
                for gap_start, gap_end in found_gaps:
                    # Show the first missing day to last missing day
                    first_missing = (gap_start + one_day).date()
                    last_missing = (gap_end - one_day).date()
                    console.print(f"  • {first_missing} to {last_missing}")
            else:
                console.print(f"[green]✓ Complete coverage from {start} to {end}")

    except Exception as e:
        console.print(f"[red]✗ Verify failed: {e}")
        raise typer.Exit(1) from None


@state_app.command("history")
@tier2
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
            return

        with duckdb.connect(str(db_path), read_only=True) as con:
            # Check if table exists
            table_exists = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'extraction_runs' AND table_schema = 'baliza_state'"
            ).fetchone()[0]

            if not table_exists:
                console.print("[yellow]No execution history found yet.[/yellow]")
                return

            runs = con.execute(
                f"""
                SELECT run_id, start_time, end_time, status, windows_processed, rows_extracted
                FROM baliza_state.extraction_runs
                ORDER BY start_time DESC
                LIMIT {limit}
            """
            ).fetchall()

        if not runs:
            console.print("[yellow]No execution history found yet.[/yellow]")
            return

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
