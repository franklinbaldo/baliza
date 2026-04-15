"""Simplified CLI using direct extraction (no dlt)."""

from __future__ import annotations

import os
import time
import concurrent.futures
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, SpinnerColumn

from .consolidator import IAConsolidator
from .daily_exporter import DailyExporter
from .extractor import PNCPExtractor
from .ia_uploader import IAUploader
from .logging import configure_logging
from .utils import (
    escape_sql_literal,
    scrub_url_params,
    validate_identifier,
    validate_resource_path,
)

app = typer.Typer()
console = Console()


@app.callback()
def main() -> None:
    """Baliza - Simple PNCP extraction tool."""
    configure_logging()


@app.command("extract")
def extract(  # noqa: PLR0913
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
    workers: int = typer.Option(
        4,
        "--workers",
        "-w",
        min=1,
        max=16,
        help="Number of concurrent workers (1-16)",
    ),
    deadline_minutes: int | None = typer.Option(
        None,
        "--deadline-minutes",
        help="Stop gracefully after N minutes (preserves checkpoint)",
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

        # Compute deadline if specified
        deadline: datetime | None = None
        if deadline_minutes is not None:
            deadline = datetime.now() + timedelta(minutes=deadline_minutes)

        # Extract data
        start_time = time.time()
        with PNCPExtractor(db_path, dataset) as extractor:
            result = extractor.extract(
                start_date, end_date, resource, workers=workers, deadline=deadline
            )
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
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Extraction failed: {msg}")
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

            # Check resource health (circuit breaker)
            try:
                health = con.execute(
                    "SELECT consecutive_empty_days, status "
                    "FROM baliza_state.resource_health WHERE resource = ?",
                    [resource],
                ).fetchone()
                if health and health[1] in ("warning", "stalled"):
                    style = "red" if health[1] == "stalled" else "yellow"
                    console.print(
                        f"[{style}]⚠ Resource '{resource}': "
                        f"{health[0]} consecutive empty days "
                        f"(status: {health[1]})[/{style}]"
                    )
            except Exception:
                pass  # Table may not exist in older databases

    except Exception as e:
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Verify failed: {msg}")
        raise typer.Exit(1) from None


@app.command("export")
def export(
    table: str = typer.Option(..., "--table", help="Table name to export"),
    output: Path = typer.Option(..., "--output", "-o", help="Output directory"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name"),
    date_col: str = typer.Option(
        "data_publicacao", "--date-col", help="Date column for partitioning"
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
                # Escape path for SQL literal to prevent injection
                parquet_file_sql = escape_sql_literal(str(parquet_file))
                con.execute(f"""
                    COPY {dataset}.{table} TO '{parquet_file_sql}' (FORMAT PARQUET)
                """)

        console.print(f"[green]✓ Exported {dataset}.{table} to {parquet_file}")

    except Exception as e:
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Export failed: {msg}")
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
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Export failed: {msg}")
        raise typer.Exit(1) from None


@app.command("upload-daily")
def upload_daily(
    date_str: str = typer.Option(
        ...,
        "--date",
        "-d",
        help="Date to export and upload (YYYY-MM-DD)",
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
) -> None:
    """Export daily parquet package and upload to Internet Archive.

    This command combines export-daily with pushing to archive.org,
    creating manifest updates and handling dedup tracking.
    Requires IA_ACCESS_KEY and IA_SECRET_KEY environment variables.
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        ia_access_key = os.environ.get("IA_ACCESS_KEY")
        ia_secret_key = os.environ.get("IA_SECRET_KEY")
        if not ia_access_key or not ia_secret_key:
            console.print("[red]✗ Missing IA_ACCESS_KEY or IA_SECRET_KEY in environment.[/red]")
            raise typer.Exit(1)

        uploader = IAUploader(db_path)

        success = uploader.upload_day(target_date, output, ia_access_key, ia_secret_key)

        if not success:
            console.print(f"[dim]{date_str}: already uploaded or no data — skipped.[/dim]")
            raise typer.Exit(0)

    except Exception as e:
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Upload failed: {msg}")
        raise typer.Exit(1) from None


@app.command("consolidate")
def consolidate(
    start_year: int = typer.Option(
        2021,
        "--start-year",
        help="First year to consolidate (PNCP launched in 2021).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-upload even for frozen years that already have a consolidated file.",
    ),
) -> None:
    """Build and upload annual consolidated Parquet files to Internet Archive.

    Reads all daily Parquet files from IA (via manifest), merges them into one
    large annual file per year with optimal row groups, and uploads to
    baliza-pncp-consolidated/.

    Frozen past years are skipped if the consolidated file already exists.
    The current year is always rebuilt.

    Requires IA_ACCESS_KEY and IA_SECRET_KEY environment variables.
    """
    try:
        ia_access_key = os.environ.get("IA_ACCESS_KEY")
        ia_secret_key = os.environ.get("IA_SECRET_KEY")
        if not ia_access_key or not ia_secret_key:
            console.print("[red]✗ Missing IA_ACCESS_KEY or IA_SECRET_KEY in environment.[/red]")
            raise typer.Exit(1)

        consolidator = IAConsolidator()
        results = consolidator.consolidate_all(
            start_year, ia_access_key, ia_secret_key, force=force
        )

        uploaded = sum(1 for v in results.values() if v)
        skipped = sum(1 for v in results.values() if not v)
        console.print(
            f"\n[green]✓ Consolidation complete: {uploaded} uploaded, {skipped} skipped.[/green]"
        )

    except Exception as e:
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Consolidation failed: {msg}")
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
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Failed to get stats: {msg}")
        raise typer.Exit(1) from None


@app.command("status")
def status(
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
        validate_identifier(dataset)

        if not db_path.exists():
            console.print("[yellow]No database found. Run extraction first.[/yellow]")
            raise typer.Exit(0)

        with duckdb.connect(str(db_path), read_only=True) as con:
            # Total contracts
            total = con.execute(f"SELECT COUNT(*) FROM {dataset}.contratos").fetchone()[0]

            # Date range
            date_range = con.execute(f"""
                SELECT MIN(CAST(data_publicacao AS DATE)), MAX(CAST(data_publicacao AS DATE))
                FROM {dataset}.contratos
            """).fetchone()

            # Days with data
            days_count = con.execute(f"""
                SELECT COUNT(DISTINCT CAST(data_publicacao AS DATE))
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

            # Resource health (circuit breaker)
            try:
                health_rows = con.execute(
                    "SELECT resource, consecutive_empty_days, last_nonempty_date, status "
                    "FROM baliza_state.resource_health"
                ).fetchall()
            except Exception:
                health_rows = []

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
            console.print(
                f"\n[yellow]⚠ {checkpoints} extraction(s) incomplete"
                " - will resume on next run[/yellow]"
            )

        # Resource health warnings
        for row in health_rows:
            resource_name, empty_days, last_nonempty, health_status = row
            if health_status in ("warning", "stalled"):
                style = "red" if health_status == "stalled" else "yellow"
                last = str(last_nonempty) if last_nonempty else "never"
                console.print(
                    f"[{style}]⚠ Resource '{resource_name}': "
                    f"{empty_days} consecutive empty days "
                    f"(status: {health_status}, last data: {last})[/{style}]"
                )
    except Exception as e:
        msg = scrub_url_params(str(e))
        console.print(f"[red]✗ Failed to get status: {msg}")
        raise typer.Exit(1) from None


@app.command("sync")
def sync(  # noqa: PLR0913, PLR0915, PLR0912
    batch_size: int | None = typer.Option(None, "--batch-size", "-n", help="Max days to sync (None for all)"),
    start_date: str = typer.Option("2023-01-01", "--start-date", help="Oldest date to backfill"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", help="DuckDB file"),
    dataset: str = typer.Option("baliza_raw", "--dataset", help="Dataset name"),
    force_date: str | None = typer.Option(None, "--force-date", help="Target a specific date regardless of manifest"),
    limit_minutes: int | None = typer.Option(None, "--limit-minutes", help="Gracefully stop after N minutes"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without uploading"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel workers for page extraction"),
) -> None:
    """Unified sync: extracts missing dates and uploads to IA (greedy backwards sweep)."""
    start_time = datetime.now()
    ia_access_key = os.environ.get("IA_ACCESS_KEY") or os.environ.get("IAS3_ACCESS_KEY")
    ia_secret_key = os.environ.get("IA_SECRET_KEY") or os.environ.get("IAS3_SECRET_KEY")

    if not dry_run and (not ia_access_key or not ia_secret_key):
        console.print("[red]✗ Missing IA_ACCESS_KEY/IAS3_ACCESS_KEY or IA_SECRET_KEY/IAS3_SECRET_KEY in environment.[/red]")
        raise typer.Exit(1)

    uploader = IAUploader(db_path)

    # 1. Determine dates to process
    if force_date:
        batch = [datetime.strptime(force_date, "%Y-%m-%d").date()]
    else:
        with console.status("[bold green]Checking IA manifest for pending dates...[/bold green]"):
            try:
                uploaded = uploader.get_uploaded_dates()
            except Exception as e:
                console.print(f"[yellow]⚠ Could not read IA manifest (starting fresh): {e}[/yellow]")
                uploaded = set()

            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            yesterday = date.today() - timedelta(days=1)

            all_dates = []
            curr = start
            while curr <= yesterday:
                all_dates.append(curr)
                curr += timedelta(days=1)

            pending = [d for d in all_dates if d not in uploaded]
            pending.sort(reverse=True)  # BACKWARDS IN TIME

            if batch_size:
                batch = pending[:batch_size]
            else:
                batch = pending

    if not batch:
        console.print("[green]✓ Everything up to date.[/green]")
        return

    console.print(f"Syncing up to {len(batch)} dates" + (f" (timeout: {limit_minutes}m)" if limit_minutes else ""))

    # 2. Orchestration logic
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        refresh_per_second=4, # Smoother UI
    ) as progress:
        # Global Summary Tasks
        probe_task = progress.add_task("[bold cyan]Probing Dates[/bold cyan]", total=len(batch))
        fetch_task = progress.add_task("[bold magenta]Global Fetch Queue[/bold magenta]", total=0)
        upload_task = progress.add_task("[bold green]IA Upload Progress[/bold green]", total=len(batch))
        
        # Worker Status Slots (Fixed number based on --workers)
        worker_tasks = [
            progress.add_task(f"[dim]Worker {i+1:02d}: Idle[/dim]", total=100, completed=0)
            for i in range(workers)
        ]

        with PNCPExtractor(db_path, dataset) as extractor:
            day_to_pages: dict[datetime.date, int] = {}
            day_to_extracted: dict[datetime.date, int] = {}
            uploader = IAUploader(db_path)
            
            # Map futures to (task_info, worker_index)
            futures = {}
            
            # Use a semaphore or simple counter to assign workers
            available_workers = list(range(workers))

            def handle_completed_task(f, task_type, t_date, w_idx):
                try:
                    res = f.result()
                    # Mark worker as idle in UI
                    progress.update(worker_tasks[w_idx], description=f"[dim]Worker {w_idx+1:02d}: Idle[/dim]", completed=0)
                    available_workers.append(w_idx)

                    if task_type == "probe":
                        tp = res["total_pages"]
                        day_to_pages[t_date] = tp
                        day_to_extracted[t_date] = 0
                        progress.update(probe_task, advance=1)
                        if tp == 0:
                            progress.update(upload_task, advance=1)
                            return False
                        progress.update(fetch_task, total=progress.tasks[fetch_task].total + tp)
                        return True
                    else:
                        day_to_extracted[t_date] += res
                        day_to_pages[t_date] -= 1
                        progress.update(fetch_task, advance=1)
                        if day_to_pages[t_date] == 0:
                            if not dry_run:
                                uploader.upload_day(t_date, Path("data/daily"), ia_access_key, ia_secret_key)
                                extractor.cleanup_uploaded(datetime.combine(t_date, datetime.min.time()))
                            progress.update(upload_task, advance=1, description=f"[bold green]Uploaded {t_date}[/bold green]")
                        return True
                except Exception as e:
                    progress.console.print(f"[red]✗ Task {task_type} failed for {t_date}: {e}[/red]")
                    if w_idx in range(workers):
                        available_workers.append(w_idx)
                    return False

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                # To manage page tasks
                page_tasks_queue = []

                # 1. Main loop: Probing and Queueing
                for target_date in batch:
                    # Time check
                    elapsed = (datetime.now() - start_time).total_seconds() / 60
                    if limit_minutes and elapsed >= limit_minutes:
                        progress.console.print(f"\n[yellow]⚠ Time limit reached ({limit_minutes}m). Stopping gracefully.[/yellow]")
                        break

                    # Wait for a worker to probe
                    while not available_workers:
                        done, _ = concurrent.futures.wait(futures.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                        for f in list(done):
                            if f in futures:
                                (t_info, w_idx) = futures.pop(f)
                                t_type, t_date, _ = t_info
                                handle_completed_task(f, t_type, t_date, w_idx)
                                
                                # If it was a probe that succeeded, queue its pages
                                if t_type == "probe" and t_date in day_to_pages:
                                    tp = day_to_pages[t_date]
                                    for p in range(1, tp + 1):
                                        page_tasks_queue.append((t_date, p))

                    # Interleaved: If we have pages in queue and workers available, dispatch them
                    # PRIORITIZE FETCHING OVER PROBING
                    while page_tasks_queue and available_workers:
                        w_idx_p = available_workers.pop(0)
                        p_date, p_num = page_tasks_queue.pop(0)
                        progress.update(worker_tasks[w_idx_p], description=f"[magenta]Worker {w_idx_p+1:02d}: Fetching {p_date} p{p_num}[/magenta]", completed=20)
                        pf = executor.submit(extractor.fetch_page, "contratos", datetime.combine(p_date, datetime.min.time()), p_num)
                        futures[pf] = (("fetch", p_date, p_num), w_idx_p)

                    # Only if we still have available workers, dispatch a Probe
                    if available_workers:
                        w_idx = available_workers.pop(0)
                        dt = datetime.combine(target_date, datetime.min.time())
                        progress.update(worker_tasks[w_idx], description=f"[cyan]Worker {w_idx+1:02d}: Probing {target_date}[/cyan]", completed=50)
                        time.sleep(2.0) # Stagger more to avoid 429
                        f = executor.submit(extractor.probe_date, "contratos", dt)
                        futures[f] = (("probe", target_date, 0), w_idx)

                # 2. Final drain
                while futures or page_tasks_queue:
                    while page_tasks_queue and available_workers:
                        w_idx_p = available_workers.pop(0)
                        p_date, p_num = page_tasks_queue.pop(0)
                        progress.update(worker_tasks[w_idx_p], description=f"[magenta]Worker {w_idx_p+1:02d}: Fetching {p_date} p{p_num}[/magenta]", completed=20)
                        pf = executor.submit(extractor.fetch_page, "contratos", datetime.combine(p_date, datetime.min.time()), p_num)
                        futures[pf] = (("fetch", p_date, p_num), w_idx_p)

                    done, _ = concurrent.futures.wait(futures.keys(), timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED)
                    for f in done:
                        if f in futures:
                            (t_info, w_idx) = futures.pop(f)
                            t_type, t_date, _ = t_info
                            handle_completed_task(f, t_type, t_date, w_idx)
                            if t_type == "probe" and t_date in day_to_pages:
                                tp = day_to_pages[t_date]
                                for p in range(1, tp + 1):
                                    page_tasks_queue.append((t_date, p))


if __name__ == "__main__":
    app()
