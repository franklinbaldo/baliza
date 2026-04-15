"""Simplified CLI using direct extraction.

STATELESS: manifest.csv on Internet Archive is the source of truth.
SHARED ENGINE: Uses a single DuckDB session for extraction and upload.
"""

from __future__ import annotations

import concurrent.futures
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .consolidator import IAConsolidator
from .engine import BalizaEngine
from .extractor import PNCPExtractor
from .ia_uploader import IAUploader
from .logging import configure_logging

app = typer.Typer()
console = Console()


@app.callback()
def main() -> None:
    """Baliza - Simple PNCP extraction tool.

    CLEAN BREAK: This version (V2) unifies all backfill/export logic into 'sync'.
    Older legacy commands have been retired for architectural stability.
    """
    configure_logging()


@app.command("sync")
def sync(  # noqa: PLR0913, PLR0915, PLR0912
    batch_size: int | None = typer.Option(
        None, "--batch-size", "-n", help="Max days to sync (None for all)"
    ),
    start_date: str = typer.Option("2023-01-01", "--start-date", help="Oldest date to backfill"),
    db_path: Path | None = typer.Option(
        None, "--duckdb", help="DuckDB file (optional, defaults to :memory:)"
    ),
    force_date: str | None = typer.Option(
        None, "--force-date", help="Target a specific date regardless of manifest"
    ),
    limit_minutes: int = typer.Option(
        0, "--limit-minutes", help="Stop after this many minutes (0 = no limit)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be done without uploading"
    ),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel workers for page extraction"),
    no_curl: bool = typer.Option(False, "--no-curl", help="Opt-out of system cURL and use httpx"),
) -> None:
    """Unified sync: extracts missing dates and uploads to IA (stateless, backwards sweep)."""
    start_time_exec = datetime.now()
    ia_access_key = os.environ.get("IA_ACCESS_KEY") or os.environ.get("IAS3_ACCESS_KEY")
    ia_secret_key = os.environ.get("IA_SECRET_KEY") or os.environ.get("IAS3_SECRET_KEY")

    if not dry_run and (not ia_access_key or not ia_secret_key):
        console.print("[red]✗ Missing IA keys in environment.[/red]")
        raise typer.Exit(1)

    # 1. CREATE SHARED ENGINE
    engine = BalizaEngine(db_path)
    uploader = IAUploader(engine)

    # 2. Determine dates to process using REMOTE manifest
    if force_date:
        batch = [datetime.strptime(force_date, "%Y-%m-%d").date()]
    else:
        with console.status("[bold green]Checking IA manifest for pending dates...[/bold green]"):
            try:
                uploaded = uploader.get_uploaded_dates()
            except Exception as e:
                console.print(
                    f"[yellow]⚠ Could not read IA manifest (starting fresh): {e}[/yellow]"
                )
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
            batch = pending[:batch_size] if batch_size else pending

    if not batch:
        console.print("[green]✓ Everything up to date.[/green]")
        return

    # 3. Orchestration logic
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        refresh_per_second=4,
        expand=True,
    ) as progress:
        overall_task = progress.add_task(
            "[bold white]Overall Progress[/bold white]", total=len(batch)
        )
        probe_task = progress.add_task("[bold cyan]Probing Dates[/bold cyan]", total=len(batch))
        fetch_task = progress.add_task("[bold magenta]Global Fetch Queue[/bold magenta]", total=0)

    # 3. Micro-metrics
    total_records = 0
    quarantine_count = 0
    error_count = 0

    # 3. Orchestration logic
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None, pulse_style="bright_black"),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        refresh_per_second=10,
        expand=True,
    ) as progress:
        overall_task = progress.add_task(
            "Overall Sync Progress", total=len(batch)
        )
        
        # We'll use a local dict to map workers to their current date task
        # But even better: just add/remove tasks dynamically.
        # Since Workers=N, only N day_tasks will be active at once.
        day_tasks: dict[date, TaskID] = {}

        with PNCPExtractor(engine, use_curl=not no_curl) as extractor:
            # Use pool for parallel scraping
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures: dict[concurrent.futures.Future[Any], tuple[str, date, int]] = {}

                def process_day_full(t_date: date, total_pages: int):
                    nonlocal total_records, quarantine_count, error_count
                    
                    tid = progress.add_task(f"Day {t_date} [Fetching]", total=total_pages, completed=1)
                    day_tasks[t_date] = tid
                    
                    try:
                        # 1. Fetch remaining pages if any
                        if total_pages > 1:
                            for p in range(2, total_pages + 1):
                                extractor.fetch_page("contratos", datetime.combine(t_date, datetime.min.time()), p)
                                progress.update(tid, advance=1)

                        # 2. Ingest
                        progress.update(tid, description=f"Day {t_date} [Ingesting]")
                        stats = extractor.ingest_day(datetime.combine(t_date, datetime.min.time()))
                        total_records += stats.get("valid", 0)
                        quarantine_count += stats.get("quarantine", 0)

                        # 3. Export & Upload
                        q_csv = Path(f"data/quarentena-{t_date.isoformat()}.csv")
                        has_q = extractor.export_quarantine(datetime.combine(t_date, datetime.min.time()), q_csv)

                        if not dry_run:
                            progress.update(tid, description=f"Day {t_date} [Uploading]")
                            if  ia_access_key and ia_secret_key:
                                uploader.upload_day(
                                    t_date, Path("data/daily"), ia_access_key, ia_secret_key,
                                    quarantine_stats=stats, quarantine_csv=q_csv if has_q else None
                                )
                        else:
                            progress.console.log(f"[yellow]Dry-run: {t_date} verified ({stats['valid']} records)[/yellow]")

                        if q_csv.exists():
                            q_csv.unlink()
                            
                        progress.update(overall_task, advance=1)
                    except Exception as e:
                        error_count += 1
                        progress.console.log(f"[bold red]✗ Error {t_date}: {e}[/bold red]")
                    finally:
                        progress.remove_task(tid)
                        if t_date in day_tasks:
                            del day_tasks[t_date]

                # DISPATCHER
                for target_date in batch:
                    # Time check
                    elapsed = (datetime.now() - start_time_exec).total_seconds() / 60
                    if limit_minutes and elapsed >= limit_minutes:
                        progress.console.log(f"[yellow]⚠ Time limit ({limit_minutes}m) reached.[/yellow]")
                        break

                    # Wait if too many futures (worker limiting)
                    # We want to maintain N active days
                    while len(futures) >= workers:
                        done, _ = concurrent.futures.wait(
                            futures.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED
                        )
                        for f in done:
                            futures.pop(f)

                    # Submit next day
                    def daily_workflow(d: date):
                        res = extractor.probe_date("contratos", datetime.combine(d, datetime.min.time()))
                        process_day_full(d, res["total_pages"])

                    f = executor.submit(daily_workflow, target_date)
                    futures[f] = ("workflow", target_date, 0)

                # Final drain
                concurrent.futures.wait(futures.keys())

    # 4. FINAL SUMMARY PANEL
    duration = datetime.now() - start_time_exec
    summary = (
        f"• [bold white]Total Data points:[/] {total_records:,}\n"
        f"• [bold yellow]Quarantine:[/] {quarantine_count:,}\n"
        f"• [bold red]Errors:[/] {error_count}\n"
        f"• [bold cyan]Duration:[/] {duration.total_seconds():.1f}s"
    )
    console.print("\n")
    console.print(Panel(summary, title="[bold green]✓ Sync Results[/]", expand=False))


@app.command("verify")
def verify(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
) -> None:
    """Verify data coverage by checking the REMOTE IA manifest."""
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.strptime(end, "%Y-%m-%d").date()

        # We need an engine even to just check the uploader manifest
        engine = BalizaEngine()
        uploader = IAUploader(engine)
        with console.status("[bold green]Checking remote manifest...[/bold green]"):
            uploaded = uploader.get_uploaded_dates()

        gaps = []
        curr = start_date
        while curr <= end_date:
            if curr not in uploaded:
                gaps.append(curr)
            curr += timedelta(days=1)

        if not gaps:
            console.print(f"[green]✓ Complete coverage from {start} to {end} on Internet Archive.")
        else:
            console.print(f"[yellow]⚠ Found {len(gaps)} missing day(s) on IA:")
            # Group gaps for readability
            for d in gaps[:20]:
                console.print(f"  • {d}")
            if len(gaps) > 20:
                console.print(f"  ... and {len(gaps) - 20} more.")

    except Exception as e:
        console.print(f"[red]✗ Verify failed: {e}")


@app.command("consolidate")
def consolidate_cmd(
    start_year: int = typer.Option(2021, "--start-year"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    """Annual consolidation of daily Parquet files (Stateless)."""
    try:
        ia_access_key = os.environ.get("IA_ACCESS_KEY")
        ia_secret_key = os.environ.get("IA_SECRET_KEY")
        if not ia_access_key or not ia_secret_key:
            console.print("[red]✗ Missing IA keys.[/red]")
            raise typer.Exit(1)

        consolidator = IAConsolidator()
        consolidator.consolidate_all(start_year, ia_access_key, ia_secret_key, force=force)
    except Exception as e:
        console.print(f"[red]✗ Consolidation failed: {e}")
        raise typer.Exit(1) from e
