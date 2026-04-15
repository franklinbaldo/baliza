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
    db_path: Path | None = typer.Option(None, "--duckdb", help="DuckDB file (optional, defaults to :memory:)"),
    force_date: str | None = typer.Option(
        None, "--force-date", help="Target a specific date regardless of manifest"
    ),
    limit_minutes: int = typer.Option(0, "--limit-minutes", help="Stop after this many minutes (0 = no limit)"),
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
        overall_task = progress.add_task("[bold white]Overall Progress[/bold white]", total=len(batch))
        probe_task = progress.add_task("[bold cyan]Probing Dates[/bold cyan]", total=len(batch))
        fetch_task = progress.add_task("[bold magenta]Global Fetch Queue[/bold magenta]", total=0)
        
        day_tasks: dict[date, TaskID] = {}

        with PNCPExtractor(engine, use_curl=not no_curl) as extractor:
            # Use pool for parallel scraping
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures: dict[concurrent.futures.Future[Any], tuple[str, date, int]] = {}

                def finish_day(t_date: date):
                    try:
                        # 1. Ingest (Stateless Pydantic + Shared Ibis Engine)
                        stats = extractor.ingest_day(datetime.combine(t_date, datetime.min.time()))
                        
                        # 2. Export Quarantine to local CSV if any
                        q_csv = Path(f"data/quarentena-{t_date.isoformat()}.csv")
                        has_q = extractor.export_quarantine(datetime.combine(t_date, datetime.min.time()), q_csv)
                        
                        if not dry_run:
                            # 3. Upload Parquet + Raw Zip + Quarantine CSV
                            if not ia_access_key or not ia_secret_key:
                                raise ValueError("Missing IA credentials for upload")
                                
                            uploader.upload_day(
                                t_date, 
                                Path("data/daily"), 
                                ia_access_key, 
                                ia_secret_key,
                                quarantine_stats=stats,
                                quarantine_csv=q_csv if has_q else None
                            )
                        else:
                            console.print(f"[yellow]Dry-run: would upload {t_date} (stats: {stats})[/yellow]")

                        if q_csv.exists():
                            q_csv.unlink()

                        if t_date in day_tasks:
                            progress.remove_task(day_tasks[t_date])
                            del day_tasks[t_date]
                        progress.update(overall_task, advance=1)
                    except Exception as e:
                        console.print(f"[bold red]✗ Fatal error processing {t_date}: {e}[/bold red]")
                        # We don't re-raise here to avoid killing the entire worker pool, 
                        # but the overall task will show it didn't complete.

                # DISPATCH LOOP
                for target_date in batch:
                    # Time check
                    elapsed = (datetime.now() - start_time_exec).total_seconds() / 60
                    if limit_minutes and elapsed >= limit_minutes:
                        console.print(f"\n[yellow]⚠ Time limit ({limit_minutes}m). Stopping.[/yellow]")
                        break

                    # Dispatch Probes
                    f = executor.submit(extractor.probe_date, "contratos", datetime.combine(target_date, datetime.min.time()))
                    futures[f] = ("probe", target_date, 0)
                    
                    # Monitor futures and dispatch new ones
                    while len(futures) >= workers:
                        done, _ = concurrent.futures.wait(futures.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
                        for f in done:
                            task_type, t_date, _ = futures.pop(f)
                            try:
                                res = f.result()
                                if task_type == "probe":
                                    tp = res["total_pages"]
                                    progress.update(probe_task, advance=1)
                                    if tp <= 1: # Done or 1 page (probe already saved p1)
                                        finish_day(t_date)
                                    else:
                                        # Queue remaining pages
                                        day_tasks[t_date] = progress.add_task(f"[dim]Day {t_date}[/dim]", total=tp, completed=1)
                                        progress.update(fetch_task, total=progress.tasks[fetch_task].total + (tp - 1))
                                        for p in range(2, tp + 1):
                                            pf = executor.submit(extractor.fetch_page, "contratos", datetime.combine(t_date, datetime.min.time()), p)
                                            futures[pf] = ("fetch", t_date, p)
                            except Exception as e:
                                console.print(f"[red]✗ {task_type} {t_date} failed: {e}[/red]")

                # Final drain
                for f in concurrent.futures.as_completed(futures):
                    task_type, t_date, _ = futures[f]
                    try:
                        res = f.result()
                        if task_type == "fetch":
                            progress.update(fetch_task, advance=1)
                            progress.update(day_tasks[t_date], advance=1)
                            # Check if day is complete
                            if progress.tasks[day_tasks[t_date]].finished:
                                finish_day(t_date)
                        elif task_type == "probe":
                            progress.update(probe_task, advance=1)
                            tp = res["total_pages"]
                            if tp <= 1:
                                finish_day(t_date)
                            else:
                                # This rarely happens in the drain phase unless workers=1 and only 1 day
                                day_tasks[t_date] = progress.add_task(f"[dim]Day {t_date}[/dim]", total=tp, completed=1)
                                finish_day(t_date) # Fallback
                    except Exception as e:
                        console.print(f"[red]✗ {t_date} drain error: {e}[/red]")

    console.print(Panel("[bold green]✓ Sync Cycle Complete[/bold green]", expand=False))

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
                console.print(f"  ... and {len(gaps)-20} more.")
                
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
