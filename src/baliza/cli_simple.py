"""Simplified CLI using direct extraction.

STATELESS: manifest.csv on Internet Archive is the source of truth.
SHARED ENGINE: Uses a single DuckDB session for extraction and upload.
"""

from __future__ import annotations

import concurrent.futures
import os
import sys
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
from .extractor import PNCPExtractor, _validate_resource
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
        None, "--batch-size", "-n", help="Max months to sync (None for all)"
    ),
    start_date: str = typer.Option("2023-01-01", "--start-date", help="Oldest date to backfill"),
    db_path: Path | None = typer.Option(
        None, "--duckdb", help="DuckDB file (optional, defaults to :memory:)"
    ),
    force_month: str | None = typer.Option(
        None, "--force-month", help="Target a specific month (YYYY-MM) regardless of manifest"
    ),
    limit_minutes: int = typer.Option(
        0, "--limit-minutes", help="Stop after this many minutes (0 = no limit)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be done without uploading"
    ),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel workers for page extraction"),
    no_curl: bool = typer.Option(False, "--no-curl", help="Opt-out of system cURL and use httpx"),
    no_consolidate: bool = typer.Option(
        False, "--no-consolidate", help="Skip end-of-run annual consolidation"
    ),
    consolidate_start_year: int = typer.Option(
        2021, "--consolidate-start-year", help="First year to consider for consolidation"
    ),
) -> None:
    """Unified sync: extracts missing dates, uploads to IA, and consolidates (stateless, backwards sweep)."""
    start_time_exec = datetime.now()
    ia_access_key = os.environ.get("IA_ACCESS_KEY") or os.environ.get("IAS3_ACCESS_KEY")
    ia_secret_key = os.environ.get("IA_SECRET_KEY") or os.environ.get("IAS3_SECRET_KEY")

    # 0. VALIDATE RESOURCE early
    try:
        # Default resource for sync is 'contratos' inside PNCPExtractor
        # But we check it here if needed.
        _validate_resource("contratos")
    except ValueError as e:
        print(str(e))
        sys.stdout.flush()
        raise typer.Exit(1) from None

    if not dry_run and (not ia_access_key or not ia_secret_key):
        console.print("[red]✗ Missing IA keys in environment.[/red]")
        raise typer.Exit(1)

    # 1. CREATE SHARED ENGINE
    engine = BalizaEngine(db_path)
    uploader = IAUploader(engine)

    # 2. Fetch manifest once — reused by the pending-months planner and
    # (if enabled) the up-front consolidation catch-up below.
    raw_manifest: list[dict] = []
    if force_month:
        batch = [datetime.strptime(force_month, "%Y-%m").date()]
        uploaded: set[str] = set()
    else:
        with console.status("[bold green]Checking IA manifest for pending months...[/bold green]"):
            try:
                # manifest dates in monthly strategy are strings "YYYY-MM"
                raw_manifest = uploader._read_manifest_from_ia()
                uploaded = {row["data_particao"] for row in raw_manifest if row.get("data_particao")}
            except Exception as e:
                console.print(
                    f"[yellow]⚠ Could not read IA manifest (starting fresh): {e}[/yellow]"
                )
                uploaded = set()

            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            # Month-based start (first day of its month)
            start = start.replace(day=1)
            
            today = date.today()
            # End is the previous month
            last_month_end = (today.replace(day=1) - timedelta(days=1)).replace(day=1)

            pending_months = []
            curr = start
            while curr <= last_month_end:
                month_key = curr.strftime("%Y-%m")
                if month_key not in uploaded:
                    pending_months.append(curr)
                
                # Advance to next month
                if curr.month == 12:
                    curr = curr.replace(year=curr.year + 1, month=1)
                else:
                    curr = curr.replace(month=curr.month + 1)

            pending_months.sort(reverse=True)  # BACKWARDS IN TIME
            batch = pending_months[:batch_size] if batch_size else pending_months

    # 3. CONSOLIDATION CATCH-UP (runs first so a truncated consolidation
    # in the previous sync gets finished this run, even if the current
    # run doesn't upload any new months). The consolidator gates itself
    # on manifest freshness (monthly_uf shard `uploaded_at` vs canonical
    # `uploaded_at`) — zero-cost when already fresh.
    consolidated = False
    consolidation_error: Exception | None = None
    if not dry_run and not no_consolidate and ia_access_key and ia_secret_key:
        try:
            with console.status("[bold green]Checking consolidation status...[/bold green]"):
                # Pass already-fetched manifest through when we have it
                # (non-force_month path); let consolidator refetch on the
                # force_month path where we skipped the planner fetch.
                IAConsolidator().consolidate_all(
                    consolidate_start_year,
                    ia_access_key,
                    ia_secret_key,
                    manifest=raw_manifest if raw_manifest else None,
                )
            consolidated = True
        except Exception as e:
            # Don't block extraction — fresh data capture is independent of
            # consolidation. But record the error so we can exit non-zero at
            # the end, which fires the workflow's report-failure job.
            consolidation_error = e
            console.print(f"[red]✗ Consolidation failed: {e}[/red]")

    if not batch:
        if consolidation_error is not None:
            raise typer.Exit(1)
        console.print("[green]✓ Everything up to date.[/green]")
        return

    # 4. Orchestration logic
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
            "[bold white]Overall Sync Progress (Months)[/bold white]", total=len(batch)
        )
        
        month_tasks: dict[str, TaskID] = {}

        # Use pool for parallel scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures: dict[concurrent.futures.Future[Any], date] = {}

            def process_month_full(start_of_month: date):  # noqa: PLR0912
                nonlocal total_records, quarantine_count, error_count
                
                month_str = start_of_month.strftime("%Y-%m")
                # Calculate end of month
                if start_of_month.month == 12:
                    next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
                else:
                    next_month = start_of_month.replace(month=start_of_month.month + 1)
                end_of_month = next_month - timedelta(days=1)

                # THREAD-SAFE ENGINE: Each worker gets its own connection
                thread_engine = engine.connect_thread_safe()
                
                # 1. Lifecycle Progress Bar Start
                tid = progress.add_task(f"Month {month_str} [Probing]", total=3, completed=0)
                month_tasks[month_str] = tid
                
                try:
                    with PNCPExtractor(thread_engine, use_curl=not no_curl) as extractor:
                        # 2. Probe (Page 1)
                        res = extractor.probe_range(
                            "contratos", 
                            datetime.combine(start_of_month, datetime.min.time()),
                            datetime.combine(end_of_month, datetime.min.time())
                        )
                        total_pages = res["total_pages"]
                        
                        # Update progress bar total and description
                        # Total = pages + 1 (ingest) + 1 (upload)
                        progress.update(tid, total=total_pages + 2, advance=1, description=f"Month {month_str} [Pages 1/{total_pages}]")
                        
                        # 3. Fetch remaining pages
                        if total_pages > 1:
                            for p in range(2, total_pages + 1):
                                progress.update(tid, description=f"Month {month_str} [Pages {p}/{total_pages}]")
                                extractor.fetch_page(
                                    "contratos", 
                                    datetime.combine(start_of_month, datetime.min.time()),
                                    datetime.combine(end_of_month, datetime.min.time()),
                                    p
                                )
                                progress.update(tid, advance=1)

                        # 4. Ingest
                        progress.update(tid, description=f"Month {month_str} [Ingesting]")
                        stats = extractor.ingest_range(datetime.combine(start_of_month, datetime.min.time()))
                        total_records += stats.get("valid", 0)
                        quarantine_count += stats.get("quarantine", 0)
                        progress.update(tid, advance=1)

                        # 5. Export & Upload
                        q_csv = Path(f"data/quarentena-{month_str}.csv")
                        has_q = extractor.export_quarantine(datetime.combine(start_of_month, datetime.min.time()), q_csv)

                        if not dry_run:
                            progress.update(tid, description=f"Month {month_str} [Uploading]")
                            if ia_access_key and ia_secret_key:
                                uploader.upload_month(
                                    start_of_month, Path("data/processed"), ia_access_key, ia_secret_key,
                                    quarantine_stats=stats, quarantine_csv=q_csv if has_q else None
                                )
                        else:
                            progress.console.log(f"[yellow]Dry-run: {month_str} verified ({stats['valid']} records)[/yellow]")
                        
                        # Step complete
                        progress.update(tid, advance=1)

                        if q_csv.exists():
                            q_csv.unlink()
                            
                    progress.update(overall_task, advance=1)
                except ValueError as e:
                    if "Invalid resource path" in str(e):
                        progress.console.log(f"[bold red]✗ {e}[/bold red]")
                    raise
                except Exception as e:
                    error_count += 1
                    progress.console.log(f"[bold red]✗ Error {month_str}: {e}[/bold red]")
                finally:
                    progress.remove_task(tid)
                    if month_str in month_tasks:
                        del month_tasks[month_str]

            # DISPATCHER
            for target_month in batch:
                # Time limit check
                elapsed = (datetime.now() - start_time_exec).total_seconds() / 60
                if limit_minutes and elapsed >= limit_minutes:
                    progress.console.log(f"[yellow]⚠ Time limit ({limit_minutes}m) reached.[/yellow]")
                    break

                # Maintain worker pool
                while len(futures) >= workers:
                    done, _ = concurrent.futures.wait(
                        futures.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for f in done:
                        futures.pop(f)

                f = executor.submit(process_month_full, target_month)
                futures[f] = target_month

            # Final drain
            concurrent.futures.wait(futures.keys())

    # 5. FINAL SUMMARY PANEL
    duration = datetime.now() - start_time_exec
    if consolidated:
        consolidation_status = "ran"
    elif consolidation_error is not None:
        consolidation_status = "FAILED"
    else:
        consolidation_status = "skipped"
    summary = (
        f"• [bold white]Total Records:[/] {total_records:,}\n"
        f"• [bold yellow]Quarantine:[/] {quarantine_count:,}\n"
        f"• [bold red]Errors:[/] {error_count}\n"
        f"• [bold magenta]Consolidation catch-up:[/] {consolidation_status}\n"
        f"• [bold cyan]Duration:[/] {duration.total_seconds():.1f}s"
    )
    console.print("\n")
    console.print(Panel(summary, title="[bold green]✓ Sincronização Finalizada[/]", expand=False))

    # Surface a non-zero exit when anything went wrong so the workflow's
    # `report-failure` job fires. Consolidation errors and per-month
    # extraction errors are both sync-level failures.
    if consolidation_error is not None or error_count > 0:
        raise typer.Exit(1)


@app.command("verify")
def verify(
    resource: str = typer.Option("contratos", "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
) -> None:
    """Verify data coverage by checking the REMOTE IA manifest."""
    try:
        _validate_resource(resource)
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        # end_date is used for validation during parsing
        datetime.strptime(end, "%Y-%m-%d").date()

        # We need an engine even to just check the uploader manifest
        engine = BalizaEngine()
        uploader = IAUploader(engine)
        with console.status("[bold green]Checking remote manifest...[/bold green]"):
            raw_manifest = uploader._read_manifest_from_ia()
            uploaded_months = {row["data_particao"] for row in raw_manifest if row.get("data_particao")}

        gaps = []
        curr = start_date.replace(day=1)
        # Verify months up to previous month
        today = date.today()
        last_month_end = (today.replace(day=1) - timedelta(days=1)).replace(day=1)

        while curr <= last_month_end:
            month_key = curr.strftime("%Y-%m")
            if month_key not in uploaded_months:
                gaps.append(month_key)
            
            # Next month
            if curr.month == 12:
                curr = curr.replace(year=curr.year + 1, month=1)
            else:
                curr = curr.replace(month=curr.month + 1)

        if not gaps:
            console.print(f"[green]✓ Complete month coverage from {start} to {last_month_end.strftime('%Y-%m')} on Internet Archive.")
        else:
            console.print(f"[yellow]⚠ Found {len(gaps)} missing month(s) on IA:")
            for m in gaps[:12]:
                console.print(f"  • {m}")
            if len(gaps) > 12:
                console.print(f"  ... and {len(gaps) - 12} more.")

    except ValueError as e:
        print(str(e))
        sys.stdout.flush()
        raise typer.Exit(1) from None
    except Exception as e:
        console.print(f"[red]✗ Verify failed: {e}")
        raise typer.Exit(1) from None


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
