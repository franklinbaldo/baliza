"""Simplified CLI using direct extraction.

STATELESS: manifest.csv on Internet Archive is the source of truth.
SHARED ENGINE: Uses a single DuckDB session for extraction and upload.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog
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

from .builder import _pending_build_months, build_month
from .consolidator import IAConsolidator
from .constants import (
    RESOURCE_CONTRATOS,
    clamp_to_known_data_start_month,
    known_data_start_month,
    month_predates_known_data,
)
from .engine import BalizaEngine
from .extractor import FETCHED_SENTINEL, PNCPExtractor, _validate_resource
from .ia_uploader import IAUploader, read_manifest_from_ia, restore_from_raw_zip
from .logging import configure_logging
from .mirror import _pending_mirror_months, mirror_month
from .resources import first_page_filename, page_filename

logger = structlog.get_logger()

app = typer.Typer()
console = Console()


def _forced_month_or_empty(resource: str, force_month: str) -> list[date]:
    """Return a one-month batch, or an intentional no-op for pre-history months."""
    forced = datetime.strptime(force_month, "%Y-%m").date()
    if month_predates_known_data(resource, forced):
        console.print(
            f"[yellow]⚠ Requested month predates known PNCP {resource} data "
            f"({known_data_start_month(resource).strftime('%Y-%m')}), skipping.[/yellow]"
        )
        return []
    return [forced]


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
    start_date: str = typer.Option("2021-09-01", "--start-date", help="Oldest date to backfill"),
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
        _validate_resource(RESOURCE_CONTRATOS)
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
    manifest_by_month: dict[str, dict] = {}
    if force_month:
        batch = _forced_month_or_empty(RESOURCE_CONTRATOS, force_month)
        uploaded: set[str] = set()
    else:
        with console.status("[bold green]Checking IA manifest for pending months...[/bold green]"):
            try:
                # manifest dates in monthly strategy are strings "YYYY-MM"
                raw_manifest = uploader._read_manifest_from_ia()
                # A month is "done" only when its Parquet is on IA AND we have a sha256
                # to prove the upload was confirmed. parquet_url alone is not sufficient:
                # old manifest-recovery runs wrote the expected URL before the file existed,
                # leaving entries that 404 and cause the consolidator to skip the whole year.
                uploaded = {
                    row["data_particao"]
                    for row in raw_manifest
                    if row.get("data_particao") and row.get("parquet_url") and row.get("sha256")
                }
                # Lookup for raw_zip_url by month — used to skip PNCP fetch
                # when we already have the ZIP on IA for a past month.
                manifest_by_month: dict[str, dict] = {
                    row["data_particao"]: row for row in raw_manifest if row.get("data_particao")
                }
            except Exception as e:
                console.print(
                    f"[yellow]⚠ Could not read IA manifest (starting fresh): {e}[/yellow]"
                )
                uploaded = set()
                manifest_by_month = {}

            start = clamp_to_known_data_start_month(
                RESOURCE_CONTRATOS, datetime.strptime(start_date, "%Y-%m-%d").date()
            )

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

            def process_month_full(start_of_month: date):  # noqa: PLR0912, PLR0915
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
                    raw_month_dir = Path("data/raw") / month_str
                    sentinel = raw_month_dir / FETCHED_SENTINEL
                    month_start_dt = datetime.combine(start_of_month, datetime.min.time())
                    month_end_dt = datetime.combine(end_of_month, datetime.min.time())

                    with PNCPExtractor(thread_engine, use_curl=not no_curl) as extractor:
                        # Validate each cached page on disk. `exists()` alone
                        # would mis-count a zero-byte or corrupt file as
                        # cached — we'd then skip fetch_page for that page,
                        # write the sentinel, and let ingest_range silently
                        # unlink the bad file, ending up with a month
                        # uploaded with missing records. So validate the
                        # cache here and unlink anything broken so
                        # fetch_page sees a clean refetch.
                        def _page_is_cached(p: int) -> bool:
                            path = raw_month_dir / page_filename(RESOURCE_CONTRATOS, p)
                            if not path.exists() or path.stat().st_size == 0:
                                return False
                            try:
                                with open(path) as fh:
                                    data = json.load(fh)
                            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                                logger.warning(
                                    "corrupt_cache_found",
                                    file=str(path),
                                    error=str(e),
                                )
                                try:
                                    path.unlink()
                                except OSError:
                                    pass
                                return False
                            # Parse success alone isn't enough — a cached
                            # `{}` would silently satisfy exists() + load()
                            # but produce zero rows during ingest, and the
                            # month would still write the sentinel and
                            # upload with missing records.
                            if isinstance(data, dict) and (
                                "data" in data or "totalPaginas" in data
                            ):
                                return True
                            logger.warning(
                                "corrupt_cache_found",
                                file=str(path),
                                error="schema mismatch (no 'data' or 'totalPaginas' key)",
                            )
                            try:
                                path.unlink()
                            except OSError:
                                pass
                            return False

                        # For past months: if the raw ZIP is already on IA,
                        # restore from it instead of re-fetching from PNCP.
                        # This must happen BEFORE the sentinel/probe checks below,
                        # so that an outage in PNCP doesn't block the restore path.
                        today_month = date.today().replace(day=1)
                        manifest_row = manifest_by_month.get(month_str, {})
                        raw_zip_url = manifest_row.get("raw_zip_url", "")

                        # A full cache miss can be approximated by lacking page 1
                        is_full_miss = not _page_is_cached(1)

                        if raw_zip_url and start_of_month < today_month and is_full_miss:
                            progress.update(
                                tid,
                                total=2,
                                advance=1,
                                description=f"Month {month_str} [Restoring from IA]",
                            )
                            restore_from_raw_zip(raw_zip_url, raw_month_dir)

                        # Sentinel optimisation: a prior run wrote .fetched
                        # after fetching all expected pages, so we can read
                        # totalPaginas from the cached page 1 and skip the
                        # API probe. The sentinel only gates the API call,
                        # NOT the per-page validation below — a gap in the
                        # cache (e.g. ingest_range unlinked a corrupt page
                        # between runs) must still be detected and refetched
                        # instead of silently uploading an incomplete month.
                        total_pages: int | None = None
                        if sentinel.exists():
                            p1 = raw_month_dir / first_page_filename(RESOURCE_CONTRATOS)
                            regression_reason: str | None = None
                            if not p1.exists() or p1.stat().st_size == 0:
                                regression_reason = "page1_missing"
                            else:
                                try:
                                    with open(p1) as fh:
                                        _d = json.load(fh)
                                except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                                    logger.warning(
                                        "page1_corrupt_unreadable", path=str(p1), error=str(e)
                                    )
                                    regression_reason = "page1_corrupt"
                                else:
                                    if isinstance(_d, dict) and isinstance(
                                        _d.get("totalPaginas"), int
                                    ):
                                        total_pages = _d["totalPaginas"]
                                    else:
                                        regression_reason = "page1_schema_invalid"
                            if total_pages is None:
                                # Sentinel exists but page 1 is gone/bad.
                                # Drop the sentinel and fall back to probe.
                                logger.warning(
                                    "sentinel_cache_regressed",
                                    month=month_str,
                                    reason=regression_reason,
                                )
                                try:
                                    sentinel.unlink()
                                except OSError:
                                    pass

                        if total_pages is None:
                            res = extractor.probe_range(
                                RESOURCE_CONTRATOS, month_start_dt, month_end_dt
                            )
                            total_pages = res["total_pages"]

                        missing_pages = [
                            p for p in range(1, total_pages + 1) if not _page_is_cached(p)
                        ]
                        cached_pages = total_pages - len(missing_pages)

                        if not missing_pages:
                            logger.info(
                                "month_cache_hit",
                                month=month_str,
                                source="all_pages_present",
                                pages=total_pages,
                            )
                            progress.update(
                                tid,
                                total=2,
                                advance=1,
                                description=f"Month {month_str} [Cache hit]",
                            )
                        else:
                            # A sentinel-path regression (cache gap after
                            # sentinel was written) is the interesting case
                            # to log — flag it so operators can investigate.
                            if sentinel.exists():
                                logger.warning(
                                    "sentinel_cache_regressed",
                                    month=month_str,
                                    reason="missing_pages",
                                    pages_missing=len(missing_pages),
                                    pages_total=total_pages,
                                )
                                try:
                                    sentinel.unlink()
                                except OSError:
                                    pass
                            logger.info(
                                "month_cache_partial" if cached_pages else "month_cache_miss",
                                month=month_str,
                                pages_cached=cached_pages,
                                pages_total=total_pages,
                                pages_to_fetch=len(missing_pages),
                            )

                            if missing_pages:
                                progress.update(
                                    tid,
                                    total=len(missing_pages) + 2,
                                    advance=1,
                                    description=f"Month {month_str} [Pages 0/{len(missing_pages)}]",
                                )
                                for i, p in enumerate(missing_pages, start=1):
                                    progress.update(
                                        tid,
                                        description=f"Month {month_str} [Pages {i}/{len(missing_pages)}]",
                                    )
                                    extractor.fetch_page(
                                        RESOURCE_CONTRATOS, month_start_dt, month_end_dt, p
                                    )
                                    progress.update(tid, advance=1)

                        # All expected pages are now on disk — write the
                        # sentinel so the next run can skip probe_range.
                        # IAUploader.upload_month rmtrees the whole
                        # directory on success, taking this with it.
                        raw_month_dir.mkdir(parents=True, exist_ok=True)
                        sentinel.touch()

                        # 3b. Upload raw ZIP to baliza-pncp-raw before ingest.
                        # keep_raw_dir=True so pages stay on disk for ingest_range.
                        if not dry_run and ia_access_key and ia_secret_key:
                            progress.update(tid, description=f"Month {month_str} [Raw ZIP]")
                            try:
                                uploader.upload_raw_zip(
                                    start_of_month,
                                    raw_month_dir,
                                    ia_access_key,
                                    ia_secret_key,
                                    keep_raw_dir=True,
                                )
                            except Exception as e:
                                progress.console.log(
                                    f"[yellow]⚠ Raw ZIP upload failed for {month_str}: {e}[/yellow]"
                                )

                        # 4. Ingest
                        progress.update(tid, description=f"Month {month_str} [Ingesting]")
                        stats = extractor.ingest_range(month_start_dt)
                        total_records += stats.get("valid", 0)
                        quarantine_count += stats.get("quarantine", 0)
                        progress.update(tid, advance=1)

                        # 5. Export & Upload
                        q_csv = Path(f"data/quarentena-{month_str}.csv")
                        has_q = extractor.export_quarantine(month_start_dt, q_csv)

                        if not dry_run:
                            progress.update(tid, description=f"Month {month_str} [Uploading]")
                            if ia_access_key and ia_secret_key:
                                # Use thread_engine so export reads from the same
                                # in-memory DB that ingest_range just populated.
                                thread_uploader = IAUploader(thread_engine)
                                thread_uploader.upload_month(
                                    start_of_month,
                                    Path("data/processed"),
                                    ia_access_key,
                                    ia_secret_key,
                                    quarantine_stats=stats,
                                    quarantine_csv=q_csv if has_q else None,
                                )
                        else:
                            progress.console.log(
                                f"[yellow]Dry-run: {month_str} verified ({stats['valid']} records)[/yellow]"
                            )

                        # Step complete
                        progress.update(tid, advance=1)

                        if q_csv.exists():
                            q_csv.unlink()

                    progress.update(overall_task, advance=1)
                except ValueError as e:
                    # Resource validation now raises 'unknown resource …'
                    # (registry miss) or 'Invalid resource_name …'
                    # (registration-time charset check). Surface either
                    # before re-raising so the operator sees what went
                    # wrong on the way out of the progress loop.
                    msg = str(e)
                    if "unknown resource" in msg or "Invalid resource_name" in msg:
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
                    progress.console.log(
                        f"[yellow]⚠ Time limit ({limit_minutes}m) reached.[/yellow]"
                    )
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

    # 4b. POST-SYNC FEED PUBLISH
    #
    # Curated RSS feeds (see baliza.rss_feed.CURATED_WATCHES) need a snapshot
    # that spans every recent month, not just whichever month a worker thread
    # happened to ingest. Run once here, after every per-month future has
    # drained, against a fresh in-memory engine that views the canonical IA
    # parquets directly via DuckDB's httpfs reader. Best-effort: a feed
    # failure must not flip the sync's exit code red because the data was
    # already published successfully above.
    if not dry_run and ia_access_key and ia_secret_key:
        try:
            manifest_rows = read_manifest_from_ia()
            # Manifest is persisted in write order, but sync runs months in
            # parallel and reverse-chronologically — slicing without sorting
            # would pick arbitrary partitions. Sort by data_particao (a
            # `YYYY-MM` string sorts correctly lexicographically) so the cap
            # at the tail always reflects the most recent months.
            canonical_rows = [
                r
                for r in manifest_rows
                if r.get("parquet_url")
                and r.get("table_name") == "contratos"
                and r.get("file_type", "monthly_canonical") == "monthly_canonical"
                and r.get("data_particao")
            ]
            canonical_rows.sort(key=lambda r: r["data_particao"])
            # Cap at the most recent 12 months so the feed view doesn't pull
            # the entire history into memory just to read the LIMIT 50 head.
            parquet_urls = [r["parquet_url"] for r in canonical_rows[-12:]]
            if parquet_urls:
                feed_engine = BalizaEngine()
                try:
                    feed_engine.con.raw_sql("INSTALL httpfs; LOAD httpfs;")
                    url_list = ", ".join(f"'{u}'" for u in parquet_urls)
                    feed_engine.con.raw_sql(
                        f"CREATE OR REPLACE VIEW main.contratos AS "
                        f"SELECT * FROM read_parquet([{url_list}], union_by_name = true)"
                    )
                    published = IAUploader(feed_engine).upload_feeds(
                        ia_access_key, ia_secret_key
                    )
                    console.print(
                        f"[green]✓ Curated feeds published: {len(published)}[/green]"
                    )
                finally:
                    try:
                        feed_engine.con.disconnect()
                    except Exception:
                        pass
            else:
                console.print(
                    "[yellow]⚠ Manifest has no parquet rows yet — skipping feed publish[/yellow]"
                )
        except Exception as e:
            console.print(f"[yellow]⚠ Feed publish failed: {e}[/yellow]")

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


@app.command("mirror")
def mirror_cmd(  # noqa: PLR0913, PLR0915
    batch_size: int | None = typer.Option(
        None, "--batch-size", "-n", help="Max months to mirror (None for all)"
    ),
    start_date: str = typer.Option("2021-09-01", "--start-date", help="Oldest date to backfill"),
    force_month: str | None = typer.Option(
        None, "--force-month", help="Target a specific month (YYYY-MM) regardless of manifest"
    ),
    limit_minutes: int = typer.Option(
        0, "--limit-minutes", help="Stop after this many minutes (0 = no limit)"
    ),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel workers"),
    no_curl: bool = typer.Option(False, "--no-curl", help="Opt-out of system cURL"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Fetch only, skip upload"),
    resource: str = typer.Option(
        RESOURCE_CONTRATOS, "--resource", "-r", help="PNCP resource to mirror"
    ),
) -> None:
    """Fetch PNCP JSON pages and upload monthly ZIPs to IA (no DuckDB, no Parquet).

    Phase 1 of the two-phase pipeline.  Use 'build' to turn the ZIPs into Parquet.
    """
    start_time_exec = datetime.now()
    ia_access_key = os.environ.get("IA_ACCESS_KEY") or os.environ.get("IAS3_ACCESS_KEY")
    ia_secret_key = os.environ.get("IA_SECRET_KEY") or os.environ.get("IAS3_SECRET_KEY")

    if not dry_run and (not ia_access_key or not ia_secret_key):
        console.print("[red]✗ Missing IA keys in environment.[/red]")
        raise typer.Exit(1)

    try:
        _validate_resource(resource)
    except ValueError as e:
        console.print(f"[red]✗ {e}[/red]")
        raise typer.Exit(1) from None

    if force_month:
        batch = _forced_month_or_empty(resource, force_month)
    else:
        start = clamp_to_known_data_start_month(
            resource, datetime.strptime(start_date, "%Y-%m-%d").date()
        )
        try:
            with console.status(
                "[bold green]Checking IA manifest for pending months...[/bold green]"
            ):
                batch = _pending_mirror_months(start, batch_size, resource=resource)
        except Exception as e:
            console.print(f"[red]✗ Cannot read IA manifest: {e}[/red]")
            raise typer.Exit(1) from None

    if not batch:
        console.print("[green]✓ All months already mirrored.[/green]")
        return

    console.print(f"[cyan]Mirror: {len(batch)} month(s) to process[/cyan]")

    total_fetched = 0
    error_count = 0

    def _collect_mirror(f: concurrent.futures.Future, m: Any) -> None:
        nonlocal total_fetched, error_count
        try:
            r = f.result()
            total_fetched += int(r.get("pages_fetched", 0))
            if not dry_run and not r.get("uploaded"):
                error_count += 1
                console.print(
                    f"[red]✗ {m.strftime('%Y-%m')}: upload failed (manifest error?)[/red]"
                )
        except Exception as e:
            error_count += 1
            console.print(f"[red]✗ {m.strftime('%Y-%m')}: {e}[/red]")

    current_month = date.today().replace(day=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[concurrent.futures.Future, Any] = {}
        for target_month in batch:
            elapsed = (datetime.now() - start_time_exec).total_seconds() / 60
            if limit_minutes and elapsed >= limit_minutes:
                console.print(f"[yellow]⚠ Time limit ({limit_minutes}m) reached.[/yellow]")
                break
            while len(futures) >= workers:
                done, _ = concurrent.futures.wait(
                    futures.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for completed in done:
                    _collect_mirror(completed, futures.pop(completed))
            f = executor.submit(
                mirror_month,
                target_month,
                ia_access_key=ia_access_key or "",
                ia_secret_key=ia_secret_key or "",
                use_curl=not no_curl,
                dry_run=dry_run,
                log_fn=lambda msg: console.log(f"[dim]{msg}[/dim]"),
                is_current_month=(target_month == current_month),
                resource=resource,
            )
            futures[f] = target_month
        concurrent.futures.wait(futures.keys())
        for f, m in list(futures.items()):
            _collect_mirror(f, m)

    console.print(
        f"\n[green]✓ Mirror done[/green] — {total_fetched} pages fetched, "
        f"{error_count} errors, {(datetime.now() - start_time_exec).total_seconds():.1f}s"
    )
    if error_count:
        raise typer.Exit(1)


@app.command("build")
def build_cmd(  # noqa: PLR0913, PLR0915
    batch_size: int | None = typer.Option(
        None, "--batch-size", "-n", help="Max months to build (None for all)"
    ),
    start_date: str = typer.Option("2021-09-01", "--start-date", help="Oldest date to backfill"),
    force_month: str | None = typer.Option(
        None, "--force-month", help="Target a specific month (YYYY-MM) regardless of manifest"
    ),
    limit_minutes: int = typer.Option(
        0, "--limit-minutes", help="Stop after this many minutes (0 = no limit)"
    ),
    workers: int = typer.Option(
        2, "--workers", "-w", help="Parallel workers (DuckDB is CPU-heavy)"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Ingest only, skip upload"),
    backfill: bool = typer.Option(
        False, "--backfill", help="Rebuild all months with outdated schema version"
    ),
    resource: str = typer.Option(
        RESOURCE_CONTRATOS, "--resource", "-r", help="PNCP resource to build"
    ),
) -> None:
    """Download raw ZIPs from IA, ingest, and upload Parquet (Phase 2 of two-phase pipeline).

    Normal mode: processes months that were mirrored but not yet built.
    --backfill: rebuilds all months whose parquet_schema_version differs from the current code.
    """
    start_time_exec = datetime.now()
    ia_access_key = os.environ.get("IA_ACCESS_KEY") or os.environ.get("IAS3_ACCESS_KEY")
    ia_secret_key = os.environ.get("IA_SECRET_KEY") or os.environ.get("IAS3_SECRET_KEY")

    if not dry_run and (not ia_access_key or not ia_secret_key):
        console.print("[red]✗ Missing IA keys in environment.[/red]")
        raise typer.Exit(1)

    try:
        _validate_resource(resource)
    except ValueError as e:
        console.print(f"[red]✗ {e}[/red]")
        raise typer.Exit(1) from None

    # Fetch manifest once — reused by _pending_build_months and each build_month call
    # to avoid one network round-trip per month when building in batch.
    try:
        with console.status("[bold green]Checking IA manifest for months to build...[/bold green]"):
            build_manifest = read_manifest_from_ia()
    except Exception as e:
        console.print(f"[red]✗ Cannot read IA manifest: {e}[/red]")
        raise typer.Exit(1) from None

    if force_month:
        batch = _forced_month_or_empty(resource, force_month)
    else:
        start = clamp_to_known_data_start_month(
            resource, datetime.strptime(start_date, "%Y-%m-%d").date()
        )
        batch = _pending_build_months(
            start,
            batch_size,
            resource=resource,
            backfill=backfill,
            manifest=build_manifest,
        )

    if not batch:
        console.print("[green]✓ Nothing to build.[/green]")
        return

    mode = "backfill" if backfill else "build"
    console.print(f"[cyan]{mode.capitalize()}: {len(batch)} month(s) to process[/cyan]")

    total_valid = 0
    total_quarantine = 0
    error_count = 0

    def _collect_build(f: concurrent.futures.Future, m: Any) -> None:
        nonlocal total_valid, total_quarantine, error_count
        try:
            r = f.result()
            total_valid += int(r.get("valid", 0))
            total_quarantine += int(r.get("quarantine", 0))
            if not dry_run and not r.get("uploaded"):
                error_count += 1
                console.print(
                    f"[red]✗ {m.strftime('%Y-%m')}: upload failed (manifest error?)[/red]"
                )
        except Exception as e:
            error_count += 1
            console.print(f"[red]✗ {m.strftime('%Y-%m')}: {e}[/red]")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures: dict[concurrent.futures.Future, Any] = {}
        for target_month in batch:
            elapsed = (datetime.now() - start_time_exec).total_seconds() / 60
            if limit_minutes and elapsed >= limit_minutes:
                console.print(f"[yellow]⚠ Time limit ({limit_minutes}m) reached.[/yellow]")
                break
            while len(futures) >= workers:
                done, _ = concurrent.futures.wait(
                    futures.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for completed in done:
                    _collect_build(completed, futures.pop(completed))
            f = executor.submit(
                build_month,
                target_month,
                ia_access_key=ia_access_key or "",
                ia_secret_key=ia_secret_key or "",
                dry_run=dry_run,
                log_fn=lambda msg: console.log(f"[dim]{msg}[/dim]"),
                manifest=build_manifest,
                resource=resource,
            )
            futures[f] = target_month
        concurrent.futures.wait(futures.keys())
        for f, m in list(futures.items()):
            _collect_build(f, m)

    console.print(
        f"\n[green]✓ Build done[/green] — {total_valid:,} records, "
        f"{total_quarantine:,} quarantined, {error_count} errors, "
        f"{(datetime.now() - start_time_exec).total_seconds():.1f}s"
    )
    if error_count:
        raise typer.Exit(1)


@app.command("verify")
def verify(
    resource: str = typer.Option(RESOURCE_CONTRATOS, "--resource", "-r", help="Resource to verify"),
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
) -> None:
    """Verify data coverage by checking the REMOTE IA manifest."""
    try:
        _validate_resource(resource)
        start_date = clamp_to_known_data_start_month(
            resource, datetime.strptime(start, "%Y-%m-%d").date()
        )
        # end_date is used for validation during parsing
        datetime.strptime(end, "%Y-%m-%d").date()

        # We need an engine even to just check the uploader manifest
        engine = BalizaEngine()
        uploader = IAUploader(engine)
        with console.status("[bold green]Checking remote manifest...[/bold green]"):
            raw_manifest = uploader._read_manifest_from_ia()
            uploaded_months = {
                row["data_particao"] for row in raw_manifest if row.get("data_particao")
            }

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
            console.print(
                f"[green]✓ Complete month coverage from {start} to {last_month_end.strftime('%Y-%m')} on Internet Archive."
            )
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


@app.command("doctor")
def doctor(  # noqa: PLR0912, PLR0915
    resource: str = typer.Option(RESOURCE_CONTRATOS, "--resource", "-r", help="Resource to check"),
    start: str = typer.Option("2021-01-01", "--start", help="Earliest month to check (YYYY-MM-DD)"),
    head_check: bool = typer.Option(
        False,
        "--head-check/--no-head-check",
        help="HTTP HEAD every parquet_url and raw_zip_url. Slower but catches deleted IA files.",
    ),
) -> None:
    """Read-only health check on the live IA manifest.

    Reports any of: missing months, malformed manifest rows, broken URLs,
    invalid sha256, table_name mismatch with --resource. Exits 1 on any
    finding so it can drive a CI alert.
    """
    try:
        _validate_resource(resource)
        effective_start = clamp_to_known_data_start_month(
            resource, datetime.strptime(start, "%Y-%m-%d").date()
        )
    except ValueError as exc:
        console.print(f"[red]✗ {exc}[/red]")
        raise typer.Exit(1) from None

    try:
        rows = read_manifest_from_ia()
    except Exception as exc:
        console.print(f"[red]✗ Could not read manifest: {exc}[/red]")
        raise typer.Exit(1) from None

    findings: list[str] = []
    canonical = [
        r for r in rows
        if r.get("table_name") == resource
        and (r.get("file_type") or "monthly_canonical") == "monthly_canonical"
    ]
    months_present = {r["data_particao"] for r in canonical if r.get("data_particao")}

    # Gap check (same window as `verify`).
    today = date.today()
    last_month_end = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    curr = effective_start.replace(day=1)
    expected: list[str] = []
    while curr <= last_month_end:
        expected.append(curr.strftime("%Y-%m"))
        curr = curr.replace(year=curr.year + 1, month=1) if curr.month == 12 else curr.replace(month=curr.month + 1)
    gaps = [m for m in expected if m not in months_present]
    if gaps:
        findings.append(f"missing canonical months for {resource}: {len(gaps)} (e.g. {gaps[:5]})")

    # Per-row schema sanity.
    sha_re = re.compile(r"^[0-9a-f]{64}$")
    for r in canonical:
        rid = f"{r.get('table_name','?')}/{r.get('data_particao','?')}"
        sha = r.get("sha256") or ""
        if not sha:
            findings.append(f"{rid}: missing sha256")
        elif not sha_re.match(sha):
            findings.append(f"{rid}: sha256 not 64-hex ({sha!r})")
        for url_col in ("parquet_url", "raw_zip_url"):
            u = r.get(url_col) or ""
            if not u:
                continue
            host = urlparse(u).netloc
            # Require an exact archive.org host or a true subdomain so
            # 'evilarchive.org' doesn't pass the integrity check.
            on_ia = u.startswith("https://") and (
                host == "archive.org" or host.endswith(".archive.org")
            )
            if not on_ia:
                findings.append(f"{rid}: {url_col} not on archive.org ({u!r})")
        if not r.get("parquet_url"):
            findings.append(f"{rid}: missing parquet_url")
        if not r.get("raw_zip_url"):
            findings.append(f"{rid}: missing raw_zip_url")
        # uf_sigla must be empty on canonical rows.
        if r.get("uf_sigla"):
            findings.append(f"{rid}: canonical row has uf_sigla={r['uf_sigla']!r}")

    # Optional: probe URLs with HEAD to catch deleted IA files.
    if head_check:
        # Retry transient HEAD failures so a 1s IA blip doesn't cry wolf.
        # 3 attempts. A genuine deletion is still surfaced because IA
        # returns 4xx, not a transport error.
        transport = httpx.HTTPTransport(retries=3)
        with httpx.Client(
            follow_redirects=True, timeout=15.0, transport=transport
        ) as client:
            for r in canonical:
                rid = f"{r.get('table_name','?')}/{r.get('data_particao','?')}"
                for url_col in ("parquet_url", "raw_zip_url"):
                    u = r.get(url_col) or ""
                    if not u:
                        continue
                    # Defense in depth: never let doctor make outbound
                    # requests to a host the manifest validator already
                    # rejected. A malicious/accidental external URL
                    # would otherwise turn the scheduled cron into an
                    # SSRF probe from the GHA runner.
                    host = urlparse(u).netloc
                    if not (
                        u.startswith("https://")
                        and (host == "archive.org" or host.endswith(".archive.org"))
                    ):
                        continue
                    try:
                        resp = client.head(u)
                    except Exception as exc:
                        findings.append(f"{rid}: {url_col} HEAD failed ({exc})")
                        continue
                    if resp.status_code >= 400:
                        findings.append(f"{rid}: {url_col} HEAD {resp.status_code}")

    if not findings:
        console.print(
            f"[green]✓ {resource}: {len(canonical)} canonical month(s), "
            f"{effective_start.strftime('%Y-%m')} .. {last_month_end.strftime('%Y-%m')}, "
            f"no findings.[/green]"
        )
        return

    shown = min(len(findings), 50)
    console.print(
        f"[red]✗ {resource}: {len(findings)} finding(s) "
        f"(showing first {shown})[/red]"
    )
    for f in findings[:50]:
        console.print(f"  • {f}")
    if len(findings) > 50:
        console.print(f"  ... and {len(findings) - 50} more.")
    raise typer.Exit(1)


@app.command("orphans")
def orphans(
    resource: str = typer.Option(RESOURCE_CONTRATOS, "--resource", "-r", help="Resource to check"),
    months: int = typer.Option(3, "--months", help="How many recent months to inspect"),
) -> None:
    """List files inside per-month IA items that aren't in the manifest.

    Read-only. Surfaces legacy daily/per-supplier parquets left over by
    older pipeline shapes (the live audit found ~28 stale files per
    month in baliza-pncp-{YYYY-MM}). Garbage collection is intentionally
    a separate, destructive command — do not add it here without a
    --apply flag and explicit IA credentials.
    """
    try:
        rows = read_manifest_from_ia()
    except Exception as exc:
        console.print(f"[red]✗ Could not read manifest: {exc}[/red]")
        raise typer.Exit(1) from None

    canonical = [
        r for r in rows
        if r.get("table_name") == resource
        and (r.get("file_type") or "monthly_canonical") == "monthly_canonical"
    ]
    canonical.sort(key=lambda r: r.get("data_particao", ""), reverse=True)
    canonical = canonical[:months]
    if not canonical:
        console.print(f"[yellow]No canonical rows for {resource}.[/yellow]")
        raise typer.Exit(1)

    expected_per_item = {
        # Files the current pipeline is allowed to produce per IA monthly
        # item — see IAUploader.upload_month in src/baliza/ia_uploader.py.
        "{resource}-{month}.parquet",
        "quarentena-{month}.csv",
        "raw-{month}.zip",
    }

    total_orphans = 0
    fetch_failures = 0
    transport = httpx.HTTPTransport(retries=3)
    with httpx.Client(timeout=30.0, transport=transport) as client:
        for r in canonical:
            month = r["data_particao"]
            item_id = r.get("ia_item_id") or f"baliza-pncp-{month}"
            try:
                meta = client.get(f"https://archive.org/metadata/{item_id}").json()
            except Exception as exc:
                console.print(f"[red]{item_id}: metadata fetch failed: {exc}[/red]")
                fetch_failures += 1
                continue
            allowed = {
                t.format(resource=resource, month=month) for t in expected_per_item
            } | {
                # IA-managed sidecars never count as orphans.
                f"{item_id}_archive.torrent",
                f"{item_id}_files.xml",
                f"{item_id}_meta.sqlite",
                f"{item_id}_meta.xml",
            }
            files = [f["name"] for f in meta.get("files", [])]
            orphan_files = [f for f in files if f not in allowed]
            if not orphan_files:
                console.print(f"[green]{item_id}: clean ({len(files)} files)[/green]")
                continue
            total_orphans += len(orphan_files)
            console.print(f"[yellow]{item_id}: {len(orphan_files)} orphan(s)[/yellow]")
            for f in orphan_files:
                console.print(f"  • {f}")

    console.print(
        f"\n[bold]{total_orphans} orphan file(s) across "
        f"{len(canonical) - fetch_failures}/{len(canonical)} month(s) "
        f"inspected.[/bold]"
    )
    if fetch_failures:
        console.print(
            f"[red]✗ {fetch_failures} month(s) could not be inspected; "
            "result is partial.[/red]"
        )
    # Treat fetch failures as orphan-like: a partial scan must not look
    # green because callers (CI) cannot tell it apart from a true clean.
    if total_orphans or fetch_failures:
        raise typer.Exit(1)
