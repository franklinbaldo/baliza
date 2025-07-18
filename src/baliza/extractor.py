"""PNCP Data Extractor V2 - True Async Architecture

Implements the resilient task-driven extraction architecture defined in ADR-002.
Uses DuckDB for persistence as specified in ADR-001.
Built with modern Python toolchain (httpx, tenacity) per ADR-005.

Architecture:
- Task-driven extraction with 4 phases (Planning, Discovery, Execution, Reconciliation)
- Fault-tolerant with persistent state in DuckDB control table
- Async/concurrent execution for performance
"""

import asyncio
import calendar
import json
import logging
import re
import signal
import sys
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

# Import configuration from the config module
from baliza.config import settings
from baliza.pncp_client import PNCPClient
from baliza.pncp_task_planner import PNCPTaskPlanner
from baliza.pncp_writer import BALIZA_DB_PATH, DATA_DIR, PNCPWriter, connect_utf8

# Configure standard logging com UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

console = Console(force_terminal=True, legacy_windows=False, stderr=False)
logger = logging.getLogger(__name__)


# JSON parsing moved to utils.py to avoid circular imports


class AsyncPNCPExtractor:
    """True async PNCP extractor with semaphore back-pressure."""

    def __init__(self, concurrency: int = settings.concurrency):
        self.concurrency = concurrency
        self.client = PNCPClient(concurrency=concurrency)
        self.task_planner = PNCPTaskPlanner()  # Instantiate the task planner
        self.writer = PNCPWriter()  # Instantiate the writer
        self.run_id = str(uuid.uuid4())

        # Statistics
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_records = 0

        # Queue-based processing
        queue_size = max(32, concurrency * 10)
        self.page_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(
            maxsize=queue_size
        )
        self.writer_running = False

        # Graceful shutdown handling
        self.shutdown_event = asyncio.Event()
        self.running_tasks = set()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown - Windows compatible."""

        def signal_handler(signum, frame):
            console.print(
                "\n⚠️ [yellow]Received Ctrl+C, initiating graceful shutdown...[/yellow]"
            )
            self.shutdown_event.set()
            # Cancel all running tasks
            for task in self.running_tasks:
                if not task.done():
                    task.cancel()

        # Windows-compatible signal handlers
        try:
            if hasattr(signal, "SIGINT"):
                signal.signal(signal.SIGINT, signal_handler)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, signal_handler)
        except (ValueError, AttributeError) as e:
            logger.warning(f"Could not setup signal handlers: {e}")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.client.__aenter__()
        await self.writer.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        await self.writer.__aexit__(exc_type, exc_val, exc_tb)
        await self._graceful_shutdown()

    async def _graceful_shutdown(self):
        """Graceful shutdown of all connections and resources."""
        try:
            # Signal writer to stop gracefully
            if hasattr(self, "writer_running") and self.writer_running:
                await self.page_queue.put(None)  # Send sentinel

            console.print(
                "✅ [bold green]Graceful shutdown completed successfully![/bold green]"
            )

        except Exception as e:
            console.print(f"⚠️ Shutdown error: {e}")

    async def _fetch_with_backpressure(
        self, url: str, params: dict[str, Any], task_id: str | None = None
    ) -> dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        self.total_requests += 1
        response = await self.client.fetch_with_backpressure(url, params, task_id)

        if response["success"]:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        return response

    async def _plan_tasks(self, start_date: date, end_date: date):
        """Phase 1: Populate the control table with all necessary tasks."""
        console.print("Phase 1: Planning tasks...")
        tasks_to_create = await self.task_planner.plan_tasks(start_date, end_date)

        if tasks_to_create:
            # Schema migration - update constraint to include modalidade
            try:
                # Check if we need to migrate the constraint
                existing_constraints = self.writer.conn.execute(
                    "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'pncp_extraction_tasks' AND constraint_type = 'UNIQUE'"
                ).fetchall()

                # If we have the old constraint, we need to migrate
                for constraint in existing_constraints:
                    if constraint[0] == "unique_task":
                        # Check if constraint includes modalidade
                        constraint_cols = self.writer.conn.execute(
                            "SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name = 'unique_task' AND table_name = 'pncp_extraction_tasks'"
                        ).fetchall()

                        if (
                            len(constraint_cols) == 2
                        ):  # Old constraint (endpoint_name, data_date)
                            # Drop old constraint and recreate with modalidade
                            self.writer.conn.execute(
                                "ALTER TABLE psa.pncp_extraction_tasks DROP CONSTRAINT unique_task"
                            )
                            self.writer.conn.execute(
                                "ALTER TABLE psa.pncp_extraction_tasks ADD CONSTRAINT unique_task UNIQUE (endpoint_name, data_date, modalidade)"
                            )
                            self.writer.conn.commit()
                            break

            except duckdb.Error:
                # If migration fails, continue - the new schema will handle it
                pass

            self.writer.conn.executemany(
                """
                INSERT INTO psa.pncp_extraction_tasks (task_id, endpoint_name, data_date, modalidade)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (task_id) DO NOTHING
                """,
                tasks_to_create,
            )
            self.writer.conn.commit()
        console.print(
            f"Planning complete. {len(tasks_to_create)} potential tasks identified."
        )

    async def _discover_tasks(self, progress: Progress):
        """Phase 2: Get metadata for all PENDING tasks."""
        pending_tasks = self.writer.conn.execute(
            "SELECT task_id, endpoint_name, data_date, modalidade FROM psa.pncp_extraction_tasks WHERE status = 'PENDING'"
        ).fetchall()

        if not pending_tasks:
            console.print("Phase 2: Discovery - No pending tasks to discover.")
            return

        discovery_progress = progress.add_task(
            "[cyan]Phase 2: Discovery", total=len(pending_tasks)
        )

        discovery_jobs = []
        for task_id, endpoint_name, data_date, modalidade in pending_tasks:
            # Mark as DISCOVERING
            self.writer.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status = 'DISCOVERING', updated_at = now() WHERE task_id = ?",
                [task_id],
            )

            endpoint = next(
                (ep for ep in settings.pncp_endpoints if ep["name"] == endpoint_name),
                None,
            )
            if not endpoint:
                continue

            # Respect minimum page size requirements
            page_size = endpoint.get("page_size", settings.page_size)
            min_page_size = endpoint.get("min_page_size", 1)
            actual_page_size = max(page_size, min_page_size)

            params = {
                "tamanhoPagina": actual_page_size,
                "pagina": 1,
            }
            if endpoint["supports_date_range"]:
                params[endpoint["date_params"][0]] = self.task_planner._format_date(
                    data_date
                )
                params[endpoint["date_params"][1]] = self.task_planner._format_date(
                    self.task_planner._monthly_chunks(data_date, data_date)[0][1]
                )
            elif endpoint.get("requires_single_date", False):
                # For single-date endpoints, use the data_date directly (should be end_date)
                # The data_date should already be correct (future date if needed) from task planning
                params[endpoint["date_params"][0]] = self.task_planner._format_date(
                    data_date
                )
            else:
                # For endpoints that don't support date ranges, use end of month chunk
                params[endpoint["date_params"][0]] = self.task_planner._format_date(
                    self.task_planner._monthly_chunks(data_date, data_date)[0][1]
                )

            # Add modalidade if this task has one
            if modalidade is not None:
                params["codigoModalidadeContratacao"] = modalidade

            discovery_jobs.append(
                self._fetch_with_backpressure(endpoint["path"], params, task_id=task_id)
            )

        self.writer.conn.commit()  # Commit status change to DISCOVERING

        for future in asyncio.as_completed(discovery_jobs):
            response = await future
            task_id = response.get("task_id")

            if response["success"]:
                total_records = response.get("total_records", 0)
                total_pages = response.get("total_pages", 1)

                # If total_pages is 0, it means no records, so it's 1 page of empty results.
                if total_pages == 0:
                    total_pages = 1

                missing_pages = list(range(2, total_pages + 1))

                self.writer.conn.execute(
                    """
                    UPDATE psa.pncp_extraction_tasks
                    SET status = ?, total_pages = ?, total_records = ?, missing_pages = ?, updated_at = now()
                    WHERE task_id = ?
                    """,
                    [
                        "FETCHING" if missing_pages else "COMPLETE",
                        total_pages,
                        total_records,
                        json.dumps(missing_pages),
                        task_id,
                    ],
                )

                # Enqueue page 1 response
                # Task ID format: {endpoint_name}_{YYYY-MM-DD} or {endpoint_name}_{YYYY-MM-DD}_modalidade_{N}
                # Since endpoint names can contain underscores, we need to extract the date part from the end
                match = re.match(
                    r"^(.+)_(\d{4}-\d{2}-\d{2})(?:_modalidade_\d+)?$", task_id
                )
                if match:
                    endpoint_name_part = match.group(1)
                    data_date_str = match.group(2)
                    data_date = datetime.fromisoformat(data_date_str).date()

                page_1_response = {
                    "endpoint_url": f"{settings.pncp_base_url}{response['url']}",
                    "endpoint_name": endpoint_name_part,
                    "request_parameters": response["params"],
                    "response_code": response["status_code"],
                    "response_content": response["content"],
                    "response_headers": response["headers"],
                    "data_date": data_date,
                    "run_id": self.run_id,
                    "total_records": total_records,  # This might not be accurate for pages > 1
                    "total_pages": total_pages,  # This might not be accurate for pages > 1
                    "current_page": 1,
                    "page_size": endpoint.get("page_size", settings.page_size),
                }
                await self.page_queue.put(page_1_response)

            else:
                error_message = f"HTTP {response.get('status_code')}: {response.get('error', 'Unknown')}"
                self.writer.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'FAILED', last_error = ?, updated_at = now() WHERE task_id = ?",
                    [error_message, task_id],
                )

            self.writer.conn.commit()
            progress.update(discovery_progress, advance=1)

    async def _fetch_page(
        self,
        endpoint_name: str,
        data_date: date,
        modalidade: int | None,
        page_number: int,
    ):
        """Helper to fetch a single page and enqueue it."""
        endpoint = next(
            (ep for ep in settings.pncp_endpoints if ep["name"] == endpoint_name), None
        )
        if not endpoint:
            return

        # Respect minimum page size requirements
        page_size = endpoint.get("page_size", settings.page_size)
        min_page_size = endpoint.get("min_page_size", 1)
        actual_page_size = max(page_size, min_page_size)

        params = {
            "tamanhoPagina": actual_page_size,
            "pagina": page_number,
        }

        if endpoint["supports_date_range"]:
            params[endpoint["date_params"][0]] = self.task_planner._format_date(
                data_date
            )
            params[endpoint["date_params"][1]] = self.task_planner._format_date(
                self.task_planner._monthly_chunks(data_date, data_date)[0][1]
            )
        elif endpoint.get("requires_single_date", False):
            # For single-date endpoints, use the data_date directly (should be end_date)
            # The data_date should already be correct (future date if needed) from task planning
            params[endpoint["date_params"][0]] = self.task_planner._format_date(
                data_date
            )
        else:
            # For endpoints that don't support date ranges, use end of month chunk
            params[endpoint["date_params"][0]] = self.task_planner._format_date(
                self.task_planner._monthly_chunks(data_date, data_date)[0][1]
            )

        # Add modalidade if this endpoint uses it
        if modalidade is not None:
            params["codigoModalidadeContratacao"] = modalidade

        response = await self._fetch_with_backpressure(endpoint["path"], params)

        # Enqueue the response for the writer worker
        page_response = {
            "endpoint_url": f"{settings.pncp_base_url}{endpoint['path']}",
            "endpoint_name": endpoint_name,
            "request_parameters": params,
            "response_code": response["status_code"],
            "response_content": response["content"],
            "response_headers": response["headers"],
            "data_date": data_date,
            "run_id": self.run_id,
            "total_records": response.get(
                "total_records", 0
            ),  # This might not be accurate for pages > 1
            "total_pages": response.get(
                "total_pages", 0
            ),  # This might not be accurate for pages > 1
            "current_page": page_number,
            "page_size": endpoint.get("page_size", settings.page_size),
        }
        await self.page_queue.put(page_response)
        return response

    async def _execute_tasks(self, progress: Progress):
        """Phase 3: Fetch all missing pages for FETCHING and PARTIAL tasks."""

        # Use unnest to get a list of all pages to fetch
        pages_to_fetch_query = """
SELECT t.task_id, t.endpoint_name, t.data_date, t.modalidade, CAST(p.page_number AS INTEGER) as page_number
FROM psa.pncp_extraction_tasks t,
     unnest(json_extract(t.missing_pages, '$')::INTEGER[]) AS p(page_number)
WHERE t.status IN ('FETCHING', 'PARTIAL');
        """
        pages_to_fetch = self.writer.conn.execute(pages_to_fetch_query).fetchall()

        if not pages_to_fetch:
            console.print("Phase 3: Execution - No pages to fetch.")
            return

        # Group pages by endpoint only
        endpoint_pages = {}
        for (
            task_id,
            endpoint_name,
            data_date,
            modalidade,
            page_number,
        ) in pages_to_fetch:
            if endpoint_name not in endpoint_pages:
                endpoint_pages[endpoint_name] = []
            endpoint_pages[endpoint_name].append(
                (task_id, data_date, modalidade, page_number)
            )

        # Create progress bars for each endpoint with beautiful colors
        progress_bars = {}
        endpoint_colors = {
            "contratos_publicacao": "green",
            "contratos_atualizacao": "blue",
            "atas_periodo": "cyan",
            "atas_atualizacao": "bright_cyan",
            "contratacoes_publicacao": "yellow",
            "contratacoes_atualizacao": "magenta",
            "pca_atualizacao": "bright_blue",
            "instrumentoscobranca_inclusao": "bright_green",
            "contratacoes_proposta": "bright_yellow",
        }

        console.print("\n🚀 [bold blue]PNCP Data Extraction Progress[/bold blue]\n")

        for endpoint_name, pages in endpoint_pages.items():
            # Get endpoint description and color
            endpoint_desc = next(
                (
                    ep["description"]
                    for ep in settings.pncp_endpoints
                    if ep["name"] == endpoint_name
                ),
                endpoint_name,
            )
            color = endpoint_colors.get(endpoint_name, "white")

            # Create beautiful, colorful progress description
            task_description = (
                f"[{color}]{endpoint_desc}[/{color}] - [dim]{len(pages):,} pages[/dim]"
            )
            progress_bars[endpoint_name] = progress.add_task(
                task_description, total=len(pages)
            )

        # Execute all fetches
        fetch_tasks = []
        for endpoint_name, pages in endpoint_pages.items():
            for task_id, data_date, modalidade, page_number in pages:
                # Check for shutdown before creating new tasks
                if self.shutdown_event.is_set():
                    console.print(
                        "⚠️ [yellow]Shutdown requested, stopping task creation...[/yellow]"
                    )
                    break

                task = asyncio.create_task(
                    self._fetch_page_with_progress(
                        endpoint_name,
                        data_date,
                        modalidade,
                        page_number,
                        progress,
                        progress_bars[endpoint_name],
                    )
                )
                fetch_tasks.append(task)
                self.running_tasks.add(task)

        # Wait for all tasks to complete with graceful shutdown support
        try:
            await asyncio.gather(*fetch_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            console.print(
                "⚠️ [yellow]Tasks cancelled during shutdown, cleaning up...[/yellow]"
            )
            raise
        finally:
            # Clean up completed tasks
            for task in fetch_tasks:
                self.running_tasks.discard(task)

        # Print beautiful overall summary
        total_pages = sum(len(pages) for pages in endpoint_pages.values())
        console.print(
            f"\n✅ [bold green]Overall: {total_pages:,} pages completed successfully![/bold green]"
        )
        console.print("")

    async def _fetch_page_with_progress(
        self,
        endpoint_name: str,
        data_date: date,
        modalidade: int | None,
        page_number: int,
        progress: Progress,
        progress_bar_id: int,
    ):
        """Fetch a page and update the progress bar with graceful shutdown support."""
        try:
            # Check for shutdown signal
            if self.shutdown_event.is_set():
                console.print(
                    "⚠️ [yellow]Shutdown requested, skipping page fetch...[/yellow]"
                )
                return

            await self._fetch_page(endpoint_name, data_date, modalidade, page_number)
            progress.update(progress_bar_id, advance=1)
        except asyncio.CancelledError:
            console.print("⚠️ [yellow]Page fetch cancelled during shutdown[/yellow]")
            raise
        except Exception as e:
            logger.error(
                f"Failed to fetch page {page_number} for {endpoint_name} {data_date} modalidade {modalidade}: {e}"
            )
            progress.update(
                progress_bar_id, advance=1
            )  # Still advance to show completion

    async def _reconcile_tasks(self):
        """Phase 4: Update task status based on downloaded data."""
        console.print("Phase 4: Reconciling tasks...")

        tasks_to_reconcile = self.writer.conn.execute(
            "SELECT task_id, endpoint_name, data_date, modalidade, total_pages FROM psa.pncp_extraction_tasks WHERE status IN ('FETCHING', 'PARTIAL')"
        ).fetchall()

        for (
            task_id,
            endpoint_name,
            data_date,
            modalidade,
            total_pages,
        ) in tasks_to_reconcile:
            # Find out which pages were successfully downloaded for this task
            # We need to check the request_parameters for modalidade if it exists
            if modalidade is not None:
                downloaded_pages_result = self.writer.conn.execute(
                    """
                    SELECT DISTINCT current_page
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
                    AND json_extract(request_parameters, '$.codigoModalidadeContratacao') = ?
                    """,
                    [endpoint_name, data_date, modalidade],
                ).fetchall()
            else:
                downloaded_pages_result = self.writer.conn.execute(
                    """
                    SELECT DISTINCT current_page
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
                    AND json_extract(request_parameters, '$.codigoModalidadeContratacao') IS NULL
                    """,
                    [endpoint_name, data_date],
                ).fetchall()

            downloaded_pages = {row[0] for row in downloaded_pages_result}

            # Generate the full set of expected pages
            all_pages = set(range(1, total_pages + 1))

            # Calculate the new set of missing pages
            new_missing_pages = sorted(all_pages - downloaded_pages)

            if not new_missing_pages:
                # All pages are downloaded
                self.writer.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'COMPLETE', missing_pages = '[]', updated_at = now() WHERE task_id = ?",
                    [task_id],
                )
            else:
                # Some pages are still missing
                self.writer.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'PARTIAL', missing_pages = ?, updated_at = now() WHERE task_id = ?",
                    [json.dumps(new_missing_pages), task_id],
                )

        self.writer.conn.commit()
        console.print("Reconciliation complete.")

    async def extract_data(
        self, start_date: date, end_date: date, force: bool = False
    ) -> dict[str, Any]:
        """Main extraction method using a task-based, phased architecture."""
        logger.info(
            f"Extraction started: {start_date.isoformat()} to {end_date.isoformat()}, "
            f"concurrency={self.concurrency}, run_id={self.run_id}, force={force}"
        )
        start_time = time.time()

        if force:
            console.print(
                "[yellow]Force mode enabled - will reset tasks and re-extract all data.[/yellow]"
            )
            self.writer.conn.execute("DELETE FROM psa.pncp_extraction_tasks")
            self.writer.conn.execute("DELETE FROM psa.pncp_raw_responses")
            self.writer.conn.commit()

        # Setup signal handlers now that we're in async context
        self.setup_signal_handlers()

        # Start writer worker
        self.writer_running = True
        writer_task = asyncio.create_task(
            self.writer.writer_worker(self.page_queue, commit_every=100)
        )
        self.running_tasks.add(writer_task)

        # --- Main Execution Flow ---

        # Phase 1: Planning
        await self._plan_tasks(start_date, end_date)

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
            refresh_per_second=10,
        ) as progress:
            # Phase 2: Discovery
            await self._discover_tasks(progress)

            # Phase 3: Execution
            await self._execute_tasks(progress)

        # Wait for writer to process all enqueued pages
        try:
            await self.page_queue.join()
            await self.page_queue.put(None)  # Send sentinel
            await writer_task
        except asyncio.CancelledError:
            console.print("⚠️ [yellow]Writer task cancelled during shutdown[/yellow]")
            # Ensure writer task is cancelled
            if not writer_task.done():
                writer_task.cancel()
                try:
                    await writer_task
                except asyncio.CancelledError:
                    pass
        finally:
            self.running_tasks.discard(writer_task)

        # Phase 4: Reconciliation
        await self._reconcile_tasks()

        # --- Final Reporting ---
        duration = time.time() - start_time

        # Fetch final stats from the control table
        total_tasks = self.writer.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks"
        ).fetchone()[0]
        complete_tasks = self.writer.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE'"
        ).fetchone()[0]
        failed_tasks = self.writer.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'FAILED'"
        ).fetchone()[0]

        # Fetch stats from raw responses
        total_records_sum = (
            self.writer.conn.execute(
                "SELECT SUM(total_records) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE'"
            ).fetchone()[0]
            or 0
        )

        total_results = {
            "run_id": self.run_id,
            "start_date": start_date,
            "end_date": end_date,
            "total_tasks": total_tasks,
            "complete_tasks": complete_tasks,
            "failed_tasks": failed_tasks,
            "total_records_extracted": total_records_sum,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "duration": duration,
        }

        console.print("\n🎉 Extraction Complete!")
        console.print(
            f"Total Tasks: {total_tasks:,} ({complete_tasks:,} complete, {failed_tasks:,} failed)"
        )
        console.print(f"Total Records: {total_records_sum:,}")
        console.print(f"Duration: {duration:.1f}s")

        return total_results


def _get_current_month_end() -> str:
    """Get the last day of the current month as YYYY-MM-DD."""
    today = date.today()
    # Get last day of current month safely
    _, last_day = calendar.monthrange(today.year, today.month)
    month_end = today.replace(day=last_day)
    return month_end.strftime("%Y-%m-%d")


# CLI interface
app = typer.Typer()


@app.command()
def extract(
    start_date: str = "2021-01-01",
    end_date: str = None,
    concurrency: int = settings.concurrency,
    force: bool = False,
):
    """Extract data using true async architecture."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    # Use current month end if no end_date provided
    if end_date is None:
        end_date = _get_current_month_end()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    async def main():
        async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
            results = await extractor.extract_data(start_dt, end_dt, force)

            # Save results
            results_file = (
                DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
            )
            with Path(results_file).open("w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, default=str)

            console.print(f"Results saved to: {results_file}")

    asyncio.run(main())


@app.command()
def stats():
    """Show extraction statistics."""
    conn = connect_utf8(str(BALIZA_DB_PATH))

    # Overall stats
    total_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses"
    ).fetchone()[0]
    success_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200"
    ).fetchone()[0]

    console.print(f"=== Total Responses: {total_responses:,} ===")
    console.print(f"Successful: {success_responses:,}")
    console.print(f"❌ Failed: {total_responses - success_responses:,}")

    if total_responses > 0:
        console.print(f"Success Rate: {success_responses / total_responses * 100:.1f}%")

    # Endpoint breakdown
    endpoint_stats = conn.execute(
        """
        SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM psa.pncp_raw_responses
        WHERE response_code = 200
        GROUP BY endpoint_name
        ORDER BY total_records DESC
    """
    ).fetchall()

    console.print("\n=== Endpoint Statistics ===")
    for name, responses, records in endpoint_stats:
        console.print(f"  {name}: {responses:,} responses, {records:,} records")

    conn.close()


if __name__ == "__main__":
    # 1. Trave o runtime em UTF-8 - reconfigure streams logo no início
    for std in (sys.stdin, sys.stdout, sys.stderr):
        std.reconfigure(encoding="utf-8", errors="surrogateescape")
    app()
