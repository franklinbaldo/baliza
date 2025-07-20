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
import contextlib
import json
import logging
import re
import signal
import sys
import time
import uuid
from collections import defaultdict
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
from rich.table import Table

# Import configuration from the config module
from baliza.config import settings
from baliza.pncp_client import PNCPClient
from baliza.pncp_task_planner import PNCPTaskPlanner
from baliza.pncp_writer import BALIZA_DB_PATH, DATA_DIR, PNCPWriter, connect_utf8
from baliza.task_claimer import TaskClaimer, create_task_plan
from baliza.plan_fingerprint import get_endpoint_config
from baliza.extraction_coordinator import ExtractionCoordinator

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

    def __init__(self, concurrency: int = settings.concurrency, force_db: bool = False):
        self.concurrency = concurrency
        self.client = PNCPClient(concurrency=concurrency)
        self.task_planner = PNCPTaskPlanner()  # Instantiate the task planner
        self.writer = PNCPWriter(force_db=force_db)  # Instantiate the writer
        self.run_id = str(uuid.uuid4())
        self.force_db = force_db

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

        def signal_handler(_signum, _frame):
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
        console.print("🎯 [bold blue]Phase 1: Resumable Task Planning[/bold blue]")
        tasks_to_create = await self.task_planner.plan_tasks(start_date, end_date, writer_conn=self.writer.conn)

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
        if len(tasks_to_create) > 0:
            console.print(f"✅ [green]Planning complete: {len(tasks_to_create):,} new tasks created[/green]")
        else:
            console.print("✅ [green]Planning complete: All tasks already exist - fully resumable![/green]")

    async def _discover_tasks(self, progress: Progress):
        """Phase 2: Get metadata for all PENDING tasks (resumable)."""
        # Get task status overview for resumability info
        task_status = self.writer.conn.execute("""
            SELECT status, COUNT(*) as count 
            FROM psa.pncp_extraction_tasks 
            GROUP BY status 
            ORDER BY status
        """).fetchall()

        console.print("📊 [bold blue]Phase 2: Resumable Task Discovery[/bold blue]")
        console.print("Task status overview:")
        for status, count in task_status:
            console.print(f"   {status}: {count:,} tasks")

        # Fast bulk PSA check using JOIN to avoid individual API calls
        console.print("🔍 [cyan]Checking for existing requests in PSA area with fast JOIN...[/cyan]")
        
        # Mark existing tasks as completed in bulk using efficient JOIN
        completed_count = self.writer.conn.execute("""
            UPDATE psa.pncp_extraction_tasks 
            SET status = 'COMPLETED', total_pages = 1, total_records = 0, updated_at = now()
            WHERE status = 'PENDING' 
            AND EXISTS (
                SELECT 1 FROM psa.pncp_requests r
                WHERE r.endpoint_name = psa.pncp_extraction_tasks.endpoint_name
                AND r.data_date = psa.pncp_extraction_tasks.data_date
                AND (
                    (psa.pncp_extraction_tasks.modalidade IS NULL AND json_extract(r.request_parameters, '$.codigoModalidadeContratacao') IS NULL)
                    OR (json_extract(r.request_parameters, '$.codigoModalidadeContratacao') = CAST(psa.pncp_extraction_tasks.modalidade AS TEXT))
                )
            )
        """).rowcount
        
        if completed_count > 0:
            console.print(f"✅ [green]Marked {completed_count:,} tasks as completed (data exists in PSA)[/green]")
        
        # Get remaining pending tasks that need discovery
        pending_tasks = self.writer.conn.execute(
            "SELECT task_id, endpoint_name, data_date, modalidade FROM psa.pncp_extraction_tasks WHERE status = 'PENDING'"
        ).fetchall()

        if not pending_tasks:
            console.print("✅ [green]No pending tasks to discover - all tasks already processed![/green]")
            return

        console.print(f"🔍 Discovering metadata for {len(pending_tasks):,} pending tasks...")
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
                # Handle 404 responses - mark task as completed since no data exists
                if response.get("status_code") == 404:
                    self.writer.conn.execute(
                        """
                        UPDATE psa.pncp_extraction_tasks
                        SET status = 'COMPLETED', total_pages = 1, total_records = 0, missing_pages = '[]', updated_at = now()
                        WHERE task_id = ?
                        """,
                        [task_id],
                    )
                    progress.update(discovery_progress, advance=1)
                    continue
                
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

        # Validate response before processing
        if not response.get("success", False):
            logger.warning(f"Failed to fetch page {page_number} for {endpoint_name} {data_date}: {response.get('error', 'Unknown error')}")
            # Don't enqueue failed responses - they should be handled by reconciliation
            return response

        # Validate response content
        content = response.get("content", "")
        if not content or content.strip() == "":
            logger.warning(f"Empty response content for page {page_number} of {endpoint_name} {data_date}")
            # Mark as failed response for reconciliation
            response["success"] = False
            response["error"] = "Empty response content"
            return response

        # Enqueue the successful response for the writer worker
        page_response = {
            "endpoint_url": f"{settings.pncp_base_url}{endpoint['path']}",
            "endpoint_name": endpoint_name,
            "request_parameters": params,
            "response_code": response["status_code"],
            "response_content": content,
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

    def _create_matrix_progress_table(self, endpoint_month_data: dict) -> Table:
        """Create a beautiful matrix-style progress table showing endpoint x month progress."""
        table = Table(show_header=True, header_style="bold blue", title="📊 PNCP Extraction Progress Matrix")

        # Get all unique months and endpoints
        all_months = set()
        all_endpoints = set()

        for (endpoint, month), data in endpoint_month_data.items():
            all_months.add(month)
            all_endpoints.add(endpoint)

        # Sort months chronologically and endpoints alphabetically
        sorted_months = sorted(all_months)
        sorted_endpoints = sorted(all_endpoints)

        # Add columns: Month first, then each endpoint
        table.add_column("Month", style="cyan", min_width=8)

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

        for endpoint in sorted_endpoints:
            # Shorten endpoint names for better table display
            short_name = endpoint.replace("_publicacao", "_pub").replace("_atualizacao", "_upd").replace("contratacoes", "contr")
            color = endpoint_colors.get(endpoint, "white")
            table.add_column(short_name, style=color, min_width=12)

        # Add rows for each month
        for month in sorted_months:
            row = [month]

            for endpoint in sorted_endpoints:
                key = (endpoint, month)
                if key in endpoint_month_data:
                    data = endpoint_month_data[key]
                    completed = data['completed']
                    total = data['total']
                    percentage = (completed / total * 100) if total > 0 else 0

                    # Create mini progress bar
                    if percentage == 100:
                        progress_bar = "[green]✅ 100%[/green]"
                    elif percentage > 75:
                        progress_bar = f"[yellow]▓▓▓░ {percentage:.0f}%[/yellow]"
                    elif percentage > 50:
                        progress_bar = f"[orange3]▓▓░░ {percentage:.0f}%[/orange3]"
                    elif percentage > 25:
                        progress_bar = f"[red]▓░░░ {percentage:.0f}%[/red]"
                    elif percentage > 0:
                        progress_bar = f"[bright_black]░░░░ {percentage:.0f}%[/bright_black]"
                    else:
                        progress_bar = "[dim]---- 0%[/dim]"

                    row.append(progress_bar)
                else:
                    row.append("[dim]N/A[/dim]")

            table.add_row(*row)

        return table

    async def _execute_tasks(self, _progress: Progress):
        """Phase 3: Fetch all missing pages for FETCHING and PARTIAL tasks (resumable)."""

        # Get execution status overview for resumability info
        execution_status = self.writer.conn.execute("""
            SELECT 
                status,
                COUNT(*) as task_count,
                SUM(total_pages) as total_pages,
                SUM(array_length(json_extract(missing_pages, '$')::INTEGER[], 1)) as missing_pages
            FROM psa.pncp_extraction_tasks 
            WHERE status IN ('FETCHING', 'PARTIAL', 'COMPLETE')
            GROUP BY status 
            ORDER BY status
        """).fetchall()

        console.print("🚀 [bold blue]Phase 3: Resumable Task Execution[/bold blue]")
        console.print("Execution status overview:")
        for status, task_count, total_pages, missing_pages in execution_status:
            if missing_pages is not None:
                console.print(f"   {status}: {task_count:,} tasks ({missing_pages:,} pages remaining)")
            else:
                console.print(f"   {status}: {task_count:,} tasks")

        # Use unnest to get a list of all pages to fetch
        pages_to_fetch_query = """
SELECT t.task_id, t.endpoint_name, t.data_date, t.modalidade, CAST(p.page_number AS INTEGER) as page_number
FROM psa.pncp_extraction_tasks t,
     unnest(json_extract(t.missing_pages, '$')::INTEGER[]) AS p(page_number)
WHERE t.status IN ('FETCHING', 'PARTIAL');
        """
        pages_to_fetch = self.writer.conn.execute(pages_to_fetch_query).fetchall()

        if not pages_to_fetch:
            console.print("✅ [green]No pages to fetch - all tasks completed![/green]")
            return

        console.print(f"📥 Fetching {len(pages_to_fetch):,} remaining pages...")

        # Group pages by endpoint and month for matrix display
        endpoint_month_pages = defaultdict(list)
        endpoint_month_totals = defaultdict(int)
        endpoint_month_completed = defaultdict(int)

        for (
            task_id,
            endpoint_name,
            data_date,
            modalidade,
            page_number,
        ) in pages_to_fetch:
            # Extract month from data_date for grouping
            month_key = data_date.strftime("%Y-%m")
            key = (endpoint_name, month_key)

            endpoint_month_pages[key].append((task_id, data_date, modalidade, page_number))
            endpoint_month_totals[key] += 1

        # Initialize completion tracking
        for key in endpoint_month_totals:
            endpoint_month_completed[key] = 0

        # Prepare matrix data for progress table
        matrix_data = {}
        for key, total in endpoint_month_totals.items():
            matrix_data[key] = {
                'total': total,
                'completed': endpoint_month_completed[key]
            }

        # Create and display initial matrix table
        console.print("\n🚀 [bold blue]PNCP Data Extraction Progress Matrix[/bold blue]\n")

        # Create live updating progress display
        matrix_table = self._create_matrix_progress_table(matrix_data)
        console.print(matrix_table)

        # Execute all fetches with matrix progress tracking
        fetch_tasks = []
        for key, pages in endpoint_month_pages.items():
            endpoint_name, month_key = key
            for task_id, data_date, modalidade, page_number in pages:
                # Check for shutdown before creating new tasks
                if self.shutdown_event.is_set():
                    console.print(
                        "⚠️ [yellow]Shutdown requested, stopping task creation...[/yellow]"
                    )
                    break

                task = asyncio.create_task(
                    self._fetch_page_with_matrix_progress(
                        endpoint_name,
                        data_date,
                        modalidade,
                        page_number,
                        matrix_data,
                        key,
                    )
                )
                fetch_tasks.append(task)
                self.running_tasks.add(task)

        # Create a task to periodically update the matrix display
        async def update_matrix_display():
            """Periodically refresh the matrix progress display."""
            last_update_time = asyncio.get_event_loop().time()
            while not all(task.done() for task in fetch_tasks):
                current_time = asyncio.get_event_loop().time()

                # Update every 3 seconds
                if current_time - last_update_time >= 3.0:
                    # Clear previous table and show updated matrix
                    console.print("\n" + "─" * 80)  # Separator line
                    updated_matrix_table = self._create_matrix_progress_table(matrix_data)
                    console.print(updated_matrix_table)

                    last_update_time = current_time

                await asyncio.sleep(0.5)  # Check every 500ms

        # Start the matrix update task
        matrix_update_task = asyncio.create_task(update_matrix_display())

        # Wait for all tasks to complete with graceful shutdown support
        try:
            await asyncio.gather(*fetch_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            console.print(
                "⚠️ [yellow]Tasks cancelled during shutdown, cleaning up...[/yellow]"
            )
            raise
        finally:
            # Cancel the matrix update task
            matrix_update_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await matrix_update_task

            # Clean up completed tasks
            for task in fetch_tasks:
                self.running_tasks.discard(task)

        # Print beautiful overall summary with final matrix
        total_pages = sum(len(pages) for pages in endpoint_month_pages.values())

        # Show final progress matrix
        console.print("\n📊 [bold blue]Final Progress Matrix[/bold blue]\n")
        final_matrix_table = self._create_matrix_progress_table(matrix_data)
        console.print(final_matrix_table)

        console.print(
            f"\n✅ [bold green]Overall: {total_pages:,} pages completed successfully![/bold green]"
        )
        console.print("")

    async def _fetch_page_with_matrix_progress(
        self,
        endpoint_name: str,
        data_date: date,
        modalidade: int | None,
        page_number: int,
        matrix_data: dict,
        matrix_key: tuple,
    ):
        """Fetch a page and update the matrix progress with graceful shutdown support."""
        try:
            # Check for shutdown signal
            if self.shutdown_event.is_set():
                console.print(
                    "⚠️ [yellow]Shutdown requested, skipping page fetch...[/yellow]"
                )
                return

            await self._fetch_page(endpoint_name, data_date, modalidade, page_number)

            # Update matrix progress
            matrix_data[matrix_key]['completed'] += 1

        except asyncio.CancelledError:
            console.print("⚠️ [yellow]Page fetch cancelled during shutdown[/yellow]")
            raise
        except Exception as e:
            logger.exception(
                f"Failed to fetch page {page_number} for {endpoint_name} {data_date} modalidade {modalidade}: {e}"
            )
            # Still update progress to show completion (even if failed)
            matrix_data[matrix_key]['completed'] += 1

    async def _fetch_page_with_progress(
        self,
        endpoint_name: str,
        data_date: date,
        modalidade: int | None,
        page_number: int,
        progress: Progress,
        progress_bar_id: int,
    ):
        """Legacy method - Fetch a page and update the progress bar with graceful shutdown support."""
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
            logger.exception(
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
                with contextlib.suppress(asyncio.CancelledError):
                    await writer_task
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

    async def extract_dbt_driven(self, start_date: date, end_date: date, use_existing_plan: bool = True):
        """dbt-driven extraction using task planning tables.
        
        This method now delegates to ExtractionCoordinator for better separation of concerns
        and improved maintainability. The complex orchestration logic has been moved to
        dedicated phase classes.
        
        Args:
            start_date: Start date for extraction
            end_date: End date for extraction  
            use_existing_plan: Use existing plan if available, otherwise generate new
        """
        # Initialize coordinator with dependency injection
        coordinator = ExtractionCoordinator(
            writer=self.writer,
            concurrency=self.concurrency,
            force_db=self.force_db
        )
        
        # Delegate to coordinator for clean orchestration
        return await coordinator.extract_dbt_driven(start_date, end_date, use_existing_plan)

    async def _dbt_plan_tasks(self, start_date: date, end_date: date, use_existing_plan: bool, claimer: TaskClaimer) -> str:
        """Phase 1: Generate or validate task plan using dbt."""
        console.print("🎯 [bold blue]Phase 1: dbt Task Planning[/bold blue]")
        
        # Check for existing plan
        with duckdb.connect(self.writer.db_path) as conn:
            existing_plan = conn.execute("""
                SELECT plan_fingerprint, task_count, generated_at
                FROM main_planning.task_plan_meta 
                WHERE date_range_start <= ? AND date_range_end >= ?
                ORDER BY generated_at DESC
                LIMIT 1
            """, (start_date, end_date)).fetchone()
        
        if existing_plan and use_existing_plan:
            plan_fingerprint, task_count, generated_at = existing_plan
            console.print(f"✅ [green]Using existing plan: {plan_fingerprint[:16]}... ({task_count:,} tasks)[/green]")
            console.print(f"   Generated: {generated_at}")
            
            # Validate fingerprint
            if not claimer.validate_plan_fingerprint(plan_fingerprint):
                console.print("⚠️ [yellow]Plan fingerprint validation failed - generating new plan[/yellow]")
                plan_fingerprint = create_task_plan(str(start_date), str(end_date), "prod")
        else:
            console.print("🔨 [blue]Generating new task plan with dbt...[/blue]")
            plan_fingerprint = create_task_plan(str(start_date), str(end_date), "prod")
            
            # Get task count
            with duckdb.connect(self.writer.db_path) as conn:
                task_count = conn.execute("SELECT COUNT(*) FROM main_planning.task_plan WHERE plan_fingerprint = ?", (plan_fingerprint,)).fetchone()[0]
            
            console.print(f"✅ [green]Plan generated: {plan_fingerprint[:16]}... ({task_count:,} tasks)[/green]")
        
        return plan_fingerprint

    async def _dbt_execute_tasks(self, progress: Progress, claimer: TaskClaimer, plan_fingerprint: str):
        """Phase 2: Claim and execute tasks from the dbt plan."""
        console.print("⚡ [bold blue]Phase 2: Task Execution[/bold blue]")
        
        # Get total pending tasks for progress tracking
        with duckdb.connect(self.writer.db_path) as conn:
            total_pending = conn.execute("""
                SELECT COUNT(*) FROM main_planning.task_plan 
                WHERE status = 'PENDING' AND plan_fingerprint = ?
            """, (plan_fingerprint,)).fetchone()[0]
        
        if total_pending == 0:
            console.print("✅ [green]No pending tasks - all work already completed![/green]")
            return
        
        console.print(f"🎯 Executing {total_pending:,} pending tasks...")
        execution_progress = progress.add_task("[cyan]Executing tasks", total=total_pending)
        
        processed_tasks = 0
        
        while not self.shutdown_event.is_set():
            # Release expired claims
            expired_count = claimer.release_expired_claims()
            if expired_count > 0:
                console.print(f"🧹 Released {expired_count} expired claims")
            
            # Claim batch of tasks
            claimed_tasks = claimer.claim_pending_tasks(limit=self.concurrency * 2)
            
            if not claimed_tasks:
                console.print("✅ [green]No more tasks to claim - execution complete![/green]")
                break
            
            console.print(f"🔒 Claimed {len(claimed_tasks)} tasks for execution")
            
            # Execute claimed tasks concurrently
            execution_jobs = []
            for task in claimed_tasks:
                job = asyncio.create_task(self._execute_dbt_task(task, claimer))
                execution_jobs.append(job)
                self.running_tasks.add(job)
            
            # Wait for all jobs to complete
            try:
                await asyncio.gather(*execution_jobs)
                processed_tasks += len(claimed_tasks)
                progress.update(execution_progress, completed=processed_tasks)
            except Exception as e:
                console.print(f"⚠️ [yellow]Some tasks failed: {e}[/yellow]")
            finally:
                for job in execution_jobs:
                    self.running_tasks.discard(job)
            
            # Small delay to prevent tight loops
            await asyncio.sleep(0.1)

    async def _execute_dbt_task(self, task: dict, claimer: TaskClaimer):
        """Execute a single claimed task."""
        task_id = task['task_id']
        endpoint_name = task['endpoint_name']
        data_date = task['data_date']
        modalidade = task['modalidade']
        
        try:
            # Update status to EXECUTING
            claimer.update_claim_status(task_id, 'EXECUTING')
            
            # Get endpoint configuration from unified config
            endpoint_config = get_endpoint_config(endpoint_name)
            
            # Build API URL and parameters
            url = f"{settings.pncp_base_url}{endpoint_config['path']}"
            params = {
                "pagina": 1,
                "tamanhoPagina": endpoint_config.get('page_size', 500),
            }
            
            # Add date parameters
            date_params = endpoint_config.get('date_params', [])
            if len(date_params) >= 2:
                params[date_params[0]] = str(data_date)
                params[date_params[1]] = str(data_date)
            elif len(date_params) == 1:
                params[date_params[0]] = str(data_date)
            
            # Add modalidade if specified
            if modalidade is not None:
                params['modalidade'] = modalidade
            
            # Execute all pages for this task
            page_number = 1
            total_records = 0
            
            while True:
                params['pagina'] = page_number
                
                # Fetch page with backpressure
                response = await self._fetch_with_backpressure(url, params, task_id)
                
                if not response['success']:
                    console.print(f"❌ [red]Failed to fetch {endpoint_name} page {page_number}: {response.get('error', 'Unknown error')}[/red]")
                    break
                
                # Record result
                request_id = str(uuid.uuid4())
                page_records = len(response.get('data', []))
                total_records += page_records
                
                claimer.record_task_result(task_id, request_id, page_number, page_records)
                
                # Queue page for writing
                page_data = {
                    'task_id': task_id,
                    'endpoint_name': endpoint_name,
                    'response': response,
                    'page_number': page_number,
                    'data_date': data_date,
                    'modalidade': modalidade,
                    'request_id': request_id
                }
                
                await self.page_queue.put(page_data)
                
                # Check if we have more pages
                if page_records < endpoint_config.get('page_size', 500):
                    break  # Last page
                
                page_number += 1
                
                # Prevent runaway pagination
                if page_number > 1000:
                    console.print(f"⚠️ [yellow]Stopping pagination at page {page_number} for safety[/yellow]")
                    break
            
            # Mark task as completed
            claimer.update_claim_status(task_id, 'COMPLETED')
            
        except Exception as e:
            console.print(f"❌ [red]Task {task_id[:8]}... failed: {e}[/red]")
            claimer.update_claim_status(task_id, 'FAILED')
            raise

    async def _get_dbt_extraction_stats(self, claimer: TaskClaimer) -> dict:
        """Get extraction statistics from dbt tables."""
        with duckdb.connect(self.writer.db_path) as conn:
            # Task statistics
            task_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_tasks,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_tasks,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_tasks,
                    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending_tasks
                FROM main_planning.task_plan
            """).fetchone()
            
            # Result statistics  
            result_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_results,
                    SUM(records_extracted) as total_records
                FROM main_runtime.task_results
            """).fetchone()
            
            # Worker statistics
            worker_stats = claimer.get_worker_stats()
            
            return {
                'total_tasks': task_stats[0] if task_stats else 0,
                'completed_tasks': task_stats[1] if task_stats else 0,
                'failed_tasks': task_stats[2] if task_stats else 0,
                'pending_tasks': task_stats[3] if task_stats else 0,
                'total_results': result_stats[0] if result_stats else 0,
                'total_records': result_stats[1] if result_stats and result_stats[1] is not None else 0,
                'worker_stats': worker_stats,
                'total_requests': self.total_requests,
                'successful_requests': self.successful_requests,
                'failed_requests': self.failed_requests
            }


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
    end_date: str | None = None,
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
