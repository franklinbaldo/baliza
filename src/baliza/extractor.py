#!/usr/bin/env python3
"""
PNCP Data Extractor V3 - Task-Oriented Architecture
This version uses a persistent control table to manage a stateful, multi-phase
extraction process (Plan -> Discover -> Fetch -> Reconcile).
"""

import asyncio
import calendar
import json
import math
import secrets
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
import orjson
import structlog
import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

# --- Basic Setup ---
console = Console()
logger = structlog.get_logger()

# --- Configuration ---
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta"
CONCURRENCY = 8  # Increased default for better performance with this architecture
PAGE_SIZE = 500
REQUEST_TIMEOUT = 30
USER_AGENT = "BALIZA/3.0 (Backup Aberto de Licitacoes)"
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"

# --- Endpoints ---
PNCP_ENDPOINTS = [
    {"name": "contratos_publicacao", "path": "/v1/contratos"},
    {"name": "contratos_atualizacao", "path": "/v1/contratos/atualizacao"},
    {"name": "atas_periodo", "path": "/v1/atas"},
    {"name": "atas_atualizacao", "path": "/v1/atas/atualizacao"},
]


# --- Helper Functions ---
def parse_json_robust(content: str) -> Any:
    """Parse JSON with orjson (fast) and fallback to stdlib json."""
    try:
        return orjson.loads(content)
    except orjson.JSONDecodeError:
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.exception(
                "JSON_PARSE_ERROR",
                error=str(e),
                content_preview=content[:200],
                exc_info=True
            )
            raise


class TaskOrientedPNCPExtractor:
    """
    Extractor based on a persistent task control table in DuckDB.
    Follows a multi-phase process: Plan -> Discover -> Fetch -> Reconcile.
    """

    def __init__(self, concurrency: int = CONCURRENCY):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.run_id = str(uuid.uuid4())
        self.client = None
        self.page_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=concurrency * 10
        )
        self.writer_running = False
        self._init_database()

    def _init_database(self):
        """Initialize DuckDB and create schemas for raw data and task control."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(BALIZA_DB_PATH), read_only=False)
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                task_id VARCHAR,
                endpoint_name VARCHAR NOT NULL,
                data_date DATE,
                current_page INTEGER,
                response_code INTEGER NOT NULL,
                response_content TEXT,
                run_id VARCHAR,
                extracted_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            ) WITH (compression = 'zstd');
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS psa.pncp_extraction_tasks (
                task_id VARCHAR PRIMARY KEY,
                endpoint_name VARCHAR NOT NULL,
                data_date DATE NOT NULL,
                status VARCHAR DEFAULT 'PENDING' NOT NULL,
                total_pages INTEGER,
                total_records INTEGER,
                missing_pages JSON,
                last_error TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                CONSTRAINT unique_task UNIQUE (endpoint_name, data_date)
            );
        """)
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_task_page ON psa.pncp_raw_responses(task_id, current_page);"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_task_status ON psa.pncp_extraction_tasks(status);"
        )

    # --- Core Execution Flow ---

    async def extract_data(self, start_date: date, end_date: date, force: bool = False):
        """Main orchestrator for the phased extraction process."""
        logger.info(
            "extraction_started",
            run_id=self.run_id,
            start=start_date,
            end=end_date,
            force=force,
        )
        start_time = time.time()

        async with httpx.AsyncClient(
            base_url=PNCP_BASE_URL,
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        ) as self.client:
            # Start the background writer
            writer_task = asyncio.create_task(self._writer_worker())

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                # Execute each phase sequentially
                await self._phase_1_plan_tasks(start_date, end_date, force, progress)
                await self._phase_2_discover_tasks(progress)
                await self._phase_3_fetch_pages(progress)
                await self._phase_4_reconcile_tasks(progress)

            # Graceful shutdown of the writer
            await self.page_queue.join()
            await self.page_queue.put(None)
            await writer_task

        duration = time.time() - start_time
        logger.info("extraction_complete", duration=f"{duration:.2f}s")
        self._print_summary(duration)
        self.conn.close()

    # --- Phase 1: Planning ---

    async def _phase_1_plan_tasks(
        self, start_date: date, end_date: date, force: bool, progress: Progress
    ):
        """Generate all tasks in the control table for the given date range."""
        phase_task = progress.add_task(
            "[bold cyan]Phase 1: Planning Tasks[/bold cyan]", total=1
        )

        months = self._get_monthly_chunks(start_date, end_date)

        if force:
            console.log(
                "[yellow]Force mode: Resetting tasks in date range to PENDING.[/yellow]"
            )
            self.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status='PENDING', updated_at=now() WHERE data_date BETWEEN ? AND ?",
                [start_date, end_date],
            )

        tasks_to_create = []
        for endpoint in PNCP_ENDPOINTS:
            for month_start, _ in months:
                task_id = f"{endpoint['name']}_{month_start.isoformat()}"
                tasks_to_create.append((task_id, endpoint["name"], month_start))

        if tasks_to_create:
            self.conn.executemany(
                "INSERT INTO psa.pncp_extraction_tasks(task_id, endpoint_name, data_date) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                tasks_to_create,
            )
            self.conn.commit()
            console.log(
                f"Planning complete. Ensured {len(tasks_to_create)} tasks exist for the period."
            )

        progress.update(
            phase_task,
            completed=1,
            description="[bold green]Phase 1: Planning Complete[/bold green]",
        )

    # --- Phase 2: Discovery ---

    async def _phase_2_discover_tasks(self, progress: Progress):
        """Fetch page 1 for all PENDING tasks to get metadata."""
        pending_tasks = self.conn.execute(
            "SELECT task_id, endpoint_name, data_date FROM psa.pncp_extraction_tasks WHERE status = 'PENDING'"
        ).fetchall()
        if not pending_tasks:
            progress.add_task(
                "[bold green]Phase 2: Discovery Skipped (No pending tasks)[/bold green]",
                total=1,
                completed=1,
            )
            return

        phase_task = progress.add_task(
            "[bold cyan]Phase 2: Discovering Tasks[/bold cyan]",
            total=len(pending_tasks),
        )

        discovery_coroutines = [
            self._discover_single_task(task, progress, phase_task)
            for task in pending_tasks
        ]
        await asyncio.gather(*discovery_coroutines)

        progress.update(
            phase_task,
            description="[bold green]Phase 2: Discovery Complete[/bold green]",
        )

    async def _discover_single_task(
        self, task: tuple, progress: Progress, phase_task_id: int
    ):
        """Fetches page 1 for a single task and updates its state."""
        task_id, endpoint_name, data_date = task

        self.conn.execute(
            "UPDATE psa.pncp_extraction_tasks SET status='DISCOVERING', updated_at=now() WHERE task_id = ?",
            [task_id],
        )

        endpoint = next((e for e in PNCP_ENDPOINTS if e["name"] == endpoint_name), None)
        if not endpoint:
            self.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status='FAILED', last_error='Endpoint not found', updated_at=now() WHERE task_id=?",
                [task_id],
            )
            progress.update(phase_task_id, advance=1)
            return

        params = {
            "dataInicial": data_date.strftime("%Y%m%d"),
            "dataFinal": self._get_month_end(data_date).strftime("%Y%m%d"),
            "tamanhoPagina": PAGE_SIZE,
            "pagina": 1,
        }

        response = await self._fetch_page(endpoint["path"], params)

        if response["success"]:
            total_records = response.get("total_records", 0)
            total_pages = (
                math.ceil(total_records / PAGE_SIZE) if total_records > 0 else 1
            )
            missing_pages = list(range(2, total_pages + 1))

            self.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status='FETCHING', total_pages=?, total_records=?, missing_pages=?, updated_at=now() WHERE task_id=?",
                [total_pages, total_records, json.dumps(missing_pages), task_id],
            )
            await self.page_queue.put(
                {
                    "task_id": task_id,
                    "endpoint_name": endpoint_name,
                    "data_date": data_date,
                    "current_page": 1,
                    "response_code": response["status_code"],
                    "response_content": response["content"],
                    "run_id": self.run_id,
                }
            )
        else:
            self.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status='FAILED', last_error=?, updated_at=now() WHERE task_id=?",
                [response.get("error", "Unknown discovery error"), task_id],
            )

        self.conn.commit()
        progress.update(phase_task_id, advance=1)

    # --- Phase 3: Fetching ---

    async def _phase_3_fetch_pages(self, progress: Progress):
        """Fetch all pages listed as 'missing' in the control table."""
        pages_to_fetch = self.conn.execute("""
            SELECT t.task_id, t.endpoint_name, t.data_date, p.page_number
            FROM psa.pncp_extraction_tasks t,
                 unnest(json_cast(t.missing_pages, 'JSON')) AS p(page_number)
            WHERE t.status IN ('FETCHING', 'PARTIAL')
        """).fetchall()

        if not pages_to_fetch:
            progress.add_task(
                "[bold green]Phase 3: Fetching Skipped (No pages to fetch)[/bold green]",
                total=1,
                completed=1,
            )
            return

        phase_task = progress.add_task(
            "[bold cyan]Phase 3: Fetching Pages[/bold cyan]", total=len(pages_to_fetch)
        )

        fetch_coroutines = [
            self._fetch_single_page(page, progress, phase_task)
            for page in pages_to_fetch
        ]
        await asyncio.gather(*fetch_coroutines)

        progress.update(
            phase_task,
            description="[bold green]Phase 3: Fetching Complete[/bold green]",
        )

    async def _fetch_single_page(
        self, page_data: tuple, progress: Progress, phase_task_id: int
    ):
        """Fetches a single page and puts it on the write queue."""
        task_id, endpoint_name, data_date, page_number = page_data
        page_number = int(page_number)  # unnest may return string

        endpoint = next((e for e in PNCP_ENDPOINTS if e["name"] == endpoint_name), None)
        params = {
            "dataInicial": data_date.strftime("%Y%m%d"),
            "dataFinal": self._get_month_end(data_date).strftime("%Y%m%d"),
            "tamanhoPagina": PAGE_SIZE,
            "pagina": page_number,
        }

        response = await self._fetch_page(endpoint["path"], params)

        await self.page_queue.put(
            {
                "task_id": task_id,
                "endpoint_name": endpoint_name,
                "data_date": data_date,
                "current_page": page_number,
                "response_code": response["status_code"],
                "response_content": response.get("content", response.get("error")),
                "run_id": self.run_id,
            }
        )

        progress.update(phase_task_id, advance=1)

    # --- Phase 4: Reconciliation ---

    async def _phase_4_reconcile_tasks(self, progress: Progress):
        """Update task statuses based on the data actually downloaded."""
        tasks_to_reconcile = self.conn.execute(
            "SELECT task_id, missing_pages FROM psa.pncp_extraction_tasks WHERE status IN ('FETCHING', 'PARTIAL')"
        ).fetchall()

        if not tasks_to_reconcile:
            progress.add_task(
                "[bold green]Phase 4: Reconciliation Skipped[/bold green]",
                total=1,
                completed=1,
            )
            return

        phase_task = progress.add_task(
            "[bold cyan]Phase 4: Reconciling Tasks[/bold cyan]",
            total=len(tasks_to_reconcile),
        )

        for task_id, missing_pages_json in tasks_to_reconcile:
            if not missing_pages_json:
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status='COMPLETE', updated_at=now() WHERE task_id=?",
                    [task_id],
                )
                progress.update(phase_task, advance=1)
                continue

            missing_pages = set(json.loads(missing_pages_json))

            downloaded_pages = self.conn.execute(
                "SELECT DISTINCT current_page FROM psa.pncp_raw_responses WHERE task_id = ? AND response_code = 200",
                [task_id],
            ).fetchall()
            downloaded_set = {row[0] for row in downloaded_pages}

            still_missing = list(missing_pages - downloaded_set)

            if not still_missing:
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status='COMPLETE', missing_pages=NULL, updated_at=now() WHERE task_id=?",
                    [task_id],
                )
            else:
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status='PARTIAL', missing_pages=?, updated_at=now() WHERE task_id=?",
                    [json.dumps(still_missing), task_id],
                )

            progress.update(phase_task, advance=1)

        self.conn.commit()
        progress.update(
            phase_task,
            description="[bold green]Phase 4: Reconciliation Complete[/bold green]",
        )

    # --- Worker and Helper Methods ---

    async def _writer_worker(self):
        """Dedicated coroutine to write pages from the queue to the database."""
        self.writer_running = True
        while True:
            page_data = await self.page_queue.get()
            if page_data is None:
                break

            try:
                self.conn.execute(
                    """
                    INSERT INTO psa.pncp_raw_responses (task_id, endpoint_name, data_date, current_page, response_code, response_content, run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        page_data.get("task_id"),
                        page_data.get("endpoint_name"),
                        page_data.get("data_date"),
                        page_data.get("current_page"),
                        page_data.get("response_code"),
                        page_data.get("response_content"),
                        page_data.get("run_id"),
                    ],
                )
                self.conn.commit()
            except Exception as e:
                logger.exception(
                    "DB_WRITE_ERROR",
                    error=str(e),
                    page_data=page_data.get("task_id"),
                    exc_info=True
                )

            self.page_queue.task_done()
        self.writer_running = False

    async def _fetch_page(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """Fetch a single page with backpressure and simple retry."""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    response = await self.client.get(url, params=params)
                    if response.status_code == 200:
                        content = response.text
                        data = parse_json_robust(content)
                        return {
                            "success": True,
                            "status_code": 200,
                            "content": content,
                            "total_records": data.get("totalRegistros", 0),
                        }
                    if response.status_code == 204:
                        return {
                            "success": True,
                            "status_code": 204,
                            "content": "",
                            "total_records": 0,
                        }

                    # For non-2xx/204, retry on 5xx, fail immediately on 4xx
                    if 400 <= response.status_code < 500:
                        return {
                            "success": False,
                            "status_code": response.status_code,
                            "error": response.text,
                        }

                    # It's a 5xx or other transient error, retry
                    if attempt < 2:
                        # Use SystemRandom for cryptographically secure random numbers
                        secure_random = secrets.SystemRandom()
                        delay = (2**attempt) * secure_random.uniform(0.5, 1.5)
                        await asyncio.sleep(delay)
                        continue

                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "error": f"Failed after 3 attempts: {response.text}",
                    }

                except Exception as e:
                    if attempt < 2:
                        # Use SystemRandom for cryptographically secure random numbers
                        secure_random = secrets.SystemRandom()
                        delay = (2**attempt) * secure_random.uniform(0.5, 1.5)
                        await asyncio.sleep(delay)
                        continue
                    return {
                        "success": False,
                        "status_code": 0,
                        "error": f"Request failed: {e!s}",
                    }

    def _get_monthly_chunks(
        self, start_date: date, end_date: date
    ) -> list[tuple[date, date]]:
        """Generate monthly date chunks."""
        chunks = []
        current = start_date.replace(day=1)
        while current <= end_date:
            _, last_day = calendar.monthrange(current.year, current.month)
            month_end = current.replace(day=last_day)
            chunks.append((current, min(month_end, end_date)))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return chunks

    def _get_month_end(self, start_of_month: date) -> date:
        """Get the last day of the month for a given date."""
        _, last_day = calendar.monthrange(start_of_month.year, start_of_month.month)
        return start_of_month.replace(day=last_day)

    def _print_summary(self, duration: float):
        """Prints a final summary of the extraction run from the control table."""
        console.print("\n[bold green]🎉 Extraction Run Complete![/bold green]")
        console.print(f"⏱️ Total Duration: {duration:.2f}s")

        summary = self.conn.execute(
            "SELECT status, COUNT(*) FROM psa.pncp_extraction_tasks GROUP BY status"
        ).fetchall()
        console.print("\n[bold]📊 Task Status Summary:[/bold]")
        if not summary:
            console.print("  No tasks were processed in this run.")
            return

        status_map = {
            "COMPLETE": "[green]✔ Complete[/green]",
            "PARTIAL": "[yellow]⏳ Partial[/yellow]",
            "FAILED": "[red]✖ Failed[/red]",
            "PENDING": "[cyan]📋 Pending[/cyan]",
        }
        for status, count in summary:
            console.print(f"  {status_map.get(status, status)}: {count} tasks")

        failed_tasks = self.conn.execute(
            "SELECT task_id, last_error FROM psa.pncp_extraction_tasks WHERE status = 'FAILED' LIMIT 10"
        ).fetchall()
        if failed_tasks:
            console.print("\n[bold red]🔍 Sample of Failed Tasks:[/bold red]")
            for task_id, error in failed_tasks:
                console.print(f"  - {task_id}: {error[:100]}...")


# --- Typer CLI Application ---

app = typer.Typer(help="Task-oriented data extractor for the Brazilian PNCP portal.")


@app.command()
def extract(
    start_date: str = typer.Option("2021-01-01", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(
        lambda: date.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)"
    ),
    concurrency: int = typer.Option(CONCURRENCY, help="Number of concurrent requests"),
    force: bool = typer.Option(
        False, "--force", help="Force re-processing of tasks in the date range."
    ),
):
    """
    Extracts data from PNCP by planning, discovering, fetching, and reconciling tasks.
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        console.print("❌ Invalid date format. Use YYYY-MM-DD.", style="bold red")
        raise typer.Exit(1)

    async def main():
        extractor = TaskOrientedPNCPExtractor(concurrency=concurrency)
        await extractor.extract_data(start_dt, end_dt, force)

    asyncio.run(main())


@app.command()
def stats():
    """Shows high-level statistics from the database."""
    if not BALIZA_DB_PATH.exists():
        console.print("[red]Database file not found. Run 'extract' first.[/red]")
        return

    conn = duckdb.connect(str(BALIZA_DB_PATH), read_only=True)

    console.print("[bold]📊 Overall Database Statistics[/bold]")
    total_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses"
    ).fetchone()[0]
    console.print(f"  - Total Raw Pages Stored: {total_responses:,}")

    console.print("\n[bold]📋 Task Status Breakdown[/bold]")
    task_stats = conn.execute(
        "SELECT status, COUNT(*) as count FROM psa.pncp_extraction_tasks GROUP BY status ORDER BY count DESC"
    ).fetchall()
    if not task_stats:
        console.print("  No tasks found in the control table.")
    else:
        for status, count in task_stats:
            console.print(f"  - {status}: {count:,} tasks")

    failed_tasks = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'FAILED'"
    ).fetchone()[0]
    if failed_tasks > 0:
        console.print(
            f"\n[bold red]❗️ {failed_tasks:,} tasks have failed. Rerun extract or inspect the database.[/bold red]"
        )

    conn.close()


if __name__ == "__main__":
    app()
