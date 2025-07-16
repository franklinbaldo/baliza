"""
PNCP Data Extractor V2 - True Async Architecture
Based on steel-man pseudocode: endpoint → 365-day ranges → async pagination
"""

import asyncio
import calendar
import contextlib
import json
import random
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
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

console = Console()
logger = structlog.get_logger()


# JSON parsing with orjson fallback
def parse_json_robust(content: str) -> Any:
    """Parse JSON with orjson (fast) and fallback to stdlib json for edge cases."""
    try:
        return orjson.loads(content)
    except orjson.JSONDecodeError:
        # Fallback for NaN/Infinity or other edge cases
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            console.print(f"⚠️ JSON parse error: {e}")
            raise


# Configuration
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta"
CONCURRENCY = 2  # Concurrent requests limit
PAGE_SIZE = 500  # Maximum page size
REQUEST_TIMEOUT = 30
USER_AGENT = "BALIZA/3.0 (Backup Aberto de Licitacoes)"

# Data directory
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"

# Working endpoints (only the reliable ones)
PNCP_ENDPOINTS = [
    {
        "name": "contratos_publicacao",
        "path": "/v1/contratos",
        "description": "Contratos por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
    },
    {
        "name": "contratos_atualizacao",
        "path": "/v1/contratos/atualizacao",
        "description": "Contratos por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
    },
    {
        "name": "atas_periodo",
        "path": "/v1/atas",
        "description": "Atas de Registro de Preço por Período de Vigência",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
    },
    {
        "name": "atas_atualizacao",
        "path": "/v1/atas/atualizacao",
        "description": "Atas por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
    },
]


class AsyncPNCPExtractor:
    """True async PNCP extractor with semaphore back-pressure."""

    def __init__(self, concurrency: int = CONCURRENCY):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.run_id = str(uuid.uuid4())
        self.client = None

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

        self._init_database()

    def _init_database(self):
        """Initialize DuckDB with PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(BALIZA_DB_PATH))

        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")

        # Create raw responses table with ZSTD compression for response_content
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.pncp_raw_responses (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                request_parameters JSON,
                response_code INTEGER NOT NULL,
                response_content TEXT,
                response_headers JSON,
                data_date DATE,
                run_id VARCHAR,
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER
            ) WITH (compression = "zstd")
        """
        )

        # Create the new control table
        self.conn.execute(
            """
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
        """
        )

        # Create indexes if they don't exist
        self._create_indexes_if_not_exist()

        # Migrate existing table to use ZSTD compression
        self._migrate_to_zstd_compression()

    def _index_exists(self, index_name: str) -> bool:
        """Check if a given index exists in the database."""
        try:
            # This is a more generic way for DuckDB
            indexes = self.conn.execute(
                "PRAGMA index_list('psa.pncp_raw_responses')"
            ).fetchall()
            return any(idx[1] == index_name for idx in indexes)
        except Exception:
            return False  # Assume it doesn't exist if we can't check

    def _create_indexes_if_not_exist(self):
        """Create indexes only if they do not already exist."""
        indexes_to_create = {
            "idx_endpoint_date_page": "CREATE INDEX idx_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)",
            "idx_response_code": "CREATE INDEX idx_response_code ON psa.pncp_raw_responses(response_code)",
        }

        for idx_name, create_sql in indexes_to_create.items():
            if not self._index_exists(idx_name):
                try:
                    self.conn.execute(create_sql)
                    logger.info(f"Index '{idx_name}' created.")
                except Exception as e:
                    logger.exception(f"Failed to create index '{idx_name}'", error=e)

    async def __aenter__(self):
        """Async context manager entry."""
        await self._init_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        await self._graceful_shutdown()

    async def _graceful_shutdown(self):
        """Graceful shutdown of all connections and resources."""
        try:
            # Signal writer to stop gracefully
            if hasattr(self, "writer_running") and self.writer_running:
                await self.page_queue.put(None)  # Send sentinel

            # Close HTTP client
            if hasattr(self, "client") and self.client:
                await self.client.aclose()

            # Close database connection
            if hasattr(self, "conn") and self.conn:
                try:
                    self.conn.commit()  # Commit any pending changes
                    self.conn.close()
                except (duckdb.Error, AttributeError) as db_err:
                    logger.warning(
                        "Error during database cleanup", error=str(db_err)
                    )  # Log the specific error

            console.print("🔄 Graceful shutdown completed")

        except Exception as e:
            console.print(f"⚠️ Shutdown error: {e}")

    def _migrate_to_zstd_compression(self):
        """Migrate existing table to use ZSTD compression for better storage efficiency."""
        try:
            # Check if table exists and has data
            table_exists = (
                self.conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'pncp_raw_responses' AND table_schema = 'psa'"
                ).fetchone()[0]
                > 0
            )

            if not table_exists:
                return  # Table doesn't exist yet, will be created with ZSTD

            # Check if migration already happened by looking for a marker
            try:
                marker_exists = (
                    self.conn.execute(
                        "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE run_id = 'ZSTD_MIGRATION_MARKER'"
                    ).fetchone()[0]
                    > 0
                )

                if marker_exists:
                    return  # Migration already completed

            except (duckdb.Error, AttributeError) as db_err:
                logger.debug(
                    "Table might not exist or have run_id column yet", error=str(db_err)
                )

            # Check if table already has ZSTD compression by attempting to create a duplicate
            try:
                self.conn.execute(
                    """
                    CREATE TABLE psa.pncp_raw_responses_zstd (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        endpoint_url VARCHAR NOT NULL,
                        endpoint_name VARCHAR NOT NULL,
                        request_parameters JSON,
                        response_code INTEGER NOT NULL,
                        response_content TEXT,
                        response_headers JSON,
                        data_date DATE,
                        run_id VARCHAR,
                        total_records INTEGER,
                        total_pages INTEGER,
                        current_page INTEGER,
                        page_size INTEGER
                    ) WITH (compression = "zstd")
                """
                )

                # Check if we have data to migrate
                row_count = self.conn.execute(
                    "SELECT COUNT(*) FROM psa.pncp_raw_responses"
                ).fetchone()[0]

                if row_count > 0:
                    console.print(
                        f"🗜️ Migrating {row_count:,} rows to ZSTD compression..."
                    )

                    # Copy data to new compressed table
                    self.conn.execute(
                        """
                        INSERT INTO psa.pncp_raw_responses_zstd
                        SELECT * FROM psa.pncp_raw_responses
                    """
                    )

                    # Add migration marker
                    self.conn.execute(
                        """
                        INSERT INTO psa.pncp_raw_responses_zstd
                        (endpoint_url, endpoint_name, request_parameters, response_code, response_content, response_headers, run_id)
                        VALUES ('MIGRATION_MARKER', 'ZSTD_MIGRATION', '{}', 0, 'Migration completed', '{}', 'ZSTD_MIGRATION_MARKER')
                    """
                    )

                    # Drop old table and rename new one
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses")
                    self.conn.execute(
                        "ALTER TABLE psa.pncp_raw_responses_zstd RENAME TO pncp_raw_responses"
                    )

                    # Recreate indexes
                    self.conn.execute(
                        "CREATE INDEX idx_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)"
                    )
                    self.conn.execute(
                        "CREATE INDEX idx_response_code ON psa.pncp_raw_responses(response_code)"
                    )

                    self.conn.commit()
                    console.print("✅ Successfully migrated to ZSTD compression")
                else:
                    # No data to migrate, just replace table
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses")
                    self.conn.execute(
                        "ALTER TABLE psa.pncp_raw_responses_zstd RENAME TO pncp_raw_responses"
                    )
                    console.print("✅ Empty table replaced with ZSTD compression")

            except Exception as create_error:
                # If table already exists with ZSTD, clean up
                with contextlib.suppress(Exception):
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses_zstd")

                # This likely means the table already has ZSTD or migration already happened
                if "already exists" in str(create_error):
                    pass  # Expected, migration already done
                else:
                    raise

        except Exception as e:
            console.print(f"⚠️ ZSTD migration error: {e}")
            # Rollback on error
            with contextlib.suppress(Exception):
                self.conn.rollback()

    async def _init_client(self):
        """Initialize HTTP client with optimal settings and HTTP/2 fallback."""
        try:
            # Try with HTTP/2 first
            self.client = httpx.AsyncClient(
                base_url=PNCP_BASE_URL,
                timeout=REQUEST_TIMEOUT,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept-Encoding": "gzip, br",
                    "Accept": "application/json",
                },
                http2=True,
                limits=httpx.Limits(
                    max_connections=self.concurrency,
                    max_keepalive_connections=self.concurrency,
                ),
            )
            logger.info("HTTP/2 client initialized")

            # Verify HTTP/2 is actually working
            await self._verify_http2_status()

        except ImportError:
            # Fallback to HTTP/1.1 if h2 not available
            self.client = httpx.AsyncClient(
                base_url=PNCP_BASE_URL,
                timeout=REQUEST_TIMEOUT,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept-Encoding": "gzip, br",
                    "Accept": "application/json",
                },
                limits=httpx.Limits(
                    max_connections=self.concurrency,
                    max_keepalive_connections=self.concurrency,
                ),
            )
            logger.warning("HTTP/2 not available, using HTTP/1.1")

    async def _verify_http2_status(self):
        """Verify that HTTP/2 is actually being used."""
        try:
            # Make a test request to check protocol
            response = await self.client.get("/", timeout=5)

            # Check if HTTP/2 was actually used
            if hasattr(response, "http_version") and response.http_version == "HTTP/2":
                logger.info("HTTP/2 protocol confirmed")
            else:
                protocol = getattr(response, "http_version", "HTTP/1.1")
                logger.warning(
                    "Using protocol: {protocol} (fallback from HTTP/2)",
                    protocol=protocol,
                )

        except Exception as e:
            logger.exception("HTTP/2 verification failed", error=e)

    async def _complete_task_and_print(
        self, progress: Progress, task_id: int, final_message: str
    ):
        """Complete task and print final message, letting it scroll up."""
        # Update to final state
        progress.update(task_id, description=final_message)

        # Small delay to show final state
        await asyncio.sleep(0.5)

        # Print final message to console (will scroll up)
        console.print(f"✅ {final_message}")

        # Remove from progress after printing
        with contextlib.suppress(Exception):
            progress.remove_task(task_id)

    async def _fetch_with_backpressure(
        self, url: str, params: dict[str, Any], task_id: str | None = None
    ) -> dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    self.total_requests += 1
                    response = await self.client.get(url, params=params)

                    # Common success data
                    if response.status_code in [200, 204]:
                        self.successful_requests += 1
                        content_text = response.text
                        data = parse_json_robust(content_text) if content_text else {}
                        return {
                            "success": True,
                            "status_code": response.status_code,
                            "data": data,
                            "headers": dict(response.headers),
                            "total_records": data.get("totalRegistros", 0),
                            "total_pages": data.get("totalPaginas", 1),
                            "content": content_text,
                            "task_id": task_id,  # Pass through task_id
                            "url": url,
                            "params": params,
                        }

                    # Handle failures
                    self.failed_requests += 1
                    if 400 <= response.status_code < 500:
                        # Don't retry client errors
                        return {
                            "success": False,
                            "status_code": response.status_code,
                            "error": f"HTTP {response.status_code}",
                            "content": response.text,
                            "headers": dict(response.headers),
                            "task_id": task_id,
                        }

                    # Retry on 5xx or other transient errors
                    if attempt < 2:
                        delay = (2**attempt) * random.uniform(0.5, 1.5)  # noqa: S311
                        await asyncio.sleep(delay)
                        continue

                    # Final failure after retries
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "error": f"HTTP {response.status_code} after {attempt + 1} attempts",
                        "content": response.text,
                        "headers": dict(response.headers),
                        "task_id": task_id,
                    }

                except Exception as e:
                    if attempt < 2:
                        delay = (2**attempt) * random.uniform(0.5, 1.5)  # noqa: S311
                        await asyncio.sleep(delay)
                        continue

                    self.failed_requests += 1
                    return {
                        "success": False,
                        "status_code": 0,
                        "error": str(e),
                        "content": "",
                        "headers": {},
                        "task_id": task_id,
                    }

    def _format_date(self, date_obj: date) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")

    def _monthly_chunks(
        self, start_date: date, end_date: date
    ) -> list[tuple[date, date]]:
        """Generate monthly date chunks (start to end of each month)."""
        chunks = []
        current = start_date

        while current <= end_date:
            # Get the first day of the current month
            month_start = current.replace(day=1)

            # Get the last day of the current month
            _, last_day = calendar.monthrange(current.year, current.month)
            month_end = current.replace(day=last_day)

            # Adjust for actual start/end boundaries
            chunk_start = max(month_start, start_date)
            chunk_end = min(month_end, end_date)

            chunks.append((chunk_start, chunk_end))

            # Move to first day of next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        return chunks

    def _batch_store_responses(self, responses: list[dict[str, Any]]):
        """Store multiple responses in a single batch operation with transaction."""
        if not responses:
            return

        # Prepare batch data
        batch_data = []
        for resp in responses:
            batch_data.append(
                [
                    resp["endpoint_url"],
                    resp["endpoint_name"],
                    json.dumps(resp["request_parameters"]),
                    resp["response_code"],
                    resp["response_content"],
                    json.dumps(resp["response_headers"]),
                    resp["data_date"],
                    resp["run_id"],
                    resp["total_records"],
                    resp["total_pages"],
                    resp["current_page"],
                    resp["page_size"],
                ]
            )

        # Batch insert with transaction
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.executemany(
                """
                INSERT INTO psa.pncp_raw_responses (
                    endpoint_url, endpoint_name, request_parameters,
                    response_code, response_content, response_headers,
                    data_date, run_id, total_records, total_pages,
                    current_page, page_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                batch_data,
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    async def writer_worker(self, commit_every: int = 75) -> None:
        """Dedicated writer coroutine for single-threaded DB writes.

        Optimized for:
        - commit_every=75 pages ≈ 5-8 seconds between commits
        - Reduces I/O overhead by 7.5x (10→75 pages per commit)
        - Local batch buffer minimizes executemany() calls
        """
        counter = 0
        batch_buffer = []

        while True:
            try:
                # Get page from queue (None is sentinel to stop)
                page = await self.page_queue.get()
                if page is None:
                    break

                # Add to local buffer
                batch_buffer.append(page)
                counter += 1

                # Flush buffer every commit_every pages
                if counter % commit_every == 0 and batch_buffer:
                    self._batch_store_responses(batch_buffer)
                    self.conn.commit()
                    batch_buffer.clear()

                # Mark task as done
                self.page_queue.task_done()

            except Exception as e:
                console.print(f"❌ Writer error: {e}")
                self.page_queue.task_done()
                break

        # Flush any remaining items
        if batch_buffer:
            self._batch_store_responses(batch_buffer)
            self.conn.commit()

        self.writer_running = False

    def get_raw_content(self, endpoint_name: str, data_date: date, page: int) -> str:
        """Retrieve raw JSON content from database."""
        result = self.conn.execute(
            """
            SELECT response_content FROM psa.pncp_raw_responses
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
            LIMIT 1
        """,
            [endpoint_name, data_date, page],
        ).fetchone()

        if not result:
            raise ValueError(f"Page not found: {endpoint_name}, {data_date}, {page}")

        return result[0]

    async def _plan_tasks(self, start_date: date, end_date: date):
        """Phase 1: Populate the control table with all necessary tasks."""
        console.print("Phase 1: Planning tasks...")
        date_chunks = self._monthly_chunks(start_date, end_date)
        tasks_to_create = []
        for endpoint in PNCP_ENDPOINTS:
            for chunk_start, _ in date_chunks:
                task_id = f"{endpoint['name']}_{chunk_start.isoformat()}"
                tasks_to_create.append((task_id, endpoint["name"], chunk_start))

        if tasks_to_create:
            self.conn.executemany(
                """
                INSERT INTO psa.pncp_extraction_tasks (task_id, endpoint_name, data_date)
                VALUES (?, ?, ?)
                ON CONFLICT (endpoint_name, data_date) DO NOTHING
                """,
                tasks_to_create,
            )
            self.conn.commit()
        console.print(
            f"Planning complete. {len(tasks_to_create)} potential tasks identified."
        )

    async def _discover_tasks(self, progress: Progress):
        """Phase 2: Get metadata for all PENDING tasks."""
        pending_tasks = self.conn.execute(
            "SELECT task_id, endpoint_name, data_date FROM psa.pncp_extraction_tasks WHERE status = 'PENDING'"
        ).fetchall()

        if not pending_tasks:
            console.print("Phase 2: Discovery - No pending tasks to discover.")
            return

        discovery_progress = progress.add_task(
            "[cyan]Phase 2: Discovery", total=len(pending_tasks)
        )

        discovery_jobs = []
        for task_id, endpoint_name, data_date in pending_tasks:
            # Mark as DISCOVERING
            self.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status = 'DISCOVERING', updated_at = now() WHERE task_id = ?",
                [task_id],
            )

            endpoint = next(
                (ep for ep in PNCP_ENDPOINTS if ep["name"] == endpoint_name), None
            )
            if not endpoint:
                continue

            params = {
                "tamanhoPagina": PAGE_SIZE,
                "dataInicial": self._format_date(data_date),
                "dataFinal": self._format_date(
                    self._monthly_chunks(data_date, data_date)[0][1]
                ),  # end of month
                "pagina": 1,
            }
            discovery_jobs.append(
                self._fetch_with_backpressure(endpoint["path"], params, task_id=task_id)
            )

        self.conn.commit()  # Commit status change to DISCOVERING

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

                self.conn.execute(
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
                endpoint_name_part, data_date_str = task_id.split("_", 1)
                data_date = datetime.fromisoformat(data_date_str).date()

                page_1_response = {
                    "endpoint_url": f"{PNCP_BASE_URL}{response['url']}",
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
                    "page_size": PAGE_SIZE,
                }
                await self.page_queue.put(page_1_response)

            else:
                error_message = f"HTTP {response.get('status_code')}: {response.get('error', 'Unknown')}"
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'FAILED', last_error = ?, updated_at = now() WHERE task_id = ?",
                    [error_message, task_id],
                )

            self.conn.commit()
            progress.update(discovery_progress, advance=1)

    async def _fetch_page(self, endpoint_name: str, data_date: date, page_number: int):
        """Helper to fetch a single page and enqueue it."""
        endpoint = next(
            (ep for ep in PNCP_ENDPOINTS if ep["name"] == endpoint_name), None
        )
        if not endpoint:
            return

        params = {
            "tamanhoPagina": PAGE_SIZE,
            "dataInicial": self._format_date(data_date),
            "dataFinal": self._format_date(
                self._monthly_chunks(data_date, data_date)[0][1]
            ),
            "pagina": page_number,
        }

        response = await self._fetch_with_backpressure(endpoint["path"], params)

        # Enqueue the response for the writer worker
        page_response = {
            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
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
            "page_size": PAGE_SIZE,
        }
        await self.page_queue.put(page_response)
        return response

    async def _execute_tasks(self, progress: Progress):
        """Phase 3: Fetch all missing pages for FETCHING and PARTIAL tasks."""

        # Use unnest to get a list of all pages to fetch
        pages_to_fetch_query = """
SELECT t.task_id, t.endpoint_name, t.data_date, CAST(p.page_number AS INTEGER) as page_number
FROM psa.pncp_extraction_tasks t,
     unnest(json_extract_array(t.missing_pages)) AS p(page_number)
WHERE t.status IN ('FETCHING', 'PARTIAL');
        """
        pages_to_fetch = self.conn.execute(pages_to_fetch_query).fetchall()

        if not pages_to_fetch:
            console.print("Phase 3: Execution - No pages to fetch.")
            return

        execution_progress = progress.add_task(
            "[magenta]Phase 3: Execution", total=len(pages_to_fetch)
        )

        fetch_tasks = []
        for _task_id, endpoint_name, data_date, page_number in pages_to_fetch:
            fetch_tasks.append(self._fetch_page(endpoint_name, data_date, page_number))

        for future in asyncio.as_completed(fetch_tasks):
            await future  # we just wait for it to complete
            progress.update(execution_progress, advance=1)

    async def _reconcile_tasks(self):
        """Phase 4: Update task status based on downloaded data."""
        console.print("Phase 4: Reconciling tasks...")

        tasks_to_reconcile = self.conn.execute(
            "SELECT task_id, endpoint_name, data_date, total_pages FROM psa.pncp_extraction_tasks WHERE status IN ('FETCHING', 'PARTIAL')"
        ).fetchall()

        for task_id, endpoint_name, data_date, total_pages in tasks_to_reconcile:
            # Find out which pages were successfully downloaded for this task
            downloaded_pages_result = self.conn.execute(
                """
                SELECT DISTINCT current_page
                FROM psa.pncp_raw_responses
                WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
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
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'COMPLETE', missing_pages = '[]', updated_at = now() WHERE task_id = ?",
                    [task_id],
                )
            else:
                # Some pages are still missing
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'PARTIAL', missing_pages = ?, updated_at = now() WHERE task_id = ?",
                    [json.dumps(new_missing_pages), task_id],
                )

        self.conn.commit()
        console.print("Reconciliation complete.")

    async def extract_data(
        self, start_date: date, end_date: date, force: bool = False
    ) -> dict[str, Any]:
        """Main extraction method using a task-based, phased architecture."""
        logger.info(
            "extraction_started",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            concurrency=self.concurrency,
            run_id=self.run_id,
            force=force,
        )
        start_time = time.time()

        if force:
            console.print(
                "⚠️ [yellow]Force mode enabled - will reset tasks and re-extract all data.[/yellow]"
            )
            self.conn.execute("DELETE FROM psa.pncp_extraction_tasks")
            self.conn.execute("DELETE FROM psa.pncp_raw_responses")
            self.conn.commit()

        # Client initialized in __aenter__
        if not hasattr(self, "client") or self.client is None:
            await self._init_client()

        # Start writer worker
        self.writer_running = True
        writer_task = asyncio.create_task(self.writer_worker(commit_every=100))

        # --- Main Execution Flow ---

        # Phase 1: Planning
        await self._plan_tasks(start_date, end_date)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            # Phase 2: Discovery
            await self._discover_tasks(progress)

            # Phase 3: Execution
            await self._execute_tasks(progress)

        # Wait for writer to process all enqueued pages
        await self.page_queue.join()
        await self.page_queue.put(None)  # Send sentinel
        await writer_task

        # Phase 4: Reconciliation
        await self._reconcile_tasks()

        # --- Final Reporting ---
        duration = time.time() - start_time

        # Fetch final stats from the control table
        total_tasks = self.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks"
        ).fetchone()[0]
        complete_tasks = self.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE'"
        ).fetchone()[0]
        failed_tasks = self.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'FAILED'"
        ).fetchone()[0]

        # Fetch stats from raw responses
        total_records_sum = (
            self.conn.execute(
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
            f"📊 Total Tasks: {total_tasks:,} ({complete_tasks:,} complete, {failed_tasks:,} failed)"
        )
        console.print(f"📈 Total Records: {total_records_sum:,}")
        console.print(f"⏱️ Duration: {duration:.1f}s")

        await self.client.aclose()
        self.conn.execute("VACUUM")
        return total_results

    def __del__(self):
        """Cleanup."""
        if hasattr(self, "conn"):
            self.conn.close()


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
    start_date: str = typer.Option("2021-09-01", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(
        _get_current_month_end(), help="End date (YYYY-MM-DD)"
    ),
    concurrency: int = typer.Option(CONCURRENCY, help="Number of concurrent requests"),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
):
    """Extract data using true async architecture."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        console.print("❌ Invalid date format. Use YYYY-MM-DD", style="bold red")
        raise typer.Exit(1) from None

    if start_dt > end_dt:
        console.print("❌ Start date must be before end date", style="bold red")
        raise typer.Exit(1)

    async def main():
        async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
            results = await extractor.extract_data(start_dt, end_dt, force)

            # Save results
            results_file = (
                DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
            )
            with Path(results_file).open("w") as f:
                json.dump(results, f, indent=2, default=str)

            console.print(f"📄 Results saved to: {results_file}")

    asyncio.run(main())


@app.command()
def stats():
    """Show extraction statistics."""
    conn = duckdb.connect(str(BALIZA_DB_PATH))

    # Overall stats
    total_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses"
    ).fetchone()[0]
    success_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200"
    ).fetchone()[0]

    console.print(f"📊 Total Responses: {total_responses:,}")
    console.print(f"✅ Successful: {success_responses:,}")
    console.print(f"❌ Failed: {total_responses - success_responses:,}")

    if total_responses > 0:
        console.print(
            f"📈 Success Rate: {success_responses / total_responses * 100:.1f}%"
        )

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

    console.print("\n📋 Endpoint Statistics:")
    for name, responses, records in endpoint_stats:
        console.print(f"  {name}: {responses:,} responses, {records:,} records")

    conn.close()


if __name__ == "__main__":
    app()
