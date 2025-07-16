#!/usr/bin/env python3
"""
PNCP Data Extractor V2 - True Async Architecture
Based on steel-man pseudocode: endpoint → 365-day ranges → async pagination
"""

import asyncio
import json
import uuid
from datetime import datetime, date
import calendar
from pathlib import Path
from typing import Dict, Any, List, Tuple
import time

import duckdb
import httpx
import typer
import orjson
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)

console = Console()

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
CONCURRENCY = 8  # Concurrent requests limit
PAGE_SIZE = 500  # Maximum page size
REQUEST_TIMEOUT = 30
USER_AGENT = "BALIZA/3.0 (Backup Aberto de Licitacoes)"
# BATCH_FLUSH_SIZE removed - now using queue with maxsize=32

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

        # Queue-based processing (replaces batch buffer)
        self.page_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=32)
        self.writer_running = False

        # Retry queue for failed 5xx responses
        self.retry_queue = []
        self.retry_lock = asyncio.Lock()

        self._init_database()

    def _init_database(self):
        """Initialize DuckDB with PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(BALIZA_DB_PATH))

        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")

        # Create raw responses table with ZSTD compression for response_content
        self.conn.execute("""
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
        """)

        # Create indexes
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_response_code ON psa.pncp_raw_responses(response_code)"
        )
        
        # Migrate existing table to use ZSTD compression
        self._migrate_to_zstd_compression()

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
            if hasattr(self, 'writer_running') and self.writer_running:
                await self.page_queue.put(None)  # Send sentinel
                
            # Close HTTP client
            if hasattr(self, 'client') and self.client:
                await self.client.aclose()
                
            # Close database connection
            if hasattr(self, 'conn') and self.conn:
                try:
                    self.conn.commit()  # Commit any pending changes
                    self.conn.close()
                except Exception:
                    pass  # DB might already be closed
                    
            console.print("🔄 Graceful shutdown completed")
            
        except Exception as e:
            console.print(f"⚠️ Shutdown error: {e}")

    def _migrate_to_zstd_compression(self):
        """Migrate existing table to use ZSTD compression for better storage efficiency."""
        try:
            # Check if table exists and has data
            table_exists = self.conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'pncp_raw_responses' AND table_schema = 'psa'"
            ).fetchone()[0] > 0
            
            if not table_exists:
                return  # Table doesn't exist yet, will be created with ZSTD
            
            # Check if migration already happened by looking for a marker
            try:
                marker_exists = self.conn.execute(
                    "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE run_id = 'ZSTD_MIGRATION_MARKER'"
                ).fetchone()[0] > 0
                
                if marker_exists:
                    return  # Migration already completed
                    
            except Exception:
                pass  # Table might not exist or have run_id column yet
            
            # Check if table already has ZSTD compression by attempting to create a duplicate
            try:
                self.conn.execute("""
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
                """)
                
                # Check if we have data to migrate
                row_count = self.conn.execute("SELECT COUNT(*) FROM psa.pncp_raw_responses").fetchone()[0]
                
                if row_count > 0:
                    console.print(f"🗜️ Migrating {row_count:,} rows to ZSTD compression...")
                    
                    # Copy data to new compressed table
                    self.conn.execute("""
                        INSERT INTO psa.pncp_raw_responses_zstd 
                        SELECT * FROM psa.pncp_raw_responses
                    """)
                    
                    # Add migration marker
                    self.conn.execute("""
                        INSERT INTO psa.pncp_raw_responses_zstd 
                        (endpoint_url, endpoint_name, request_parameters, response_code, response_content, response_headers, run_id)
                        VALUES ('MIGRATION_MARKER', 'ZSTD_MIGRATION', '{}', 0, 'Migration completed', '{}', 'ZSTD_MIGRATION_MARKER')
                    """)
                    
                    # Drop old table and rename new one
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses")
                    self.conn.execute("ALTER TABLE psa.pncp_raw_responses_zstd RENAME TO pncp_raw_responses")
                    
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
                    self.conn.execute("ALTER TABLE psa.pncp_raw_responses_zstd RENAME TO pncp_raw_responses")
                    console.print("✅ Empty table replaced with ZSTD compression")
                    
            except Exception as create_error:
                # If table already exists with ZSTD, clean up
                try:
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses_zstd")
                except Exception:
                    pass
                
                # This likely means the table already has ZSTD or migration already happened
                if "already exists" in str(create_error):
                    pass  # Expected, migration already done
                else:
                    raise create_error
                    
        except Exception as e:
            console.print(f"⚠️ ZSTD migration error: {e}")
            # Rollback on error
            try:
                self.conn.rollback()
            except Exception:
                pass

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
            console.print("✅ HTTP/2 enabled")
            
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
            console.print("⚠️ HTTP/2 not available, using HTTP/1.1")

    async def _verify_http2_status(self):
        """Verify that HTTP/2 is actually being used."""
        try:
            # Make a test request to check protocol
            response = await self.client.get("/", timeout=5)
            
            # Check if HTTP/2 was actually used
            if hasattr(response, 'http_version') and response.http_version == "HTTP/2":
                console.print("🔗 HTTP/2 protocol confirmed")
            else:
                protocol = getattr(response, 'http_version', 'HTTP/1.1')
                console.print(f"⚠️ Using protocol: {protocol} (fallback from HTTP/2)")
                
        except Exception as e:
            console.print(f"⚠️ HTTP/2 verification failed: {e}")

    async def _complete_task_and_print(self, progress: Progress, task_id: int, final_message: str):
        """Complete task and print final message, letting it scroll up."""
        # Update to final state
        progress.update(task_id, description=final_message)
        
        # Small delay to show final state
        await asyncio.sleep(0.5)
        
        # Print final message to console (will scroll up)
        console.print(f"✅ {final_message}")
        
        # Remove from progress after printing
        try:
            progress.remove_task(task_id)
        except Exception:
            pass

    async def _fetch_with_backpressure(
        self, url: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    self.total_requests += 1
                    response = await self.client.get(url, params=params)

                    if response.status_code == 200:
                        self.successful_requests += 1
                        # Get text once and parse JSON from it
                        content_text = response.text
                        data = parse_json_robust(content_text)
                        return {
                            "success": True,
                            "status_code": response.status_code,
                            "data": data,
                            "headers": dict(response.headers),
                            "total_records": data.get("totalRegistros", 0),
                            "total_pages": data.get("totalPaginas", 1),
                            "content": content_text,
                        }
                    else:
                        # Log error but don't retry 4xx errors
                        if response.status_code >= 400 and response.status_code < 500:
                            self.failed_requests += 1
                            return {
                                "success": False,
                                "status_code": response.status_code,
                                "error": f"HTTP {response.status_code}",
                                "content": response.text,
                                "headers": dict(response.headers),
                            }

                        # Retry on 5xx errors
                        if attempt < 2:
                            await asyncio.sleep(2**attempt)  # Exponential backoff
                            continue

                        # Add to retry queue for later processing
                        # Extract endpoint name from URL and date from params
                        endpoint_name = url.split("/")[-1] if "/" in url else "unknown"
                        data_date = None
                        if "dataInicial" in params:
                            try:
                                date_str = params["dataInicial"]
                                data_date = datetime.strptime(date_str, "%Y%m%d").date()
                            except (ValueError, TypeError):
                                pass

                        await self._add_to_retry_queue(
                            url, params, response.status_code, endpoint_name, data_date
                        )

                        self.failed_requests += 1
                        return {
                            "success": False,
                            "status_code": response.status_code,
                            "error": f"HTTP {response.status_code} after {attempt + 1} attempts",
                            "content": response.text,
                            "headers": dict(response.headers),
                        }

                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2**attempt)
                        continue

                    self.failed_requests += 1
                    return {
                        "success": False,
                        "status_code": 0,
                        "error": str(e),
                        "content": "",
                        "headers": {},
                    }

    def _format_date(self, date_obj: date) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")

    def _monthly_chunks(
        self, start_date: date, end_date: date
    ) -> List[Tuple[date, date]]:
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

    def _check_page_exists(
        self, endpoint_name: str, data_date: date, page: int
    ) -> bool:
        """Check if a specific page already exists with success status."""
        result = self.conn.execute(
            """
            SELECT COUNT(*) FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
        """,
            [endpoint_name, data_date, page],
        ).fetchone()

        return (result[0] if result else 0) > 0

    def _check_any_page_exists(self, endpoint_name: str, data_date: date) -> bool:
        """Check if ANY page exists for this endpoint/date combination."""
        result = self.conn.execute(
            """
            SELECT COUNT(*) FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
        """,
            [endpoint_name, data_date],
        ).fetchone()

        return (result[0] if result else 0) > 0

    def _get_existing_pages_info(
        self, endpoint_name: str, data_date: date
    ) -> Dict[str, Any] | None:
        """Get info about existing pages, inferring total_pages from any successful page."""
        result = self.conn.execute(
            """
            SELECT total_pages, total_records, current_page
            FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
            ORDER BY current_page
            LIMIT 1
        """,
            [endpoint_name, data_date],
        ).fetchone()

        if result:
            return {
                "total_pages": result[0],
                "total_records": result[1],
                "current_page": result[2],
            }
        return None

    def _get_existing_page_info(
        self, endpoint_name: str, data_date: date, page: int
    ) -> Dict[str, Any] | None:
        """Get information about an existing page (optimized - no response_content)."""
        result = self.conn.execute(
            """
            SELECT total_pages, total_records
            FROM psa.pncp_raw_responses 
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
            LIMIT 1
        """,
            [endpoint_name, data_date, page],
        ).fetchone()

        if result:
            return {
                "total_pages": result[0],
                "total_records": result[1],
            }
        return None

    async def _fetch_missing_pages(
        self,
        endpoint: Dict[str, Any],
        start_date: date,
        end_date: date,
        missing_pages: List[int],
        total_pages: int,
        progress: Progress,
        task_id: int,
        results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Fetch only the missing pages for a partially complete range."""
        base_params = {
            "tamanhoPagina": PAGE_SIZE,
            "dataInicial": self._format_date(start_date),
            "dataFinal": self._format_date(end_date),
        }

        # Get total records from existing first page
        existing_first_page = self._get_existing_page_info(
            endpoint["name"], start_date, 1
        )
        if existing_first_page:
            results["total_records"] = existing_first_page.get("total_records", 0)

        # Update progress with correct counts
        results["pages_skipped"] = total_pages - len(missing_pages)

        # Fetch missing pages concurrently
        if missing_pages:
            page_tasks = []
            for page in missing_pages:
                page_params = base_params.copy()
                page_params["pagina"] = page
                page_tasks.append(
                    self._fetch_with_backpressure(endpoint["path"], page_params)
                )

            # Wait for all missing pages
            page_responses = await asyncio.gather(*page_tasks)

            # Process results
            for page_num, response in zip(missing_pages, page_responses):
                results["total_requests"] += 1

                if response["success"]:
                    results["successful_requests"] += 1
                    results["pages_processed"] += 1

                    # Add to queue
                    success_response = {
                        "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
                        "endpoint_name": endpoint["name"],
                        "request_parameters": {**base_params, "pagina": page_num},
                        "response_code": response["status_code"],
                        "response_content": response["content"],
                        "response_headers": response["headers"],
                        "data_date": start_date,
                        "run_id": self.run_id,
                        "total_records": response.get("total_records", 0),
                        "total_pages": response.get("total_pages", total_pages),
                        "current_page": page_num,
                        "page_size": PAGE_SIZE,
                    }
                    await self.page_queue.put(success_response)
                else:
                    results["failed_requests"] += 1

                    # Store failed response too
                    failed_response = {
                        "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
                        "endpoint_name": endpoint["name"],
                        "request_parameters": {**base_params, "pagina": page_num},
                        "response_code": response["status_code"],
                        "response_content": response["content"],
                        "response_headers": response["headers"],
                        "data_date": start_date,
                        "run_id": self.run_id,
                        "total_records": 0,
                        "total_pages": total_pages,
                        "current_page": page_num,
                        "page_size": PAGE_SIZE,
                    }
                    await self.page_queue.put(failed_response)

                progress.update(task_id, advance=1)

        # All responses are now handled by the queue/writer worker
        
        # Complete task and let it scroll up
        final_message = f"{endpoint['name']} {start_date} to {end_date} - Completed {results['pages_processed']} pages"
        asyncio.create_task(self._complete_task_and_print(progress, task_id, final_message))

        return results

    
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
                if counter % commit_every == 0:
                    if batch_buffer:
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

    async def _process_endpoint_batched(
        self,
        endpoint: Dict[str, Any],
        date_chunks: List[Tuple[date, date]],
        progress: Progress,
        force: bool,
        batch_size: int = 3,  # Process 3 months at a time per endpoint
    ) -> List[Dict[str, Any]]:
        """Process date ranges for a single endpoint in batches."""
        all_results = []
        
        # Process date ranges in smaller batches
        for i in range(0, len(date_chunks), batch_size):
            batch_chunks = date_chunks[i:i + batch_size]
            
            # Create tasks for this batch of date ranges
            batch_tasks = []
            for chunk_start, chunk_end in batch_chunks:
                task_id = progress.add_task(
                    f"[blue]{endpoint['name']}[/blue] {chunk_start} to {chunk_end}",
                    total=1,
                )

                task = self._crawl_endpoint_range(
                    endpoint, chunk_start, chunk_end, progress, task_id, force
                )
                batch_tasks.append(task)

            # Run this batch of date ranges concurrently
            batch_results = await asyncio.gather(*batch_tasks)
            all_results.extend(batch_results)
            
            # Small delay between batches to be nice to the API
            if i + batch_size < len(date_chunks):
                await asyncio.sleep(0.5)  # Shorter delay since endpoints run concurrently
        
        return all_results
    
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
    
    # Old batch buffer system removed - now using queue/writer worker

    async def _add_to_retry_queue(
        self,
        url: str,
        params: Dict[str, Any],
        status_code: int,
        endpoint_name: str = "",
        data_date: date = None,
    ):
        """Add failed 5xx request to retry queue with metadata."""
        async with self.retry_lock:
            self.retry_queue.append(
                {
                    "url": url,
                    "params": params,
                    "status_code": status_code,
                    "endpoint_name": endpoint_name,
                    "data_date": data_date,
                    "attempts": 0,
                    "timestamp": time.time(),
                }
            )

    async def _process_retry_queue(self) -> int:
        """Process retry queue and return number of successful retries."""
        if not self.retry_queue:
            return 0

        successful_retries = 0

        async with self.retry_lock:
            retry_tasks = []
            for retry_item in self.retry_queue:
                if retry_item["attempts"] < 3:  # Max 3 retry attempts
                    retry_tasks.append(self._retry_request(retry_item))

            if retry_tasks:
                retry_results = await asyncio.gather(
                    *retry_tasks, return_exceptions=True
                )

                # Process results and persist successful retries
                for i, result in enumerate(retry_results):
                    if isinstance(result, dict) and result.get("success"):
                        successful_retries += 1

                        # Create response object for database storage
                        retry_item = (
                            self.retry_queue[i] if i < len(self.retry_queue) else {}
                        )
                        retry_response = {
                            "endpoint_url": retry_item.get("url", ""),
                            "endpoint_name": retry_item.get("endpoint_name", "unknown"),
                            "request_parameters": retry_item.get("params", {}),
                            "response_code": result["status_code"],
                            "response_content": result["content"],
                            "response_headers": result["headers"],
                            "data_date": retry_item.get("data_date"),
                            "run_id": self.run_id,
                            "total_records": result.get("total_records", 0),
                            "total_pages": result.get("total_pages", 1),
                            "current_page": retry_item.get("params", {}).get(
                                "pagina", 1
                            ),
                            "page_size": retry_item.get("params", {}).get(
                                "tamanhoPagina", PAGE_SIZE
                            ),
                        }

                        # Add to queue for persistence
                        await self.page_queue.put(retry_response)

            # Clear processed items
            self.retry_queue.clear()

        return successful_retries

    async def _retry_request(self, retry_item: Dict[str, Any]) -> Dict[str, Any]:
        """Retry a single failed request without adding to retry queue."""
        retry_item["attempts"] += 1

        # Exponential backoff based on attempts
        await asyncio.sleep(2 ** retry_item["attempts"])

        # Direct HTTP request without retry queue
        async with self.semaphore:
            try:
                self.total_requests += 1
                response = await self.client.get(
                    retry_item["url"], params=retry_item["params"]
                )

                if response.status_code == 200:
                    self.successful_requests += 1
                    data = parse_json_robust(response.text)
                    return {
                        "success": True,
                        "status_code": response.status_code,
                        "data": data,
                        "headers": dict(response.headers),
                        "total_records": data.get("totalRegistros", 0),
                        "total_pages": data.get("totalPaginas", 1),
                        "content": response.text,
                    }
                else:
                    self.failed_requests += 1
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "error": f"HTTP {response.status_code}",
                        "content": response.text,
                        "headers": dict(response.headers),
                    }
            except Exception as e:
                self.failed_requests += 1
                return {
                    "success": False,
                    "status_code": 0,
                    "error": str(e),
                    "content": "",
                    "headers": {},
                }

    def _batch_store_responses(self, responses: List[Dict[str, Any]]):
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
        except Exception as e:
            self.conn.execute("ROLLBACK")
            raise e

    async def _crawl_endpoint_range(
        self,
        endpoint: Dict[str, Any],
        start_date: date,
        end_date: date,
        progress: Progress,
        task_id: int,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Crawl a single endpoint for a date range."""
        results = {
            "endpoint_name": endpoint["name"],
            "start_date": start_date,
            "end_date": end_date,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_records": 0,
            "pages_processed": 0,
            "pages_skipped": 0,
        }

        # Build base parameters
        base_params = {
            "tamanhoPagina": PAGE_SIZE,
            "dataInicial": self._format_date(start_date),
            "dataFinal": self._format_date(end_date),
        }

        # Step 1: Fetch first page to discover total pages
        first_page_params = base_params.copy()
        first_page_params["pagina"] = 1

        # Check if we should skip this range completely or handle partial data
        # Check if ANY page exists for this endpoint/date combination
        if not force and self._check_any_page_exists(endpoint["name"], start_date):
            # Get metadata from any existing page (can infer total_pages from any page)
            existing_pages_info = self._get_existing_pages_info(
                endpoint["name"], start_date
            )
            if existing_pages_info:
                total_pages = existing_pages_info.get("total_pages", 1)

                # Check if we have all pages for this range
                missing_pages = []
                for page in range(1, total_pages + 1):
                    if not self._check_page_exists(endpoint["name"], start_date, page):
                        missing_pages.append(page)

                if not missing_pages:
                    # We have all pages, skip completely
                    total_records = existing_pages_info.get("total_records", 0)
                    final_message = f"{endpoint['name']} {start_date} to {end_date} - Skipping (complete) {total_records:,} records"
                    
                    progress.update(
                        task_id,
                        total=total_pages,
                        completed=total_pages,
                        description=f"[blue]{endpoint['name']}[/blue] {start_date} to {end_date} - Skipping (complete) {total_records:,} records",
                    )
                    
                    # Complete task and let it scroll up
                    asyncio.create_task(self._complete_task_and_print(progress, task_id, final_message))
                    
                    results["pages_skipped"] += total_pages
                    results["total_records"] = total_records
                    return results
                else:
                    # We have partial data, fetch missing pages (including page 1 if needed)
                    progress.update(
                        task_id,
                        total=total_pages,
                        description=f"[yellow]{endpoint['name']}[/yellow] {start_date} to {end_date} - Partial ({len(missing_pages)} missing)",
                    )

                    # Set total records from existing data
                    results["total_records"] = existing_pages_info.get(
                        "total_records", 0
                    )

                    # Fetch missing pages (could include page 1)
                    return await self._fetch_missing_pages(
                        endpoint,
                        start_date,
                        end_date,
                        missing_pages,
                        total_pages,
                        progress,
                        task_id,
                        results,
                    )
            else:
                # Some pages exist but we can't get metadata info, skip with warning
                # Count how many pages actually exist
                actual_pages = self.conn.execute(
                    "SELECT COUNT(DISTINCT current_page) FROM psa.pncp_raw_responses WHERE endpoint_name = ? AND data_date = ? AND response_code = 200",
                    [endpoint["name"], start_date]
                ).fetchone()[0]
                
                estimated_pages = max(1, actual_pages)  # At least 1 page exists
                final_message = f"{endpoint['name']} {start_date} to {end_date} - Skipping (partial data, no metadata)"
                
                progress.update(
                    task_id,
                    total=estimated_pages,
                    completed=estimated_pages,
                    description=f"[blue]{endpoint['name']}[/blue] {start_date} to {end_date} - Skipping (partial data, no metadata)",
                )
                
                # Complete task and let it scroll up
                asyncio.create_task(self._complete_task_and_print(progress, task_id, final_message))
                
                results["pages_skipped"] += estimated_pages
                return results

        # Fetch first page
        first_response = await self._fetch_with_backpressure(
            endpoint["path"], first_page_params
        )
        results["total_requests"] += 1

        if not first_response["success"]:
            results["failed_requests"] += 1
            
            # Get error details for debugging
            error_code = first_response.get("status_code", 0)
            error_msg = first_response.get("error", "Unknown error")
            
            final_message = f"{endpoint['name']} {start_date} to {end_date} - Failed (HTTP {error_code}: {error_msg})"
            
            progress.update(
                task_id,
                description=f"[red]{endpoint['name']}[/red] {start_date} to {end_date} - Failed (HTTP {error_code})",
            )
            
            # Complete failed task and let it scroll up
            asyncio.create_task(self._complete_task_and_print(progress, task_id, final_message))
            
            return results

        total_pages = first_response["total_pages"]
        total_records = first_response["total_records"]
        results["successful_requests"] += 1
        results["total_records"] = total_records

        # Update progress bar with real total
        progress.update(
            task_id,
            total=total_pages,
            description=f"[green]{endpoint['name']}[/green] {start_date} to {end_date} - {total_records:,} records",
        )

        # Store first page with batch buffering
        first_page_response = {
            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
            "endpoint_name": endpoint["name"],
            "request_parameters": first_page_params,
            "response_code": first_response["status_code"],
            "response_content": first_response["content"],
            "response_headers": first_response["headers"],
            "data_date": start_date,
            "run_id": self.run_id,
            "total_records": first_response["total_records"],
            "total_pages": first_response["total_pages"],
            "current_page": 1,
            "page_size": PAGE_SIZE,
        }

        await self.page_queue.put(first_page_response)

        results["pages_processed"] += 1
        progress.update(task_id, advance=1)

        # Step 2: Fetch remaining pages concurrently
        if total_pages > 1:
            # Determine which pages to fetch
            pages_to_fetch = []
            for page in range(2, total_pages + 1):
                if force or not self._check_page_exists(
                    endpoint["name"], start_date, page
                ):
                    pages_to_fetch.append(page)
                else:
                    results["pages_skipped"] += 1
                    progress.update(task_id, advance=1)

            # Fetch pages concurrently
            if pages_to_fetch:
                page_tasks = []
                for page in pages_to_fetch:
                    page_params = base_params.copy()
                    page_params["pagina"] = page
                    page_tasks.append(
                        self._fetch_with_backpressure(endpoint["path"], page_params)
                    )

                # Wait for all pages
                page_responses = await asyncio.gather(*page_tasks)

                # Process results
                for page_num, response in zip(pages_to_fetch, page_responses):
                    results["total_requests"] += 1

                    if response["success"]:
                        results["successful_requests"] += 1
                        results["pages_processed"] += 1

                        # Add to queue
                        success_response = {
                            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
                            "endpoint_name": endpoint["name"],
                            "request_parameters": {**base_params, "pagina": page_num},
                            "response_code": response["status_code"],
                            "response_content": response["content"],
                            "response_headers": response["headers"],
                            "data_date": start_date,
                            "run_id": self.run_id,
                            "total_records": response.get("total_records", 0),
                            "total_pages": response.get("total_pages", total_pages),
                            "current_page": page_num,
                            "page_size": PAGE_SIZE,
                        }
                        await self.page_queue.put(success_response)
                    else:
                        results["failed_requests"] += 1

                        # Store failed response too
                        failed_response = {
                            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
                            "endpoint_name": endpoint["name"],
                            "request_parameters": {**base_params, "pagina": page_num},
                            "response_code": response["status_code"],
                            "response_content": response["content"],
                            "response_headers": response["headers"],
                            "data_date": start_date,
                            "run_id": self.run_id,
                            "total_records": 0,
                            "total_pages": total_pages,
                            "current_page": page_num,
                            "page_size": PAGE_SIZE,
                        }
                        await self.page_queue.put(failed_response)

                    progress.update(task_id, advance=1)

        # All responses are now handled by the queue/writer worker
        
        # Complete task and let it scroll up
        final_message = f"{endpoint['name']} {start_date} to {end_date} - Completed {results['pages_processed']} pages"
        asyncio.create_task(self._complete_task_and_print(progress, task_id, final_message))

        return results

    async def extract_data(
        self, start_date: date, end_date: date, force: bool = False
    ) -> Dict[str, Any]:
        """Main extraction method with true async architecture."""
        console.print("🚀 Starting Async PNCP Extraction")
        console.print(f"📅 Date Range: {start_date} to {end_date}")
        console.print(f"🔧 Concurrency: {self.concurrency}")
        console.print(f"🆔 Run ID: {self.run_id}")

        if force:
            console.print(
                "⚠️ [yellow]Force mode enabled - will re-extract existing data[/yellow]"
            )

        # Client initialized in __aenter__ if using context manager
        if not hasattr(self, 'client') or self.client is None:
            await self._init_client()

        start_time = time.time()

        # Start writer worker with optimized commit frequency
        self.writer_running = True
        writer_task = asyncio.create_task(self.writer_worker(commit_every=75))

        # Process endpoints concurrently but limit date ranges within each endpoint
        date_chunks = self._monthly_chunks(start_date, end_date)
        
        # Create progress bars
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
            # Create endpoint workers that process date ranges in batches
            endpoint_workers = []
            for endpoint in PNCP_ENDPOINTS:
                worker = self._process_endpoint_batched(
                    endpoint, date_chunks, progress, force
                )
                endpoint_workers.append(worker)
            
            # Run all endpoint workers concurrently
            endpoint_results = await asyncio.gather(*endpoint_workers)
            
            # Flatten results from all endpoints
            results = []
            for endpoint_result in endpoint_results:
                results.extend(endpoint_result)

        # Wait for all pages to be processed by writer
        await self.page_queue.join()
        
        # Stop writer worker
        await self.page_queue.put(None)  # Send sentinel
        await writer_task  # Wait for writer to finish

        # Process retry queue for failed 5xx responses
        retry_successes = await self._process_retry_queue()

        # Aggregate results
        total_results = {
            "run_id": self.run_id,
            "start_date": start_date,
            "end_date": end_date,
            "total_requests": sum(r["total_requests"] for r in results),
            "successful_requests": sum(r["successful_requests"] for r in results),
            "failed_requests": sum(r["failed_requests"] for r in results),
            "retry_successes": retry_successes,
            "total_records": sum(r["total_records"] for r in results),
            "pages_processed": sum(r["pages_processed"] for r in results),
            "pages_skipped": sum(r["pages_skipped"] for r in results),
            "duration": time.time() - start_time,
            "endpoints": results,
        }

        # Print summary
        console.print("\n🎉 Extraction Complete!")
        console.print(f"📊 Total Requests: {total_results['total_requests']:,}")
        console.print(f"✅ Successful: {total_results['successful_requests']:,}")
        console.print(f"❌ Failed: {total_results['failed_requests']:,}")
        console.print(f"🔄 Retry Successes: {total_results['retry_successes']:,}")
        console.print(f"📈 Total Records: {total_results['total_records']:,}")
        console.print(f"⏱️ Duration: {total_results['duration']:.1f}s")
        console.print(
            f"🚀 Avg RPS: {total_results['total_requests'] / total_results['duration']:.2f}"
        )

        # Close client
        await self.client.aclose()

        # Compact database after extraction
        console.print("🗜️ Compacting database...")
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
        raise typer.Exit(1)

    if start_dt > end_dt:
        console.print("❌ Start date must be before end date", style="bold red")
        raise typer.Exit(1)

    async def main():
        async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
            results = await extractor.extract_data(start_dt, end_dt, force)

            # Save results
            results_file = DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
            with open(results_file, "w") as f:
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
    endpoint_stats = conn.execute("""
        SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM psa.pncp_raw_responses 
        WHERE response_code = 200
        GROUP BY endpoint_name
        ORDER BY total_records DESC
    """).fetchall()

    console.print("\n📋 Endpoint Statistics:")
    for name, responses, records in endpoint_stats:
        console.print(f"  {name}: {responses:,} responses, {records:,} records")

    conn.close()


if __name__ == "__main__":
    app()
