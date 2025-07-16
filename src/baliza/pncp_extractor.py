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
import hashlib

import duckdb
import httpx
import typer
import zstandard as zstd
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)

console = Console()

# Configuration
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta"
CONCURRENCY = 8  # Concurrent requests limit
PAGE_SIZE = 500  # Maximum page size
REQUEST_TIMEOUT = 30
USER_AGENT = "BALIZA/3.0 (Backup Aberto de Licitacoes)"
# BATCH_FLUSH_SIZE removed - now using queue with maxsize=32

# Data directory
DATA_DIR = Path.cwd() / "data"
RAW_DIR = DATA_DIR / "raw"
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
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(BALIZA_DB_PATH))

        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")

        # Create lightweight metadata table (NEW)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS psa.pncp_meta (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                endpoint_url VARCHAR NOT NULL,
                endpoint_name VARCHAR NOT NULL,
                request_parameters JSON,
                response_code INTEGER NOT NULL,
                response_headers JSON,
                data_date DATE,
                run_id VARCHAR,
                total_records INTEGER,
                total_pages INTEGER,
                current_page INTEGER,
                page_size INTEGER,
                path_raw VARCHAR NOT NULL,
                bytes_raw INTEGER NOT NULL,
                sha256_raw CHAR(64) NOT NULL
            )
        """)

        # Create indexes for fast metadata queries
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_fast ON psa.pncp_meta(endpoint_name, data_date, current_page)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_response_code ON psa.pncp_meta(response_code)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_sha256 ON psa.pncp_meta(sha256_raw)"
        )
        
        # Keep old table for backward compatibility (will be deprecated)
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
            )
        """)

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
                        data = json.loads(content_text)
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
            SELECT COUNT(*) FROM psa.pncp_meta 
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
        """,
            [endpoint_name, data_date, page],
        ).fetchone()

        return (result[0] if result else 0) > 0

    def _check_any_page_exists(self, endpoint_name: str, data_date: date) -> bool:
        """Check if ANY page exists for this endpoint/date combination."""
        result = self.conn.execute(
            """
            SELECT COUNT(*) FROM psa.pncp_meta 
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
            FROM psa.pncp_meta 
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
            FROM psa.pncp_meta 
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

        return results

    def _store_page_compressed(self, page: Dict[str, Any]) -> Dict[str, Any]:
        """Store JSON content in compressed file, return metadata."""
        # Extract JSON content
        raw_content = page.get("response_content", "")
        raw_bytes = raw_content.encode('utf-8')
        
        # Calculate hash for deduplication
        sha256_hash = hashlib.sha256(raw_bytes).hexdigest()
        
        # Create hierarchical path: raw/endpoint=.../year=.../month=.../
        data_date = page["data_date"]
        rel_path = (
            f"raw/endpoint={page['endpoint_name']}/"
            f"year={data_date.year:04d}/"
            f"month={data_date.month:02d}/"
            f"page_{page['current_page']:06d}_{sha256_hash[:8]}.zst"
        )
        
        abs_path = DATA_DIR / rel_path
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Compress and write file
        compressed_data = self.compressor.compress(raw_bytes)
        abs_path.write_bytes(compressed_data)
        
        # Return metadata (without the large JSON)
        metadata = {
            k: v for k, v in page.items() 
            if k != "response_content"  # Remove large JSON
        }
        metadata.update({
            "path_raw": rel_path,
            "bytes_raw": len(raw_bytes),
            "sha256_raw": sha256_hash,
        })
        
        return metadata
    
    async def writer_worker(self, commit_every: int = 75) -> None:
        """Dedicated writer coroutine for single-threaded DB writes.
        
        Optimized for:
        - commit_every=75 pages ≈ 5-8 seconds between commits
        - Reduces I/O overhead by 7.5x (10→75 pages per commit)
        - JSON stored in compressed files (~30-100KB vs 300-400KB)
        - DuckDB only stores lightweight metadata
        """
        counter = 0
        batch_buffer = []
        
        while True:
            try:
                # Get page from queue (None is sentinel to stop)
                page = await self.page_queue.get()
                if page is None:
                    break
                
                # Store JSON in compressed file, get metadata
                metadata = self._store_page_compressed(page)
                
                # Add metadata to local buffer
                batch_buffer.append(metadata)
                counter += 1
                
                # Flush buffer every commit_every pages
                if counter % commit_every == 0:
                    if batch_buffer:
                        self._batch_store_metadata(batch_buffer)
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
            self._batch_store_metadata(batch_buffer)
            self.conn.commit()
        
        self.writer_running = False

    def get_raw_content(self, endpoint_name: str, data_date: date, page: int) -> str:
        """Retrieve raw JSON content from compressed file."""
        result = self.conn.execute(
            """
            SELECT path_raw FROM psa.pncp_meta 
            WHERE endpoint_name = ? AND data_date = ? AND current_page = ? AND response_code = 200
            LIMIT 1
        """,
            [endpoint_name, data_date, page],
        ).fetchone()
        
        if not result:
            raise ValueError(f"Page not found: {endpoint_name}, {data_date}, {page}")
        
        # Read and decompress file
        file_path = DATA_DIR / result[0]
        if not file_path.exists():
            raise FileNotFoundError(f"Raw file not found: {file_path}")
        
        compressed_data = file_path.read_bytes()
        decompressor = zstd.ZstdDecompressor()
        raw_bytes = decompressor.decompress(compressed_data)
        
        return raw_bytes.decode('utf-8')
    
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
                    data = response.json()
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

    def _batch_store_metadata(self, metadata_list: List[Dict[str, Any]]):
        """Store multiple metadata records in a single batch operation with transaction."""
        if not metadata_list:
            return

        # Prepare batch data for metadata table
        batch_data = []
        for meta in metadata_list:
            batch_data.append(
                [
                    meta["endpoint_url"],
                    meta["endpoint_name"],
                    json.dumps(meta["request_parameters"]),
                    meta["response_code"],
                    json.dumps(meta["response_headers"]),
                    meta["data_date"],
                    meta["run_id"],
                    meta["total_records"],
                    meta["total_pages"],
                    meta["current_page"],
                    meta["page_size"],
                    meta["path_raw"],
                    meta["bytes_raw"],
                    meta["sha256_raw"],
                ]
            )

        # Batch insert with transaction
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.executemany(
                """
                INSERT INTO psa.pncp_meta (
                    endpoint_url, endpoint_name, request_parameters,
                    response_code, response_headers, data_date, run_id,
                    total_records, total_pages, current_page, page_size,
                    path_raw, bytes_raw, sha256_raw
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                batch_data,
            )
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            raise e
            
    def _batch_store_responses(self, responses: List[Dict[str, Any]]):
        """DEPRECATED: Store multiple responses in old format (backward compatibility)."""
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
                    progress.update(
                        task_id,
                        description=f"[blue]{endpoint['name']}[/blue] {start_date} to {end_date} - Skipping (complete)",
                    )
                    results["pages_skipped"] += total_pages
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
                progress.update(
                    task_id,
                    description=f"[blue]{endpoint['name']}[/blue] {start_date} to {end_date} - Skipping (partial data, no metadata)",
                )
                results["pages_skipped"] += 1
                return results

        # Fetch first page
        first_response = await self._fetch_with_backpressure(
            endpoint["path"], first_page_params
        )
        results["total_requests"] += 1

        if not first_response["success"]:
            results["failed_requests"] += 1
            progress.update(
                task_id,
                description=f"[red]{endpoint['name']}[/red] {start_date} to {end_date} - Failed",
            )
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

        # Initialize client
        await self._init_client()
        
        # Initialize zstandard compressor
        self.compressor = zstd.ZstdCompressor(level=3)

        start_time = time.time()

        # Start writer worker with optimized commit frequency
        self.writer_running = True
        writer_task = asyncio.create_task(self.writer_worker(commit_every=75))

        # Process one endpoint at a time to avoid overwhelming the API
        all_results = []
        
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
            # Process each endpoint sequentially
            for endpoint in PNCP_ENDPOINTS:
                console.print(f"\n🔄 Processing endpoint: {endpoint['name']}")
                
                # Get monthly chunks for this endpoint
                date_chunks = self._monthly_chunks(start_date, end_date)
                
                # Process date ranges in smaller batches to avoid overwhelming API
                batch_size = 6  # Process 6 months at a time
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
                        await asyncio.sleep(1)
            
            results = all_results

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
    start_date: str = typer.Option("2021-01-01", help="Start date (YYYY-MM-DD)"),
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
        extractor = AsyncPNCPExtractor(concurrency=concurrency)
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

    # Check if new metadata table exists
    has_meta_table = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'pncp_meta' AND table_schema = 'psa'"
    ).fetchone()[0] > 0
    
    if has_meta_table:
        # Use new metadata table
        table_name = "psa.pncp_meta"
        console.print("📊 Using new metadata table (compressed storage)")
        
        # Calculate disk savings
        total_bytes = conn.execute(
            "SELECT SUM(bytes_raw) FROM psa.pncp_meta"
        ).fetchone()[0] or 0
        
        if total_bytes > 0:
            console.print(f"💾 Raw JSON size: {total_bytes / 1024 / 1024:.1f} MB")
            console.print(f"🗜️ Compressed files: ~{total_bytes * 0.3 / 1024 / 1024:.1f} MB (70% compression)")
    else:
        # Fallback to old table
        table_name = "psa.pncp_raw_responses"
        console.print("📊 Using legacy table (full JSON storage)")

    # Overall stats
    total_responses = conn.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]
    success_responses = conn.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE response_code = 200"
    ).fetchone()[0]

    console.print(f"📊 Total Responses: {total_responses:,}")
    console.print(f"✅ Successful: {success_responses:,}")
    console.print(f"❌ Failed: {total_responses - success_responses:,}")

    if total_responses > 0:
        console.print(
            f"📈 Success Rate: {success_responses / total_responses * 100:.1f}%"
        )

    # Endpoint breakdown
    endpoint_stats = conn.execute(f"""
        SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM {table_name} 
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
