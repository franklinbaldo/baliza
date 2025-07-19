"""PNCP Writer - DuckDB Single-Writer Architecture with Split Tables

Implements centralized write operations to DuckDB as specified in ADR-001 and ADR-006.
Uses split table architecture for content deduplication and optimized storage.
Ensures single-writer constraint through file locking and centralized write operations.
Manages raw data storage and task state tracking for the extraction pipeline.
"""

import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

import duckdb
from filelock import FileLock, Timeout
from rich.console import Console

from .content_utils import analyze_content, is_empty_response

logger = logging.getLogger(__name__)
console = Console(force_terminal=True, legacy_windows=False, stderr=False)


# Data directory
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"


def connect_utf8(path: str) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB with UTF-8 error handling."""
    try:
        return duckdb.connect(path)
    except duckdb.Error as exc:
        # redecodifica string problema (CP‑1252 → UTF‑8)
        msg = (
            str(exc).encode("latin1", errors="ignore").decode("utf-8", errors="replace")
        )
        # Para DuckDB, usamos RuntimeError com a mensagem corrigida
        raise RuntimeError(msg) from exc


class PNCPWriter:
    """Handles writing PNCP data to DuckDB."""

    def __init__(self):
        self.conn: duckdb.DuckDBPyConnection | None = None
        self.db_lock: FileLock | None = None
        self.writer_running = False

    async def __aenter__(self):
        """Async context manager entry."""
        self._init_database()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        if self.conn:
            try:
                self.conn.commit()  # Commit any pending changes
                self.conn.close()
            except (duckdb.Error, AttributeError) as db_err:
                logger.warning(f"Error during database cleanup: {db_err}")

        if self.db_lock:
            try:
                self.db_lock.release()
            except (Timeout, RuntimeError) as lock_err:
                logger.warning(f"Error releasing database lock: {lock_err}")

    def _init_database(self):
        """Initialize DuckDB with PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        self.db_lock = FileLock(str(BALIZA_DB_PATH) + ".lock")
        try:
            self.db_lock.acquire(timeout=0.5)
        except Timeout:
            raise RuntimeError("Outra instância está usando pncp_new.db")

        self.conn = connect_utf8(str(BALIZA_DB_PATH))

        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")

        # Create the split table architecture (ADR-006)
        
        # Table 1: Content storage with deduplication
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.pncp_content (
                id UUID PRIMARY KEY, -- UUIDv5 based on content hash
                response_content TEXT NOT NULL,
                content_sha256 VARCHAR(64) NOT NULL UNIQUE, -- For integrity verification
                content_size_bytes INTEGER,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reference_count INTEGER DEFAULT 1 -- How many requests reference this content
            ) WITH (compression = "zstd")
        """
        )

        # Table 2: Request metadata with foreign key to content
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS psa.pncp_requests (
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
                content_id UUID REFERENCES psa.pncp_content(id) -- Foreign key to content
            ) WITH (compression = "zstd")
        """
        )
        
        # Legacy table for backwards compatibility during migration
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
        self.conn.execute("DROP TABLE IF EXISTS psa.pncp_extraction_tasks")
        self.conn.execute(
            """
            CREATE TABLE psa.pncp_extraction_tasks (
                task_id VARCHAR PRIMARY KEY,
                endpoint_name VARCHAR NOT NULL,
                data_date DATE NOT NULL,
                modalidade INTEGER,
                status VARCHAR DEFAULT 'PENDING' NOT NULL,
                total_pages INTEGER,
                total_records INTEGER,
                missing_pages JSON,
                last_error TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                CONSTRAINT unique_task UNIQUE (endpoint_name, data_date, modalidade)
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
            # Query information_schema to check if index exists
            result = self.conn.execute(
                "SELECT 1 FROM information_schema.indexes WHERE index_name = ?",
                [index_name],
            ).fetchone()
            return result is not None
        except duckdb.Error:
            # Fallback: try to create the index and catch the error
            try:
                self.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name}_test ON psa.pncp_raw_responses(endpoint_name)"
                )
                self.conn.execute(f"DROP INDEX IF EXISTS {index_name}_test")
                return False  # If we can create a test index, the target doesn't exist
            except duckdb.Error:
                return True  # If we can't create test index, assume target exists

    def _create_indexes_if_not_exist(self):
        """Create indexes for split table architecture."""
        indexes_to_create = {
            # Indexes for new split tables
            "idx_requests_endpoint_date_page": "CREATE INDEX IF NOT EXISTS idx_requests_endpoint_date_page ON psa.pncp_requests(endpoint_name, data_date, current_page)",
            "idx_requests_response_code": "CREATE INDEX IF NOT EXISTS idx_requests_response_code ON psa.pncp_requests(response_code)",
            "idx_requests_content_id": "CREATE INDEX IF NOT EXISTS idx_requests_content_id ON psa.pncp_requests(content_id)",
            "idx_content_hash": "CREATE INDEX IF NOT EXISTS idx_content_hash ON psa.pncp_content(content_sha256)",
            "idx_content_first_seen": "CREATE INDEX IF NOT EXISTS idx_content_first_seen ON psa.pncp_content(first_seen_at)",
            # Legacy table indexes for backwards compatibility
            "idx_legacy_endpoint_date_page": "CREATE INDEX IF NOT EXISTS idx_legacy_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)",
            "idx_legacy_response_code": "CREATE INDEX IF NOT EXISTS idx_legacy_response_code ON psa.pncp_raw_responses(response_code)",
        }

        for idx_name, create_sql in indexes_to_create.items():
            try:
                self.conn.execute(create_sql)
                logger.info(f"Index '{idx_name}' ensured.")
            except duckdb.Error as e:
                logger.exception(f"Failed to create index '{idx_name}': {e}")

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
                    console.print("Successfully migrated to ZSTD compression")
                else:
                    # No data to migrate, just replace table
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses")
                    self.conn.execute(
                        "ALTER TABLE psa.pncp_raw_responses_zstd RENAME TO pncp_raw_responses"
                    )
                    console.print("Empty table replaced with ZSTD compression")

            except duckdb.Error as create_error:
                # If table already exists with ZSTD, clean up
                with contextlib.suppress(duckdb.Error):
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses_zstd")

                # This likely means the table already has ZSTD or migration already happened
                if "already exists" in str(create_error):
                    pass  # Expected, migration already done
                else:
                    raise

        except duckdb.Error as e:
            console.print(f"⚠️ ZSTD migration error: {e}")
            # Rollback on error
            with contextlib.suppress(duckdb.Error):
                self.conn.rollback()

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
        except duckdb.Error as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Batch store failed: {e}")
            raise

    def _ensure_content_exists(self, content: str) -> str:
        """Ensure content exists in psa.pncp_content table and return content_id.
        
        Uses content deduplication logic from ADR-006.
        
        Args:
            content: Response content string
            
        Returns:
            UUID string of the content record
        """
        if is_empty_response(content):
            # For empty responses, create a special empty content record
            content = ""
        
        # Analyze content to get ID and hash
        content_id, content_hash, content_size = analyze_content(content)
        
        try:
            # Try to find existing content by hash
            existing = self.conn.execute(
                "SELECT id, reference_count FROM psa.pncp_content WHERE content_sha256 = ?",
                [content_hash]
            ).fetchone()
            
            if existing:
                # Content exists - increment reference count and update last_seen_at
                self.conn.execute(
                    """
                    UPDATE psa.pncp_content 
                    SET reference_count = reference_count + 1,
                        last_seen_at = CURRENT_TIMESTAMP
                    WHERE content_sha256 = ?
                    """,
                    [content_hash]
                )
                logger.debug(f"Content deduplicated: {content_id} (new ref count: {existing[1] + 1})")
                return existing[0]  # Return existing content_id
            else:
                # New content - insert new record
                self.conn.execute(
                    """
                    INSERT INTO psa.pncp_content 
                    (id, response_content, content_sha256, content_size_bytes, reference_count)
                    VALUES (?, ?, ?, ?, 1)
                    """,
                    [content_id, content, content_hash, content_size]
                )
                logger.debug(f"New content stored: {content_id} ({content_size} bytes)")
                return content_id
                
        except duckdb.Error as e:
            logger.error(f"Content storage failed for hash {content_hash}: {e}")
            raise

    def _store_request_with_content_id(self, page_data: dict, content_id: str) -> None:
        """Store request metadata with reference to deduplicated content."""
        try:
            self.conn.execute(
                """
                INSERT INTO psa.pncp_requests (
                    extracted_at, endpoint_url, endpoint_name, request_parameters,
                    response_code, response_headers, data_date, run_id,
                    total_records, total_pages, current_page, page_size, content_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    page_data.get("extracted_at"),
                    page_data.get("endpoint_url"),
                    page_data.get("endpoint_name"),
                    json.dumps(page_data.get("request_parameters", {})),
                    page_data.get("response_code"),
                    json.dumps(page_data.get("response_headers", {})),
                    page_data.get("data_date"),
                    page_data.get("run_id"),
                    page_data.get("total_records"),
                    page_data.get("total_pages"),
                    page_data.get("current_page"),
                    page_data.get("page_size"),
                    content_id
                ]
            )
        except duckdb.Error as e:
            logger.error(f"Request storage failed: {e}")
            raise

    def _batch_store_split_tables(self, pages: list[dict]) -> None:
        """Store pages using split table architecture with content deduplication."""
        for page_data in pages:
            content = page_data.get("response_content", "")
            
            # Step 1: Ensure content exists and get content_id
            content_id = self._ensure_content_exists(content)
            
            # Step 2: Store request metadata with content_id reference
            self._store_request_with_content_id(page_data, content_id)
            
            # Step 3: Also store in legacy table for backwards compatibility
            self._store_legacy_response(page_data)

    def _store_legacy_response(self, page_data: dict) -> None:
        """Store in legacy pncp_raw_responses table for backwards compatibility."""
        try:
            self.conn.execute(
                """
                INSERT INTO psa.pncp_raw_responses (
                    extracted_at, endpoint_url, endpoint_name, request_parameters,
                    response_code, response_content, response_headers, data_date, run_id,
                    total_records, total_pages, current_page, page_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    page_data.get("extracted_at"),
                    page_data.get("endpoint_url"),
                    page_data.get("endpoint_name"),
                    json.dumps(page_data.get("request_parameters", {})),
                    page_data.get("response_code"),
                    page_data.get("response_content", ""),
                    json.dumps(page_data.get("response_headers", {})),
                    page_data.get("data_date"),
                    page_data.get("run_id"),
                    page_data.get("total_records"),
                    page_data.get("total_pages"),
                    page_data.get("current_page"),
                    page_data.get("page_size")
                ]
            )
        except duckdb.Error as e:
            logger.error(f"Legacy response storage failed: {e}")
            raise

    async def writer_worker(
        self, page_queue: asyncio.Queue, commit_every: int = 75
    ) -> None:
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
                page = await page_queue.get()
                if page is None:
                    break

                # Add to local buffer
                batch_buffer.append(page)
                counter += 1

                # Flush buffer every commit_every pages
                if counter % commit_every == 0 and batch_buffer:
                    self._batch_store_split_tables(batch_buffer)  # Use new split table logic
                    self.conn.commit()
                    batch_buffer.clear()

                # Mark task as done
                page_queue.task_done()

            except duckdb.Error as e:
                console.print(f"❌ Writer error: {e}")
                page_queue.task_done()
                break

        # Flush any remaining items
        if batch_buffer:
            self._batch_store_split_tables(batch_buffer)  # Use new split table logic
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

    def __del__(self):
        """Cleanup."""
        if hasattr(self, "conn"):
            self.conn.close()
