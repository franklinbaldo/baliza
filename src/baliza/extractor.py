"""Simple PNCP data extraction without dlt.

Replaces the dlt pipeline with straightforward httpx + DuckDB code.
Supports per-page checkpointing for resume on timeout.
"""

from __future__ import annotations

import concurrent.futures
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict

import duckdb
import httpx
import pyarrow as pa
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .utils import validate_identifier, validate_resource_path, validate_url

console = Console()

class CheckpointData(TypedDict):
    """Data structure for checkpoint state."""

    current_page: int
    total_pages: int
    rows_extracted: int


# Arrow schema for PNCP API response to handle nested structures
PNCP_ARROW_SCHEMA = pa.schema([
    ("numeroControlePNCP", pa.string()),
    ("anoCompra", pa.int64()),
    ("sequencialCompra", pa.int64()),
    ("orgaoEntidade", pa.struct([
        ("cnpj", pa.string()),
        ("razaoSocial", pa.string()),
        ("poderId", pa.string())
    ])),
    ("unidadeOrgao", pa.struct([
        ("codigoUnidade", pa.string()),
        ("nomeUnidade", pa.string())
    ])),
    ("modalidadeId", pa.int64()),
    ("modalidadeNome", pa.string()),
    ("valorInicial", pa.float64()),
    ("dataPublicacao", pa.string()),
    ("dataVigenciaInicio", pa.string()),
    ("dataVigenciaFim", pa.string()),
    ("objetoContrato", pa.string()),
    ("informacaoComplementar", pa.string()),
    ("numeroProcesso", pa.string()),
    ("linkSistemaOrigem", pa.string()),
    ("dataInclusao", pa.string()),
    ("dataAtualizacao", pa.string()),
    ("usuarioNome", pa.string())
])


DEFAULT_MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10 MB


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=16),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException)),
    reraise=True,
)
def _fetch_page(client: httpx.Client, url: str, params: dict, max_size: int = DEFAULT_MAX_RESPONSE_SIZE) -> dict:
    """Fetch a single page from PNCP API with retry logic and size limit."""
    with client.stream("GET", url, params=params) as response:
        response.raise_for_status()

        chunks = []
        total_size = 0
        for chunk in response.iter_bytes():
            total_size += len(chunk)
            if total_size > max_size:
                raise ValueError(f"Response too large: >{max_size} bytes")
            chunks.append(chunk)

    return json.loads(b"".join(chunks))


class PNCPExtractor:
    """Simple extractor for PNCP API data with checkpoint support."""

    def __init__(
        self,
        db_path: Path,
        dataset: str = "baliza_raw",
        base_url: str = "https://pncp.gov.br/api/consulta/v1",
    ):
        self.db_path = db_path
        # Validate dataset name to prevent SQL injection
        self.dataset = validate_identifier(dataset)
        # Validate base_url to prevent SSRF
        self.base_url = validate_url(base_url)
        self.client = httpx.Client(timeout=30.0)

    def _ensure_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create schema and tables if they don't exist."""
        # Data schema
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dataset}")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.dataset}.contratos (
                numeroControlePNCP VARCHAR PRIMARY KEY,
                anoCompra INTEGER,
                sequencialCompra INTEGER,
                orgaoEntidade_cnpj VARCHAR,
                orgaoEntidade_razaoSocial VARCHAR,
                orgaoEntidade_poderId VARCHAR,
                unidadeOrgao_codigoUnidade VARCHAR,
                unidadeOrgao_nomeUnidade VARCHAR,
                modalidadeId INTEGER,
                modalidadeNome VARCHAR,
                valorInicial DECIMAL(18,2),
                dataPublicacao TIMESTAMP,
                dataVigenciaInicio TIMESTAMP,
                dataVigenciaFim TIMESTAMP,
                objetoContrato VARCHAR,
                informacaoComplementar VARCHAR,
                numeroProcesso VARCHAR,
                linkSistemaOrigem VARCHAR,
                dataInclusao TIMESTAMP,
                dataAtualizacao TIMESTAMP,
                usuarioNome VARCHAR
            )
        """)

        # State schema
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.coverage (
                resource VARCHAR,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                status VARCHAR,
                total_paginas INTEGER,
                rows_extracted INTEGER,
                extracted_at TIMESTAMP,
                PRIMARY KEY (resource, window_start, window_end)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.runs (
                run_id VARCHAR PRIMARY KEY,
                resource VARCHAR,
                pipeline_name VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR,
                windows_completed INTEGER,
                windows_failed INTEGER,
                rows_extracted INTEGER,
                error_message VARCHAR
            )
        """)
        # Checkpoint table for resumable extraction
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.extraction_checkpoint (
                resource VARCHAR,
                extraction_date DATE,
                current_page INTEGER,
                total_pages INTEGER,
                rows_extracted INTEGER,
                started_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (resource, extraction_date)
            )
        """)
        # Track what's been uploaded to Internet Archive
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.uploaded_to_ia (
                item_id VARCHAR PRIMARY KEY,
                extraction_date DATE,
                uploaded_at TIMESTAMP,
                file_count INTEGER,
                total_rows INTEGER
            )
        """)

    def _get_checkpoint(
        self, con: duckdb.DuckDBPyConnection, resource: str, extraction_date: datetime
    ) -> dict[str, Any] | None:
        """Get existing checkpoint for a date."""
        result = con.execute(
            """
            SELECT current_page, total_pages, rows_extracted, started_at
            FROM baliza_state.extraction_checkpoint
            WHERE resource = ? AND extraction_date = ?
        """,
            [resource, extraction_date.date()],
        ).fetchone()

        if result:
            return {
                "current_page": result[0],
                "total_pages": result[1],
                "rows_extracted": result[2],
                "started_at": result[3],
            }
        return None

    def _save_checkpoint(
        self,
        con: duckdb.DuckDBPyConnection,
        resource: str,
        extraction_date: datetime,
        stats: CheckpointData,
        started_at: datetime,
    ) -> None:
        """Save extraction checkpoint."""
        con.execute(
            """
            INSERT OR REPLACE INTO baliza_state.extraction_checkpoint
            VALUES (?, ?, ?, ?, ?, ?, NOW())
        """,
            [
                resource,
                extraction_date.date(),
                stats["current_page"],
                stats["total_pages"],
                stats["rows_extracted"],
                started_at,
            ],
        )

    def _clear_checkpoint(
        self, con: duckdb.DuckDBPyConnection, resource: str, extraction_date: datetime
    ) -> None:
        """Clear checkpoint after successful completion."""
        con.execute(
            """
            DELETE FROM baliza_state.extraction_checkpoint
            WHERE resource = ? AND extraction_date = ?
        """,
            [resource, extraction_date.date()],
        )

    def _insert_page(
        self, con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]
    ) -> int:
        """Insert a single page of results immediately."""
        if not rows:
            return 0

        # Create Arrow table from rows with explicit schema to handle nested fields
        # This is significantly faster than iterating in Python (~250x speedup)
        try:
            table = pa.Table.from_pylist(rows, schema=PNCP_ARROW_SCHEMA)
        except Exception as e:
            # Fallback for unexpected schema mismatches, though unlikely with explicit schema
            console.print(f"[yellow]Warning: Arrow conversion failed ({e}), falling back to slow path")
            return self._insert_page_slow(con, rows)

        # Register arrow table as a view
        con.register("page_view", table)

        try:
            # Insert using SQL with struct accessors
            # Note: Flattening happens in the SELECT statement
            con.execute(f"""
                INSERT OR IGNORE INTO {self.dataset}.contratos (
                    numeroControlePNCP,
                    anoCompra,
                    sequencialCompra,
                    orgaoEntidade_cnpj,
                    orgaoEntidade_razaoSocial,
                    orgaoEntidade_poderId,
                    unidadeOrgao_codigoUnidade,
                    unidadeOrgao_nomeUnidade,
                    modalidadeId,
                    modalidadeNome,
                    valorInicial,
                    dataPublicacao,
                    dataVigenciaInicio,
                    dataVigenciaFim,
                    objetoContrato,
                    informacaoComplementar,
                    numeroProcesso,
                    linkSistemaOrigem,
                    dataInclusao,
                    dataAtualizacao,
                    usuarioNome
                )
                SELECT
                    numeroControlePNCP,
                    anoCompra,
                    sequencialCompra,
                    orgaoEntidade.cnpj,
                    orgaoEntidade.razaoSocial,
                    orgaoEntidade.poderId,
                    unidadeOrgao.codigoUnidade,
                    unidadeOrgao.nomeUnidade,
                    modalidadeId,
                    modalidadeNome,
                    valorInicial,
                    dataPublicacao,
                    dataVigenciaInicio,
                    dataVigenciaFim,
                    objetoContrato,
                    informacaoComplementar,
                    numeroProcesso,
                    linkSistemaOrigem,
                    dataInclusao,
                    dataAtualizacao,
                    usuarioNome
                FROM page_view
            """)
        finally:
            con.unregister("page_view")

        return len(rows)

    def _insert_page_slow(
        self, con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]
    ) -> int:
        """Fallback insertion method (legacy slow path)."""
        values = []
        for row in rows:
            values.append(
                (
                    row.get("numeroControlePNCP"),
                    row.get("anoCompra"),
                    row.get("sequencialCompra"),
                    row.get("orgaoEntidade", {}).get("cnpj"),
                    row.get("orgaoEntidade", {}).get("razaoSocial"),
                    row.get("orgaoEntidade", {}).get("poderId"),
                    row.get("unidadeOrgao", {}).get("codigoUnidade"),
                    row.get("unidadeOrgao", {}).get("nomeUnidade"),
                    row.get("modalidadeId"),
                    row.get("modalidadeNome"),
                    row.get("valorInicial"),
                    row.get("dataPublicacao"),
                    row.get("dataVigenciaInicio"),
                    row.get("dataVigenciaFim"),
                    row.get("objetoContrato"),
                    row.get("informacaoComplementar"),
                    row.get("numeroProcesso"),
                    row.get("linkSistemaOrigem"),
                    row.get("dataInclusao"),
                    row.get("dataAtualizacao"),
                    row.get("usuarioNome"),
                )
            )

        # Insert or ignore (deduplication by primary key)
        con.executemany(
            f"""
            INSERT OR IGNORE INTO {self.dataset}.contratos
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            values,
        )

        return len(values)

    def _extract_range_task(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str,
        progress: Progress | None = None,
    ) -> dict[str, Any]:
        """Extract data for a date range (worker function)."""
        data_inicial = start_date.strftime("%Y%m%d")
        data_final = end_date.strftime("%Y%m%d")
        total_rows = 0
        page = 1
        total_pages = None

        # Add transient task for this range if progress bar provided
        task_id = None
        if progress:
            desc = f"Fetching {start_date.date()}"
            if start_date != end_date:
                desc += f" to {end_date.date()}"
            task_id = progress.add_task(f"{desc}...", total=None)

        try:
            # Open dedicated connection for this thread
            with duckdb.connect(str(self.db_path)) as con:
                # No need to ensure schema here, done in coordinator

                # Check for existing checkpoint
                checkpoint = self._get_checkpoint(con, resource, start_date)
                started_at = datetime.now()
                if checkpoint:
                    page = checkpoint["current_page"] + 1
                    total_rows = checkpoint["rows_extracted"]
                    total_pages = checkpoint["total_pages"]
                    started_at = checkpoint["started_at"]
                    if progress and task_id:
                        progress.update(task_id, description=f"Resuming {start_date.date()} p{page}/{total_pages}")

                # Prepare invariant params
                url = f"{self.base_url}/{resource}"
                params = {
                    "dataInicial": data_inicial,
                    "dataFinal": data_final,
                    "tamanhoPagina": 500,
                }

                while True:
                    # Call PNCP API with retry
                    params["pagina"] = page

                    data = _fetch_page(self.client, url, params)
                    rows = data.get("data", [])

                    if not rows:
                        break

                    # Insert THIS page immediately
                    inserted = self._insert_page(con, rows)
                    total_rows += inserted

                    # Update total_pages on first response
                    if total_pages is None:
                        total_pages = data.get("totalPaginas", 1)
                        if progress and task_id:
                            progress.update(task_id, total=total_pages)

                    # Checkpoint after each page
                    stats: CheckpointData = {
                        "current_page": page,
                        "total_pages": total_pages,  # type: ignore[required]
                        "rows_extracted": total_rows,
                    }
                    self._save_checkpoint(
                        con, resource, start_date, stats, started_at
                    )

                    if progress and task_id:
                        progress.update(
                            task_id,
                            completed=page,
                            description=f"Fetching {start_date.date()} {page}/{total_pages}",
                        )

                    if page >= total_pages:
                        break

                    page += 1

                # Clear checkpoint on successful completion
                self._clear_checkpoint(con, resource, start_date)

                # Record coverage for this range
                con.execute(
                    """
                    INSERT OR REPLACE INTO baliza_state.coverage
                    VALUES (?, ?, ?, 'complete', ?, ?, NOW())
                """,
                    [resource, start_date, end_date, page, total_rows],
                )
        finally:
            if progress and task_id:
                progress.remove_task(task_id)

        return {
            "rows_extracted": total_rows,
            "pages": page,
            "start_date": start_date,
            "end_date": end_date,
        }

    def extract(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
        workers: int = 4,
    ) -> dict[str, Any]:
        """Extract data from PNCP API for a date range.

        Supports parallel extraction with multiple workers.

        Args:
            start_date: Start of date range
            end_date: End of date range
            resource: Resource type (default: contratos)
            workers: Number of concurrent workers (default: 4)

        Returns:
            Dict with extraction results (rows_extracted, pages, etc.)
        """
        # Validate resource path to prevent traversal/injection
        validate_resource_path(resource)

        console.print(
            f"[cyan]Extracting {resource} from {start_date.date()} to {end_date.date()} "
            f"using {workers} workers...[/cyan]"
        )

        # Ensure schema exists before any worker starts
        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)

        total_rows = 0
        total_pages = 0
        failed_dates = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            if workers == 1:
                # Sequential mode (matches original behavior)
                try:
                    result = self._extract_range_task(start_date, end_date, resource, progress)
                    total_rows = result["rows_extracted"]
                    total_pages = result["pages"]
                except Exception as e:
                    console.print(f"[red]✗ Failed to extract: {e}")
                    raise
            else:
                # Parallel mode (split by day)
                dates = []
                curr = start_date
                while curr <= end_date:
                    dates.append(curr)
                    curr += timedelta(days=1)

                main_task = progress.add_task(f"Overall Progress ({len(dates)} days)", total=len(dates))

                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(self._extract_range_task, date, date, resource, progress): date
                        for date in dates
                    }

                    for future in concurrent.futures.as_completed(futures):
                        date = futures[future]
                        try:
                            result = future.result()
                            total_rows += result["rows_extracted"]
                            total_pages += result["pages"]
                            progress.update(main_task, advance=1)
                        except Exception as e:
                            console.print(f"[red]✗ Failed to extract {date.date()}: {e}")
                            failed_dates.append(date)
                            # We don't stop everything, but we should probably record failure
                            # Checkpoint remains so it can be retried later

        console.print(
            f"[green]✓ Extracted {total_rows} rows across {total_pages} pages "
            f"({(1 if workers == 1 else len(dates)) - len(failed_dates)} tasks successful)"
        )

        if failed_dates:
            console.print(f"[red]⚠ {len(failed_dates)} days failed[/red]")

        return {
            "rows_extracted": total_rows,
            "pages": total_pages,
            "start_date": start_date,
            "end_date": end_date,
            "failed_dates": [d.date() for d in failed_dates]
        }

    def cleanup_uploaded(self, extraction_date: datetime) -> int:
        """Remove data from buffer after successful IA upload.

        Keeps state tables intact, only removes raw contract data.

        Args:
            extraction_date: Date to clean up

        Returns:
            Number of rows deleted
        """
        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)

            # Calculate next day for range query to optimize index usage
            next_day = extraction_date + timedelta(days=1)

            # Count rows before deletion
            result = con.execute(
                f"""
                SELECT COUNT(*) FROM {self.dataset}.contratos
                WHERE dataPublicacao >= ? AND dataPublicacao < ?
            """,
                [extraction_date.date(), next_day.date()],
            ).fetchone()
            row_count = result[0] if result else 0

            if row_count > 0:
                # Delete only the data for this date
                con.execute(
                    f"""
                    DELETE FROM {self.dataset}.contratos
                    WHERE dataPublicacao >= ? AND dataPublicacao < ?
                """,
                    [extraction_date.date(), next_day.date()],
                )

                console.print(
                    f"[green]✓ Cleaned up {row_count} rows for {extraction_date.date()}"
                )

            return row_count

    def record_ia_upload(
        self,
        item_id: str,
        extraction_date: datetime,
        file_count: int,
        total_rows: int,
    ) -> None:
        """Record successful Internet Archive upload."""
        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)
            con.execute(
                """
                INSERT OR REPLACE INTO baliza_state.uploaded_to_ia
                VALUES (?, ?, NOW(), ?, ?)
            """,
                [item_id, extraction_date.date(), file_count, total_rows],
            )

    def get_dates_ready_for_export(self, stability_days: int = 7) -> list[datetime]:
        """Get dates that are complete and old enough for export.

        Args:
            stability_days: Days to wait before considering data stable

        Returns:
            List of dates ready for export
        """
        cutoff = datetime.now() - timedelta(days=stability_days)

        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)

            # Find dates that:
            # 1. Have data in contratos
            # 2. Are older than stability window
            # 3. Haven't been uploaded to IA yet
            result = con.execute(
                f"""
                SELECT DISTINCT CAST(dataPublicacao AS DATE) as dt
                FROM {self.dataset}.contratos
                WHERE dataPublicacao < ?
                  AND CAST(dataPublicacao AS DATE) NOT IN (
                      SELECT extraction_date FROM baliza_state.uploaded_to_ia
                  )
                ORDER BY dt
            """,
                [cutoff.date()],
            ).fetchall()

            return [datetime.combine(row[0], datetime.min.time()) for row in result]

    def get_buffer_stats(self) -> dict[str, Any]:
        """Get statistics about the current buffer."""
        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)

            # Total rows in buffer
            total_rows = con.execute(
                f"SELECT COUNT(*) FROM {self.dataset}.contratos"
            ).fetchone()[0]

            # Rows by date
            by_date = con.execute(
                f"""
                SELECT CAST(dataPublicacao AS DATE) as dt, COUNT(*) as cnt
                FROM {self.dataset}.contratos
                GROUP BY dt
                ORDER BY dt
            """
            ).fetchall()

            # Uploaded dates
            uploaded = con.execute(
                "SELECT COUNT(*) FROM baliza_state.uploaded_to_ia"
            ).fetchone()[0]

            # Pending checkpoints
            checkpoints = con.execute(
                "SELECT COUNT(*) FROM baliza_state.extraction_checkpoint"
            ).fetchone()[0]

            return {
                "total_rows": total_rows,
                "dates_in_buffer": len(by_date),
                "rows_by_date": {str(row[0]): row[1] for row in by_date},
                "dates_uploaded_to_ia": uploaded,
                "pending_checkpoints": checkpoints,
            }

    def close(self) -> None:
        """Close HTTP client."""
        self.client.close()

    def __enter__(self) -> PNCPExtractor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
