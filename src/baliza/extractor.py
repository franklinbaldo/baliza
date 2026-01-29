"""Simple PNCP data extraction without dlt.

Replaces the dlt pipeline with straightforward httpx + DuckDB code.
Supports per-page checkpointing for resume on timeout.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pyarrow as pa
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .utils import validate_identifier, validate_resource_path, validate_url

console = Console()

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


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=16),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException)),
    reraise=True,
)
def _fetch_page(client: httpx.Client, url: str, params: dict) -> dict:
    """Fetch a single page from PNCP API with retry logic."""
    response = client.get(url, params=params)
    response.raise_for_status()
    return response.json()


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
            SELECT current_page, total_pages, rows_extracted
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
            }
        return None

    def _save_checkpoint(  # noqa: PLR0913
        self,
        con: duckdb.DuckDBPyConnection,
        resource: str,
        extraction_date: datetime,
        current_page: int,
        total_pages: int,
        rows_extracted: int,
    ) -> None:
        """Save extraction checkpoint."""
        con.execute(
            """
            INSERT OR REPLACE INTO baliza_state.extraction_checkpoint
            VALUES (?, ?, ?, ?, ?, COALESCE(
                (SELECT started_at FROM baliza_state.extraction_checkpoint
                 WHERE resource = ? AND extraction_date = ?),
                NOW()
            ), NOW())
        """,
            [
                resource,
                extraction_date.date(),
                current_page,
                total_pages,
                rows_extracted,
                resource,
                extraction_date.date(),
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

    def extract(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
    ) -> dict[str, Any]:
        """Extract data from PNCP API for a date range.

        Supports resuming from checkpoint if interrupted.

        Args:
            start_date: Start of date range
            end_date: End of date range
            resource: Resource type (default: contratos)

        Returns:
            Dict with extraction results (rows_extracted, pages, etc.)
        """
        # Validate resource path to prevent traversal/injection
        validate_resource_path(resource)

        # Format dates for PNCP API (YYYYMMDD)
        data_inicial = start_date.strftime("%Y%m%d")
        data_final = end_date.strftime("%Y%m%d")

        total_rows = 0
        page = 1
        total_pages = None

        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)

            # Check for existing checkpoint
            checkpoint = self._get_checkpoint(con, resource, start_date)
            if checkpoint:
                page = checkpoint["current_page"] + 1
                total_rows = checkpoint["rows_extracted"]
                total_pages = checkpoint["total_pages"]
                console.print(
                    f"[yellow]Resuming from page {page}/{total_pages} "
                    f"({total_rows} rows already extracted)"
                )

            console.print(
                f"[cyan]Extracting {resource} from {start_date.date()} to {end_date.date()}..."
            )

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    "Fetching pages...",
                    total=total_pages,
                    completed=page - 1 if checkpoint else 0,
                )

                while True:
                    # Call PNCP API with retry
                    url = f"{self.base_url}/{resource}"
                    params = {
                        "dataInicial": data_inicial,
                        "dataFinal": data_final,
                        "pagina": page,
                        "tamanhoPagina": 500,
                    }

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
                        progress.update(task, total=total_pages)

                    # Checkpoint after each page
                    self._save_checkpoint(
                        con, resource, start_date, page, total_pages, total_rows
                    )

                    progress.update(
                        task,
                        completed=page,
                        description=f"Fetching page {page}/{total_pages}",
                    )

                    if page >= total_pages:
                        break

                    page += 1

            console.print(
                f"[green]✓ Extracted {total_rows} rows across {page} pages"
            )

            # Clear checkpoint on successful completion
            self._clear_checkpoint(con, resource, start_date)

            # Record coverage
            con.execute(
                """
                INSERT OR REPLACE INTO baliza_state.coverage
                VALUES (?, ?, ?, 'complete', ?, ?, NOW())
            """,
                [resource, start_date, end_date, page, total_rows],
            )

        return {
            "rows_extracted": total_rows,
            "pages": page,
            "start_date": start_date,
            "end_date": end_date,
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

            # Count rows before deletion
            result = con.execute(
                f"""
                SELECT COUNT(*) FROM {self.dataset}.contratos
                WHERE CAST(dataPublicacao AS DATE) = ?
            """,
                [extraction_date.date()],
            ).fetchone()
            row_count = result[0] if result else 0

            if row_count > 0:
                # Delete only the data for this date
                con.execute(
                    f"""
                    DELETE FROM {self.dataset}.contratos
                    WHERE CAST(dataPublicacao AS DATE) = ?
                """,
                    [extraction_date.date()],
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
                WHERE CAST(dataPublicacao AS DATE) < ?
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
