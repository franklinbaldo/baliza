"""Simple PNCP data extraction without dlt.

Replaces the dlt pipeline with straightforward httpx + DuckDB code.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import duckdb
import httpx
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

console = Console()


@dataclass
class Window:
    """Represents a time window for extraction."""

    start: datetime
    end: datetime
    status: str


class PNCPExtractor:
    """Handles stateful, resumable extraction from the PNCP API."""

    def __init__(
        self,
        db_path: Path,
        dataset: str = "baliza_raw",
        base_url: str = "https://pncp.gov.br/api/consulta/v1",
    ):
        self.db_path = db_path
        self.dataset = dataset
        self.base_url = base_url
        self.client = httpx.Client(timeout=30.0)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create schema and tables if they don't exist."""
        with duckdb.connect(str(self.db_path)) as con:
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
                    PRIMARY KEY (resource, window_start)
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS baliza_state.runs (
                    run_id VARCHAR PRIMARY KEY,
                    resource VARCHAR,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    status VARCHAR,
                    windows_completed INTEGER,
                    windows_failed INTEGER,
                    rows_extracted INTEGER,
                    error_message VARCHAR
                )
            """)

    def get_unprocessed_windows(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
    ) -> Iterator[Window]:
        """Identify and yield unprocessed or failed daily windows."""
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            completed_windows = {
                row[0].date()
                for row in con.execute(
                    """
                    SELECT window_start FROM baliza_state.coverage
                    WHERE resource = ? AND status = 'completed' AND window_start >= ? AND window_start <= ?
                    """,
                    [resource, start_date, end_date],
                ).fetchall()
            }

        current_date = start_date
        while current_date <= end_date:
            if current_date.date() not in completed_windows:
                yield Window(
                    start=current_date,
                    end=current_date + timedelta(days=1) - timedelta(seconds=1),
                    status="pending",
                )
            current_date += timedelta(days=1)

    def run(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
    ) -> dict[str, Any]:
        """Run the stateful extraction process."""
        run_id = str(uuid.uuid4())
        windows_to_process = list(self.get_unprocessed_windows(start_date, end_date, resource))

        if not windows_to_process:
            console.print("[green]✓ No new data to process. Coverage is up to date.")
            return {"windows_completed": 0, "windows_failed": 0, "rows_extracted": 0}

        console.print(
            f"Found {len(windows_to_process)} unprocessed day(s) from {start_date.date()} to {end_date.date()}."
        )

        with duckdb.connect(str(self.db_path)) as con:
            con.execute(
                "INSERT INTO baliza_state.runs (run_id, resource, started_at, status) VALUES (?, ?, NOW(), 'running')",
                [run_id, resource],
            )

        total_rows = 0
        completed_count = 0
        failed_count = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing windows...", total=len(windows_to_process))

            for i, window in enumerate(windows_to_process):
                progress.update(
                    task,
                    description=f"[{i+1}/{len(windows_to_process)}] Window {window.start.date()}",
                )
                try:
                    result = self._extract_window(window, resource)
                    total_rows += result["rows_extracted"]
                    completed_count += 1
                except Exception as e:
                    failed_count += 1
                    console.print(f"[red]✗ Failed to process window {window.start.date()}: {e}")
                progress.update(task, advance=1)

        # Finalize run state
        status = "completed" if failed_count == 0 else "completed_with_errors"
        with duckdb.connect(str(self.db_path)) as con:
            con.execute(
                """
                UPDATE baliza_state.runs
                SET finished_at = NOW(), status = ?, windows_completed = ?, windows_failed = ?, rows_extracted = ?
                WHERE run_id = ?
                """,
                [status, completed_count, failed_count, total_rows, run_id],
            )

        console.print(
            f"\n[green]✓ Run finished. Completed: {completed_count}, Failed: {failed_count}, Rows: {total_rows}"
        )
        return {
            "windows_completed": completed_count,
            "windows_failed": failed_count,
            "rows_extracted": total_rows,
        }

    def _extract_window(self, window: Window, resource: str) -> dict[str, Any]:
        """Extract data for a single window and update state."""
        with duckdb.connect(str(self.db_path)) as con:
            con.execute(
                """
                INSERT OR REPLACE INTO baliza_state.coverage
                (resource, window_start, window_end, status, extracted_at)
                VALUES (?, ?, ?, 'running', NOW())
                """,
                [resource, window.start, window.end],
            )

        try:
            data_inicial = window.start.strftime("%Y%m%d")
            data_final = window.end.strftime("%Y%m%d")
            all_rows = []
            page = 1
            total_pages = 1

            while page <= total_pages:
                params = {
                    "dataInicial": data_inicial,
                    "dataFinal": data_final,
                    "pagina": page,
                    "tamanhoPagina": 500,
                }
                response = self.client.get(f"{self.base_url}/{resource}", params=params)
                response.raise_for_status()
                data = response.json()
                rows = data.get("data", [])
                if not rows:
                    break
                all_rows.extend(rows)
                total_pages = data.get("totalPaginas", 1)
                page += 1

            if all_rows:
                self._insert_data(all_rows)

            with duckdb.connect(str(self.db_path)) as con:
                con.execute(
                    """
                    INSERT OR REPLACE INTO baliza_state.coverage
                    VALUES (?, ?, ?, 'completed', ?, ?, NOW())
                    """,
                    [resource, window.start, window.end, page - 1, len(all_rows)],
                )
            return {"rows_extracted": len(all_rows), "pages": page - 1}

        except Exception as e:
            with duckdb.connect(str(self.db_path)) as con:
                con.execute(
                    """
                    UPDATE baliza_state.coverage SET status = 'failed'
                    WHERE resource = ? AND window_start = ?
                    """,
                    [resource, window.start],
                )
            raise e

    def _insert_data(self, rows: list[dict[str, Any]]) -> None:
        """Insert rows into the appropriate table."""
        values = [
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
            for row in rows
        ]
        with duckdb.connect(str(self.db_path)) as con:
            con.executemany(
                f"""
                INSERT OR IGNORE INTO {self.dataset}.contratos
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def close(self) -> None:
        """Close HTTP client."""
        self.client.close()

    def __enter__(self) -> PNCPExtractor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
