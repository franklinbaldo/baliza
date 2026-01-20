"""Simple PNCP data extraction without dlt.

Replaces the dlt pipeline with straightforward httpx + DuckDB code.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import httpx
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

console = Console()


class PNCPExtractor:
    """Simple extractor for PNCP API data."""

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

    def run(
        self,
        resource: str = "contratos",
        lookback_days: int = 3,
        project_start_date: datetime = datetime(2023, 1, 1),
    ) -> dict[str, Any]:
        """Run the state-aware, resumable extraction pipeline."""
        console.print("[cyan]Starting automatic extraction...")

        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)
            last_date = self._get_last_extracted_date(con, resource)

        if last_date:
            start_date = last_date - timedelta(days=lookback_days - 1)
            console.print(
                f"Found last run on {last_date.date()}. Resuming from {start_date.date()} "
                f"(with {lookback_days}-day lookback)."
            )
        else:
            start_date = project_start_date
            console.print(f"No previous run found. Starting from project start: {start_date.date()}")

        end_date = datetime.now()
        total_rows = 0
        windows_processed = 0

        current_date = start_date
        while current_date < end_date:
            window_start = current_date
            window_end = current_date.replace(hour=23, minute=59, second=59)

            console.print(f"\n[bold]Processing {window_start.date()}...[/bold]")
            try:
                result = self.extract(window_start, window_end, resource)
                total_rows += result["rows_extracted"]
                windows_processed += 1
            except Exception:
                console.print(f"[red]✗ Failed to process window {window_start.date()}. Continuing.")

            current_date += timedelta(days=1)

        return {
            "total_rows": total_rows,
            "windows_processed": windows_processed,
            "start_date": start_date,
            "end_date": end_date,
        }

    def _get_last_extracted_date(
        self, con: duckdb.DuckDBPyConnection, resource: str
    ) -> datetime | None:
        """Get the latest successfully extracted date from the coverage table."""
        result = con.execute(
            """
            SELECT MAX(window_end)
            FROM baliza_state.coverage
            WHERE resource = ? AND status IN ('complete', 'failed')
        """,
            [resource],
        ).fetchone()
        return result[0] if result and result[0] else None

    def extract(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
    ) -> dict[str, Any]:
        """Extract data from PNCP API for a single, specific date range (window)."""
        all_rows = []
        page = 1
        status = "complete"

        try:
            data_inicial = start_date.strftime("%Y%m%d")
            data_final = end_date.strftime("%Y%m%d")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console,
                transient=True,
            ) as progress:
                task = progress.add_task("Fetching pages...", total=None)
                total_pages = None

                while True:
                    url = f"{self.base_url}/{resource}"
                    params = {
                        "dataInicial": data_inicial, "dataFinal": data_final,
                        "pagina": page, "tamanhoPagina": 500,
                    }
                    response = self.client.get(url, params=params)
                    response.raise_for_status()

                    if response.status_code == 204:
                        console.print(f"  [yellow]No data found for {start_date.date()}.")
                        break

                    data = response.json()
                    rows = data.get("data", [])
                    if not rows:
                        break

                    all_rows.extend(rows)
                    if total_pages is None:
                        total_pages = data.get("totalPaginas", 1)
                        progress.update(task, total=total_pages)

                    progress.update(task, completed=page, description=f"Page {page}/{total_pages}")
                    if page >= total_pages:
                        break
                    page += 1

            console.print(f"  [green]✓ Fetched {len(all_rows)} rows across {page} pages.")

            if all_rows:
                with duckdb.connect(str(self.db_path)) as con:
                    self._ensure_schema(con)
                    con.executemany(
                        f"INSERT OR IGNORE INTO {self.dataset}.contratos VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            (
                                r.get("numeroControlePNCP"), r.get("anoCompra"),
                                r.get("sequencialCompra"),
                                r.get("orgaoEntidade", {}).get("cnpj"),
                                r.get("orgaoEntidade", {}).get("razaoSocial"),
                                r.get("orgaoEntidade", {}).get("poderId"),
                                r.get("unidadeOrgao", {}).get("codigoUnidade"),
                                r.get("unidadeOrgao", {}).get("nomeUnidade"),
                                r.get("modalidadeId"), r.get("modalidadeNome"),
                                r.get("valorInicial"), r.get("dataPublicacao"),
                                r.get("dataVigenciaInicio"), r.get("dataVigenciaFim"),
                                r.get("objetoContrato"), r.get("informacaoComplementar"),
                                r.get("numeroProcesso"), r.get("linkSistemaOrigem"),
                                r.get("dataInclusao"), r.get("dataAtualizacao"),
                                r.get("usuarioNome"),
                            )
                            for r in all_rows
                        ],
                    )
                    console.print(f"  [green]✓ Inserted data into {self.dataset}.contratos.")

        except Exception as e:
            status = "failed"
            console.print(f"\n[red]✗ An error occurred: {e}")
            raise
        finally:
            with duckdb.connect(str(self.db_path)) as con:
                self._ensure_schema(con)
                con.execute(
                    """
                    INSERT OR REPLACE INTO baliza_state.coverage
                    VALUES (?, ?, ?, ?, ?, ?, NOW())
                    """,
                    [resource, start_date, end_date, status, page, len(all_rows)],
                )

        return {
            "rows_extracted": len(all_rows),
            "pages": page,
            "start_date": start_date,
            "end_date": end_date,
        }

    def close(self) -> None:
        """Close HTTP client."""
        self.client.close()

    def __enter__(self) -> PNCPExtractor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
