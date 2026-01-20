"""Simple PNCP data extraction without dlt.

Replaces the dlt pipeline with straightforward httpx + DuckDB code.
"""

from __future__ import annotations

from datetime import datetime
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

    def extract(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
    ) -> dict[str, Any]:
        """Extract data from PNCP API for a date range.

        Args:
            start_date: Start of date range
            end_date: End of date range
            resource: Resource type (default: contratos)

        Returns:
            Dict with extraction results (rows_extracted, pages, etc.)
        """
        # Format dates for PNCP API (YYYYMMDD)
        data_inicial = start_date.strftime("%Y%m%d")
        data_final = end_date.strftime("%Y%m%d")

        all_rows = []
        page = 1
        total_pages = None

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
            task = progress.add_task("Fetching pages...", total=None)

            while True:
                # Call PNCP API
                url = f"{self.base_url}/{resource}"
                params = {
                    "dataInicial": data_inicial,
                    "dataFinal": data_final,
                    "pagina": page,
                    "tamanhoPagina": 500,
                }

                response = self.client.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                rows = data.get("data", [])

                if not rows:
                    break

                all_rows.extend(rows)

                # Update progress
                if total_pages is None:
                    total_pages = data.get("totalPaginas", 1)
                    progress.update(task, total=total_pages)

                progress.update(
                    task, completed=page, description=f"Fetching page {page}/{total_pages}"
                )

                if page >= total_pages:
                    break

                page += 1

        console.print(f"[green]✓ Fetched {len(all_rows)} rows across {page} pages")

        # Insert into DuckDB
        with duckdb.connect(str(self.db_path)) as con:
            self._ensure_schema(con)

            if all_rows:
                # Prepare data for insertion
                values = []
                for row in all_rows:
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

                # Insert or ignore (append-only, deduplication by primary key)
                con.executemany(
                    f"""
                    INSERT OR IGNORE INTO {self.dataset}.contratos
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    values,
                )

                console.print(
                    f"[green]✓ Inserted {len(all_rows)} rows into {self.dataset}.contratos (duplicates ignored)"
                )

            # Record coverage
            con.execute(
                """
                INSERT OR REPLACE INTO baliza_state.coverage
                VALUES (?, ?, ?, 'complete', ?, ?, NOW())
            """,
                [resource, start_date, end_date, page, len(all_rows)],
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
