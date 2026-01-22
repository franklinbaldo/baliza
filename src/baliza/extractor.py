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

from .utils import validate_identifier

console = Console()


class PNCPExtractor:
    """Simple extractor for PNCP API data."""

    RESOURCE_CONFIG = {
        "contratos": {
            "primary_key": "numeroControlePNCP",
            "columns": {
                "numeroControlePNCP": "VARCHAR",
                "anoCompra": "INTEGER",
                "sequencialCompra": "INTEGER",
                "orgaoEntidade_cnpj": "VARCHAR",
                "orgaoEntidade_razaoSocial": "VARCHAR",
                "orgaoEntidade_poderId": "VARCHAR",
                "unidadeOrgao_codigoUnidade": "VARCHAR",
                "unidadeOrgao_nomeUnidade": "VARCHAR",
                "modalidadeId": "INTEGER",
                "modalidadeNome": "VARCHAR",
                "valorInicial": "DECIMAL(18,2)",
                "dataPublicacao": "TIMESTAMP",
                "dataVigenciaInicio": "TIMESTAMP",
                "dataVigenciaFim": "TIMESTAMP",
                "objetoContrato": "VARCHAR",
                "informacaoComplementar": "VARCHAR",
                "numeroProcesso": "VARCHAR",
                "linkSistemaOrigem": "VARCHAR",
                "dataInclusao": "TIMESTAMP",
                "dataAtualizacao": "TIMESTAMP",
                "usuarioNome": "VARCHAR",
            },
            "mappers": {
                "numeroControlePNCP": lambda row: row.get("numeroControlePNCP"),
                "anoCompra": lambda row: row.get("anoCompra"),
                "sequencialCompra": lambda row: row.get("sequencialCompra"),
                "orgaoEntidade_cnpj": lambda row: row.get("orgaoEntidade", {}).get(
                    "cnpj"
                ),
                "orgaoEntidade_razaoSocial": lambda row: row.get(
                    "orgaoEntidade", {}
                ).get("razaoSocial"),
                "orgaoEntidade_poderId": lambda row: row.get("orgaoEntidade", {}).get(
                    "poderId"
                ),
                "unidadeOrgao_codigoUnidade": lambda row: row.get(
                    "unidadeOrgao", {}
                ).get("codigoUnidade"),
                "unidadeOrgao_nomeUnidade": lambda row: row.get(
                    "unidadeOrgao", {}
                ).get("nomeUnidade"),
                "modalidadeId": lambda row: row.get("modalidadeId"),
                "modalidadeNome": lambda row: row.get("modalidadeNome"),
                "valorInicial": lambda row: row.get("valorInicial"),
                "dataPublicacao": lambda row: row.get("dataPublicacao"),
                "dataVigenciaInicio": lambda row: row.get("dataVigenciaInicio"),
                "dataVigenciaFim": lambda row: row.get("dataVigenciaFim"),
                "objetoContrato": lambda row: row.get("objetoContrato"),
                "informacaoComplementar": lambda row: row.get(
                    "informacaoComplementar"
                ),
                "numeroProcesso": lambda row: row.get("numeroProcesso"),
                "linkSistemaOrigem": lambda row: row.get("linkSistemaOrigem"),
                "dataInclusao": lambda row: row.get("dataInclusao"),
                "dataAtualizacao": lambda row: row.get("dataAtualizacao"),
                "usuarioNome": lambda row: row.get("usuarioNome"),
            },
        },
        "contratacoes_publicacao": {
            "primary_key": "numeroControlePNCP",
            "columns": {
                "numeroControlePNCP": "VARCHAR",
                "anoCompra": "INTEGER",
                "sequencialCompra": "INTEGER",
                "dataAtualizacao": "TIMESTAMP",
                "orgaoEntidade_cnpj": "VARCHAR",
                "orgaoEntidade_razaoSocial": "VARCHAR",
                "orgaoEntidade_poderId": "VARCHAR",
                "orgaoEntidade_esferaId": "VARCHAR",
                "unidadeOrgao_ufNome": "VARCHAR",
                "unidadeOrgao_codigoUnidade": "VARCHAR",
                "unidadeOrgao_ufSigla": "VARCHAR",
                "unidadeOrgao_municipioNome": "VARCHAR",
                "unidadeOrgao_nomeUnidade": "VARCHAR",
                "unidadeOrgao_codigoIbge": "VARCHAR",
                "numeroCompra": "VARCHAR",
                "processo": "VARCHAR",
                "objetoCompra": "VARCHAR",
                "valorTotalHomologado": "DECIMAL(18,2)",
                "srp": "BOOLEAN",
                "dataInclusao": "TIMESTAMP",
                "amparoLegal_codigo": "INTEGER",
                "amparoLegal_nome": "VARCHAR",
                "amparoLegal_descricao": "VARCHAR",
                "dataAberturaProposta": "TIMESTAMP",
                "dataEncerramentoProposta": "TIMESTAMP",
                "informacaoComplementar": "VARCHAR",
                "linkSistemaOrigem": "VARCHAR",
                "justificativaPresencial": "VARCHAR",
                "dataPublicacaoPncp": "TIMESTAMP",
                "modalidadeId": "INTEGER",
                "dataAtualizacaoGlobal": "TIMESTAMP",
                "linkProcessoEletronico": "VARCHAR",
                "modoDisputaId": "INTEGER",
                "tipoInstrumentoConvocatorioNome": "VARCHAR",
                "tipoInstrumentoConvocatorioCodigo": "INTEGER",
                "modalidadeNome": "VARCHAR",
                "valorTotalEstimado": "DECIMAL(18,2)",
                "modoDisputaNome": "VARCHAR",
                "situacaoCompraId": "INTEGER",
                "situacaoCompraNome": "VARCHAR",
                "usuarioNome": "VARCHAR",
            },
            "mappers": {
                "numeroControlePNCP": lambda row: row.get("numeroControlePNCP"),
                "anoCompra": lambda row: row.get("anoCompra"),
                "sequencialCompra": lambda row: row.get("sequencialCompra"),
                "dataAtualizacao": lambda row: row.get("dataAtualizacao"),
                "orgaoEntidade_cnpj": lambda row: row.get("orgaoEntidade", {}).get(
                    "cnpj"
                ),
                "orgaoEntidade_razaoSocial": lambda row: row.get(
                    "orgaoEntidade", {}
                ).get("razaoSocial"),
                "orgaoEntidade_poderId": lambda row: row.get("orgaoEntidade", {}).get(
                    "poderId"
                ),
                "orgaoEntidade_esferaId": lambda row: row.get("orgaoEntidade", {}).get(
                    "esferaId"
                ),
                "unidadeOrgao_ufNome": lambda row: row.get("unidadeOrgao", {}).get(
                    "ufNome"
                ),
                "unidadeOrgao_codigoUnidade": lambda row: row.get(
                    "unidadeOrgao", {}
                ).get("codigoUnidade"),
                "unidadeOrgao_ufSigla": lambda row: row.get("unidadeOrgao", {}).get(
                    "ufSigla"
                ),
                "unidadeOrgao_municipioNome": lambda row: row.get(
                    "unidadeOrgao", {}
                ).get("municipioNome"),
                "unidadeOrgao_nomeUnidade": lambda row: row.get(
                    "unidadeOrgao", {}
                ).get("nomeUnidade"),
                "unidadeOrgao_codigoIbge": lambda row: row.get(
                    "unidadeOrgao", {}
                ).get("codigoIbge"),
                "numeroCompra": lambda row: row.get("numeroCompra"),
                "processo": lambda row: row.get("processo"),
                "objetoCompra": lambda row: row.get("objetoCompra"),
                "valorTotalHomologado": lambda row: row.get("valorTotalHomologado"),
                "srp": lambda row: row.get("srp"),
                "dataInclusao": lambda row: row.get("dataInclusao"),
                "amparoLegal_codigo": lambda row: row.get("amparoLegal", {}).get(
                    "codigo"
                ),
                "amparoLegal_nome": lambda row: row.get("amparoLegal", {}).get("nome"),
                "amparoLegal_descricao": lambda row: row.get("amparoLegal", {}).get(
                    "descricao"
                ),
                "dataAberturaProposta": lambda row: row.get("dataAberturaProposta"),
                "dataEncerramentoProposta": lambda row: row.get(
                    "dataEncerramentoProposta"
                ),
                "informacaoComplementar": lambda row: row.get(
                    "informacaoComplementar"
                ),
                "linkSistemaOrigem": lambda row: row.get("linkSistemaOrigem"),
                "justificativaPresencial": lambda row: row.get(
                    "justificativaPresencial"
                ),
                "dataPublicacaoPncp": lambda row: row.get("dataPublicacaoPncp"),
                "modalidadeId": lambda row: row.get("modalidadeId"),
                "dataAtualizacaoGlobal": lambda row: row.get("dataAtualizacaoGlobal"),
                "linkProcessoEletronico": lambda row: row.get(
                    "linkProcessoEletronico"
                ),
                "modoDisputaId": lambda row: row.get("modoDisputaId"),
                "tipoInstrumentoConvocatorioNome": lambda row: row.get(
                    "tipoInstrumentoConvocatorioNome"
                ),
                "tipoInstrumentoConvocatorioCodigo": lambda row: row.get(
                    "tipoInstrumentoConvocatorioCodigo"
                ),
                "modalidadeNome": lambda row: row.get("modalidadeNome"),
                "valorTotalEstimado": lambda row: row.get("valorTotalEstimado"),
                "modoDisputaNome": lambda row: row.get("modoDisputaNome"),
                "situacaoCompraId": lambda row: row.get("situacaoCompraId"),
                "situacaoCompraNome": lambda row: row.get("situacaoCompraNome"),
                "usuarioNome": lambda row: row.get("usuarioNome"),
            },
        },
    }

    def __init__(
        self,
        db_path: Path,
        dataset: str = "baliza_raw",
        base_url: str = "https://pncp.gov.br/api/consulta/v1",
    ):
        self.db_path = db_path
        # Validate dataset name to prevent SQL injection
        self.dataset = validate_identifier(dataset)
        self.base_url = base_url
        self.client = httpx.Client(timeout=30.0)

    def _ensure_schema(self, con: duckdb.DuckDBPyConnection, resource: str) -> None:
        """Create schema and tables if they don't exist."""
        if resource not in self.RESOURCE_CONFIG:
            raise ValueError(f"Unknown resource: {resource}")

        config = self.RESOURCE_CONFIG[resource]
        columns = config["columns"]
        primary_key = config["primary_key"]

        # Data schema
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dataset}")

        columns_sql = ",\n".join(
            f'                "{col_name}" {col_type}'
            for col_name, col_type in columns.items()
        )
        # Add primary key constraint
        columns_sql += f',\n                PRIMARY KEY ("{primary_key}")'

        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.dataset}.{resource} (
{columns_sql}
            )
        """
        con.execute(create_table_sql)

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
            self._ensure_schema(con, resource)

            if all_rows:
                config = self.RESOURCE_CONFIG[resource]
                mappers = config["mappers"]
                columns = list(mappers.keys())

                # Prepare data for insertion
                values = []
                for row in all_rows:
                    values.append(tuple(mappers[col](row) for col in columns))

                # Insert or ignore (append-only, deduplication by primary key)
                placeholders = ", ".join(["?"] * len(columns))
                insert_sql = f"""
                    INSERT OR IGNORE INTO {self.dataset}.{resource}
                    ({', '.join(f'"{c}"' for c in columns)})
                    VALUES ({placeholders})
                """
                con.executemany(insert_sql, values)

                console.print(
                    f"[green]✓ Inserted {len(all_rows)} rows into {self.dataset}.{resource} (duplicates ignored)"
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
