"""Simple PNCP data extraction without dlt.

Replaces the dlt pipeline with straightforward httpx + DuckDB code.
Supports per-page checkpointing for resume on timeout.
"""

from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict

import duckdb
import httpx
import pyarrow as pa
import pyarrow.compute as pc
import structlog
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .utils import (
    scrub_url_params,
    secure_url_connection_params,
    validate_identifier,
    validate_resource_path,
)

# Configure logger for retry logging
logger = structlog.get_logger()

console = Console()


class CheckpointData(TypedDict):
    """Data structure for checkpoint state."""

    current_page: int
    total_pages: int
    rows_extracted: int


# Arrow schema for PNCP API response — full RecuperarContratoDTO surface.
# Nested structs are captured as-is; flattening happens in the INSERT SQL.
PNCP_ARROW_SCHEMA = pa.schema(
    [
        # --- Identity ---
        ("numeroControlePNCP", pa.string()),
        ("numeroControlePncpCompra", pa.string()),  # FK → contratacoes
        ("anoContrato", pa.int64()),
        ("sequencialContrato", pa.int64()),
        ("anoCompra", pa.int64()),  # legacy field, kept for compat
        ("sequencialCompra", pa.int64()),
        ("numeroContratoEmpenho", pa.string()),
        ("numeroRetificacao", pa.int64()),  # 0=original, >0=amendment
        ("processo", pa.string()),
        # --- Órgão contratante ---
        (
            "orgaoEntidade",
            pa.struct(
                [
                    ("cnpj", pa.string()),
                    ("razaoSocial", pa.string()),
                    ("poderId", pa.string()),  # E/L/J
                    ("esferaId", pa.string()),  # F/E/M/D
                ]
            ),
        ),
        (
            "unidadeOrgao",
            pa.struct(
                [
                    ("codigoUnidade", pa.string()),
                    ("nomeUnidade", pa.string()),
                    ("ufSigla", pa.string()),
                    ("ufNome", pa.string()),
                    ("municipioNome", pa.string()),
                    ("codigoIbge", pa.string()),
                ]
            ),
        ),
        # --- Órgão sub-rogado (proxy agency, nullable) ---
        (
            "orgaoSubRogado",
            pa.struct(
                [
                    ("cnpj", pa.string()),
                    ("razaoSocial", pa.string()),
                    ("poderId", pa.string()),
                    ("esferaId", pa.string()),
                ]
            ),
        ),
        (
            "unidadeSubRogada",
            pa.struct(
                [
                    ("codigoUnidade", pa.string()),
                    ("nomeUnidade", pa.string()),
                    ("ufSigla", pa.string()),
                    ("ufNome", pa.string()),
                    ("municipioNome", pa.string()),
                    ("codigoIbge", pa.string()),
                ]
            ),
        ),
        # --- Fornecedor (supplier) ---
        ("niFornecedor", pa.string()),  # CNPJ or CPF — the WHO got paid
        ("tipoPessoa", pa.string()),  # PJ / PF / PE
        ("nomeRazaoSocialFornecedor", pa.string()),
        ("codigoPaisFornecedor", pa.string()),  # for PE (foreign entities)
        ("niFornecedorSubContratado", pa.string()),
        ("nomeFornecedorSubContratado", pa.string()),
        ("tipoPessoaSubContratada", pa.string()),
        # --- Classification ---
        ("modalidadeId", pa.int64()),
        ("modalidadeNome", pa.string()),
        (
            "tipoContrato",
            pa.struct([("id", pa.int64()), ("nome", pa.string())]),
        ),
        (
            "categoriaProcesso",
            pa.struct([("id", pa.int64()), ("nome", pa.string())]),
        ),
        ("receita", pa.bool_()),  # TRUE = revenue contract
        # --- Financial ---
        ("valorInicial", pa.float64()),
        ("valorParcela", pa.float64()),
        ("valorGlobal", pa.float64()),  # total value including amendments
        ("valorAcumulado", pa.float64()),  # accumulated payments so far
        ("numeroParcelas", pa.int64()),
        # --- Dates ---
        ("dataPublicacao", pa.string()),  # ← primary partition key
        ("dataPublicacaoPncp", pa.string()),
        ("dataAssinatura", pa.string()),
        ("dataVigenciaInicio", pa.string()),
        ("dataVigenciaFim", pa.string()),
        ("dataInclusao", pa.string()),
        ("dataAtualizacao", pa.string()),
        ("dataAtualizacaoGlobal", pa.string()),
        # --- Text / links ---
        ("objetoContrato", pa.string()),
        ("informacaoComplementar", pa.string()),
        ("linkSistemaOrigem", pa.string()),  # kept from old schema
        ("identificadorCipi", pa.string()),
        ("urlCipi", pa.string()),
        # --- Audit ---
        ("usuarioNome", pa.string()),
    ]
)


DEFAULT_MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10 MB


# Columns that need to be cast to timestamp for efficient DuckDB insertion
TIMESTAMP_COLS = {
    "dataPublicacao",
    "dataPublicacaoPncp",
    "dataVigenciaInicio",
    "dataVigenciaFim",
    "dataInclusao",
    "dataAtualizacao",
    "dataAtualizacaoGlobal",
}

# Date-only columns (not timestamp)
DATE_COLS = {
    "dataAssinatura",
}


def _is_retryable_error(exc: BaseException) -> bool:
    """Check if an exception is retryable.

    Only retries on:
    - HTTP 429 (Too Many Requests)
    - HTTP 5xx (Server errors)
    - Connection errors
    - Timeout exceptions

    Does NOT retry on:
    - HTTP 4xx client errors (except 429)
    - ValueError (response too large)
    - Other exceptions
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException))


def _log_retry(retry_state: RetryCallState) -> None:
    """Log retry attempts with useful context."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    attempt = retry_state.attempt_number

    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        logger.warning(
            "retry_attempt",
            attempt=attempt,
            max_attempts=5,
            kind="http_error",
            status=status,
            url=scrub_url_params(str(exc.request.url)),
        )
    elif isinstance(exc, httpx.ConnectError):
        logger.warning(
            "retry_attempt",
            attempt=attempt,
            max_attempts=5,
            kind="connection_error",
            error=scrub_url_params(str(exc)),
        )
    elif isinstance(exc, httpx.TimeoutException):
        logger.warning(
            "retry_attempt",
            attempt=attempt,
            max_attempts=5,
            kind="timeout",
            error=scrub_url_params(str(exc)),
        )
    else:
        logger.warning(
            "retry_attempt",
            attempt=attempt,
            max_attempts=5,
            kind="unknown",
            error=scrub_url_params(str(exc)),
        )


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception(_is_retryable_error),
    before_sleep=_log_retry,
    reraise=True,
)
def _fetch_page(
    client: httpx.Client,
    url: str,
    params: dict,
    max_size: int = DEFAULT_MAX_RESPONSE_SIZE,
) -> dict:
    """Fetch a single page from PNCP API with retry logic and size limit.

    Uses exponential backoff with retries on:
    - HTTP 429 (rate limit)
    - HTTP 5xx (server errors)
    - Connection errors
    - Timeouts

    Args:
        client: HTTP client to use
        url: API endpoint URL
        params: Query parameters
        max_size: Maximum response size in bytes

    Returns:
        Parsed JSON response

    Raises:
        httpx.HTTPStatusError: On non-retryable HTTP errors or after max retries
        ValueError: If response exceeds max_size
    """
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
        self._db_lock = threading.Lock()

        # Resolve URL and get security headers to prevent DNS Rebinding (SSRF)
        self.base_url, security_headers = secure_url_connection_params(base_url)

        # Add User-Agent and security headers
        headers = {
            "User-Agent": "curl/8.14.1",
            "Accept": "*/*",
            **security_headers,
        }
        self.client = httpx.Client(
            timeout=httpx.Timeout(180.0, connect=30.0), 
            headers=headers,
            http2=True,  # Match curl's performance
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50)
        )

        # Pre-compute SQL queries to avoid reconstruction in loops
        # Optimization: Hoist invariant SQL construction out of the loop
        self._insert_sql = f"""
            INSERT OR REPLACE INTO {self.dataset}.contratos (
                numero_controle_pncp,
                numero_controle_pncp_compra,
                ano_contrato,
                sequencial_contrato,
                ano_compra,
                sequencial_compra,
                numero_contrato_empenho,
                numero_retificacao,
                processo,
                cnpj_orgao,
                razao_social_orgao,
                poder_id,
                esfera_id,
                codigo_unidade,
                nome_unidade,
                uf_sigla,
                uf_nome,
                municipio_nome,
                codigo_ibge,
                cnpj_orgao_subrogado,
                razao_social_orgao_subrogado,
                codigo_unidade_subrogada,
                nome_unidade_subrogada,
                uf_sigla_subrogada,
                ni_fornecedor,
                tipo_pessoa,
                nome_razao_social_fornecedor,
                codigo_pais_fornecedor,
                ni_fornecedor_subcontratado,
                nome_fornecedor_subcontratado,
                tipo_pessoa_subcontratada,
                modalidade_id,
                modalidade_nome,
                tipo_contrato_id,
                tipo_contrato_nome,
                categoria_processo_id,
                categoria_processo_nome,
                receita,
                valor_inicial,
                valor_parcela,
                valor_global,
                valor_acumulado,
                numero_parcelas,
                data_publicacao,
                data_publicacao_pncp,
                data_assinatura,
                data_vigencia_inicio,
                data_vigencia_fim,
                data_inclusao,
                data_atualizacao,
                data_atualizacao_global,
                objeto_contrato,
                informacao_complementar,
                link_sistema_origem,
                identificador_cipi,
                url_cipi,
                usuario_nome
            )
            SELECT
                numeroControlePNCP,
                numeroControlePncpCompra,
                anoContrato,
                sequencialContrato,
                anoCompra,
                sequencialCompra,
                numeroContratoEmpenho,
                COALESCE(numeroRetificacao, 0),
                processo,
                orgaoEntidade.cnpj,
                orgaoEntidade.razaoSocial,
                orgaoEntidade.poderId,
                orgaoEntidade.esferaId,
                unidadeOrgao.codigoUnidade,
                unidadeOrgao.nomeUnidade,
                unidadeOrgao.ufSigla,
                unidadeOrgao.ufNome,
                unidadeOrgao.municipioNome,
                unidadeOrgao.codigoIbge,
                orgaoSubRogado.cnpj,
                orgaoSubRogado.razaoSocial,
                unidadeSubRogada.codigoUnidade,
                unidadeSubRogada.nomeUnidade,
                unidadeSubRogada.ufSigla,
                niFornecedor,
                tipoPessoa,
                nomeRazaoSocialFornecedor,
                codigoPaisFornecedor,
                niFornecedorSubContratado,
                nomeFornecedorSubContratado,
                tipoPessoaSubContratada,
                modalidadeId,
                modalidadeNome,
                tipoContrato.id,
                tipoContrato.nome,
                categoriaProcesso.id,
                categoriaProcesso.nome,
                receita,
                valorInicial,
                valorParcela,
                valorGlobal,
                valorAcumulado,
                numeroParcelas,
                COALESCE(dataPublicacao, dataPublicacaoPncp) AS data_publicacao,
                dataPublicacaoPncp,
                TRY_CAST(dataAssinatura AS DATE),
                TRY_CAST(dataVigenciaInicio AS DATE),
                TRY_CAST(dataVigenciaFim AS DATE),
                dataInclusao,
                dataAtualizacao,
                dataAtualizacaoGlobal,
                objetoContrato,
                informacaoComplementar,
                linkSistemaOrigem,
                identificadorCipi,
                urlCipi,
                usuarioNome
            FROM arrow_table
        """

        # Dimension upsert SQLs — populated on each page insert
        self._upsert_orgao_sql = f"""
            INSERT INTO {self.dataset}.dim_orgaos
                (cnpj, razao_social, poder_id, esfera_id, first_seen_at, last_seen_at, last_seen_resource)
            VALUES (?, ?, ?, ?, NOW(), NOW(), 'contratos')
            ON CONFLICT (cnpj) DO UPDATE SET
                last_seen_at = NOW(),
                razao_social = COALESCE(EXCLUDED.razao_social, dim_orgaos.razao_social)
        """

        self._upsert_unidade_sql = f"""
            INSERT INTO {self.dataset}.dim_unidades
                (codigo_unidade, cnpj_orgao, nome_unidade, uf_sigla, uf_nome,
                 municipio_nome, codigo_ibge, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
            ON CONFLICT (codigo_unidade, cnpj_orgao) DO UPDATE SET
                last_seen_at = NOW()
        """

        self._upsert_fornecedor_sql = f"""
            INSERT INTO {self.dataset}.dim_fornecedores
                (ni_fornecedor, tipo_pessoa, nome_razao_social, codigo_pais,
                 first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, NOW(), NOW())
            ON CONFLICT (ni_fornecedor) DO UPDATE SET
                last_seen_at = NOW(),
                nome_razao_social = COALESCE(EXCLUDED.nome_razao_social, dim_fornecedores.nome_razao_social)
        """

    def _ensure_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create schema and tables if they don't exist.

        Implements the normalized relational schema from the PNCP data model design.
        Dimension tables (dim_*) are populated during ingestion.
        Fact table (contratos) is the primary data store.
        """
        # ── Data schema ──────────────────────────────────────────────────────────
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dataset}")

        # Dimension: government agencies
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.dataset}.dim_orgaos (
                cnpj                VARCHAR PRIMARY KEY,
                razao_social        VARCHAR,
                poder_id            VARCHAR,   -- E=Executive L=Legislative J=Judiciary
                esfera_id           VARCHAR,   -- F=Federal E=Estadual M=Municipal D=DF
                first_seen_at       TIMESTAMP,
                last_seen_at        TIMESTAMP,
                last_seen_resource  VARCHAR
            )
        """)

        # Dimension: administrative units (codigoUnidade is not globally unique)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.dataset}.dim_unidades (
                codigo_unidade      VARCHAR,
                cnpj_orgao          VARCHAR,
                nome_unidade        VARCHAR,
                uf_sigla            VARCHAR,
                uf_nome             VARCHAR,
                municipio_nome      VARCHAR,
                codigo_ibge         VARCHAR,
                last_seen_at        TIMESTAMP,
                PRIMARY KEY (codigo_unidade, cnpj_orgao)
            )
        """)

        # Dimension: suppliers (the WHO got paid)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.dataset}.dim_fornecedores (
                ni_fornecedor       VARCHAR PRIMARY KEY,  -- CNPJ or CPF
                tipo_pessoa         VARCHAR,              -- PJ/PF/PE
                nome_razao_social   VARCHAR,
                codigo_pais         VARCHAR,              -- for PE (foreign entities)
                first_seen_at       TIMESTAMP,
                last_seen_at        TIMESTAMP
            )
        """)

        # Fact: signed contracts — full RecuperarContratoDTO surface
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.dataset}.contratos (
                -- Identity
                numero_controle_pncp            VARCHAR PRIMARY KEY,
                numero_controle_pncp_compra     VARCHAR,   -- FK → contratacoes
                ano_contrato                    INTEGER,
                sequencial_contrato             INTEGER,
                ano_compra                      INTEGER,   -- legacy compat
                sequencial_compra               INTEGER,
                numero_contrato_empenho         VARCHAR,
                numero_retificacao              INTEGER DEFAULT 0,
                processo                        VARCHAR,
                -- Órgão contratante
                cnpj_orgao                      VARCHAR,
                razao_social_orgao              VARCHAR,
                poder_id                        VARCHAR,
                esfera_id                       VARCHAR,
                codigo_unidade                  VARCHAR,
                nome_unidade                    VARCHAR,
                uf_sigla                        VARCHAR,
                uf_nome                         VARCHAR,
                municipio_nome                  VARCHAR,
                codigo_ibge                     VARCHAR,
                -- Órgão sub-rogado (nullable)
                cnpj_orgao_subrogado            VARCHAR,
                razao_social_orgao_subrogado    VARCHAR,
                codigo_unidade_subrogada        VARCHAR,
                nome_unidade_subrogada          VARCHAR,
                uf_sigla_subrogada              VARCHAR,
                -- Fornecedor
                ni_fornecedor                   VARCHAR,   -- CNPJ or CPF
                tipo_pessoa                     VARCHAR,   -- PJ/PF/PE
                nome_razao_social_fornecedor    VARCHAR,
                codigo_pais_fornecedor          VARCHAR,
                ni_fornecedor_subcontratado     VARCHAR,
                nome_fornecedor_subcontratado   VARCHAR,
                tipo_pessoa_subcontratada       VARCHAR,
                -- Classification
                modalidade_id                   INTEGER,
                modalidade_nome                 VARCHAR,
                tipo_contrato_id                INTEGER,
                tipo_contrato_nome              VARCHAR,
                categoria_processo_id           INTEGER,
                categoria_processo_nome         VARCHAR,
                receita                         BOOLEAN,
                -- Financial
                valor_inicial                   DECIMAL(18,2),
                valor_parcela                   DECIMAL(18,2),
                valor_global                    DECIMAL(18,2),
                valor_acumulado                 DECIMAL(18,2),
                numero_parcelas                 INTEGER,
                -- Dates
                data_publicacao                 TIMESTAMP,  -- partition key
                data_publicacao_pncp            TIMESTAMP,
                data_assinatura                 DATE,
                data_vigencia_inicio            DATE,
                data_vigencia_fim               DATE,
                data_inclusao                   TIMESTAMP,
                data_atualizacao                TIMESTAMP,
                data_atualizacao_global         TIMESTAMP,
                -- Text / links
                objeto_contrato                 VARCHAR,
                informacao_complementar         VARCHAR,
                link_sistema_origem             VARCHAR,
                identificador_cipi              VARCHAR,
                url_cipi                        VARCHAR,
                -- Audit
                usuario_nome                    VARCHAR
            )
        """)

        # ── State schema ──────────────────────────────────────────────────────────
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
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.uploaded_to_ia (
                item_id VARCHAR PRIMARY KEY,
                extraction_date DATE,
                uploaded_at TIMESTAMP,
                file_count INTEGER,
                total_rows INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.resource_health (
                resource VARCHAR PRIMARY KEY,
                consecutive_empty_days INTEGER DEFAULT 0,
                last_nonempty_date DATE,
                last_checked_date DATE,
                status VARCHAR DEFAULT 'healthy',
                updated_at TIMESTAMP
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
        with self._db_lock:
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
        with self._db_lock:
            con.execute(
                """
                DELETE FROM baliza_state.extraction_checkpoint
                WHERE resource = ? AND extraction_date = ?
            """,
                [resource, extraction_date.date()],
            )

    # -- Circuit breaker / resource health tracking --

    STALL_WARNING_THRESHOLD = 3
    STALL_CRITICAL_THRESHOLD = 7

    def _update_resource_health(
        self,
        con: duckdb.DuckDBPyConnection,
        resource: str,
        checked_date: date,
        rows_extracted: int,
    ) -> None:
        """Update resource health tracking for stall detection."""
        existing = con.execute(
            "SELECT consecutive_empty_days FROM baliza_state.resource_health WHERE resource = ?",
            [resource],
        ).fetchone()

        if rows_extracted == 0:
            new_count = (existing[0] + 1) if existing else 1
            if new_count >= self.STALL_CRITICAL_THRESHOLD:
                new_status = "stalled"
            elif new_count >= self.STALL_WARNING_THRESHOLD:
                new_status = "warning"
            else:
                new_status = "healthy"

            # Preserve last_nonempty_date from existing row
            last_nonempty = con.execute(
                "SELECT last_nonempty_date FROM baliza_state.resource_health WHERE resource = ?",
                [resource],
            ).fetchone()
            last_nonempty_date = last_nonempty[0] if last_nonempty else None

            with self._db_lock:
                con.execute(
                    """INSERT OR REPLACE INTO baliza_state.resource_health
                    VALUES (?, ?, ?, ?, ?, NOW())""",
                    [resource, new_count, last_nonempty_date, checked_date, new_status],
                )

            if new_status != "healthy":
                logger.warning(
                    "resource_health_degraded",
                    resource=resource,
                    consecutive_empty_days=new_count,
                    status=new_status,
                )
        else:
            with self._db_lock:
                con.execute(
                    """INSERT OR REPLACE INTO baliza_state.resource_health
                    VALUES (?, 0, ?, ?, 'healthy', NOW())""",
                    [resource, checked_date, checked_date],
                )

    def _insert_page(self, con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
        """Insert a single page of results, updating dimension tables."""
        if not rows:
            return 0

        # Create Arrow table from rows with explicit schema to handle nested fields
        # This is significantly faster than iterating in Python (~250x speedup)
        try:
            arrow_table = pa.Table.from_pylist(rows, schema=PNCP_ARROW_SCHEMA)  # noqa: F841

            # Cast timestamp string columns to pa.timestamp for efficient DuckDB insertion
            new_columns = []
            new_fields = []
            for i, field in enumerate(arrow_table.schema):
                col = arrow_table.column(i)
                if field.name in TIMESTAMP_COLS:
                    col = pc.cast(col, pa.timestamp("us"))
                    new_fields.append(field.with_type(pa.timestamp("us")))
                else:
                    new_fields.append(field)
                new_columns.append(col)
            arrow_table = pa.Table.from_arrays(new_columns, schema=pa.schema(new_fields))

        except Exception as e:
            msg = scrub_url_params(str(e))
            console.print(
                f"[yellow]Warning: Arrow conversion failed ({msg}), falling back to slow path"
            )
            return self._insert_page_slow(con, rows)

        # 1. Insert fact rows (INSERT OR REPLACE — idempotent on numero_controle_pncp)
        con.execute(self._insert_sql)

        # 2. Upsert dimension tables from the same page data
        #    These are best-effort: dimension misses don't block fact ingestion.
        try:
            for row in rows:
                orgao = row.get("orgaoEntidade") or {}
                unidade = row.get("unidadeOrgao") or {}
                orgao_sub = row.get("orgaoSubRogado") or {}
                unidade_sub = row.get("unidadeSubRogada") or {}

                # Primary agency
                if orgao.get("cnpj"):
                    con.execute(
                        self._upsert_orgao_sql,
                        [
                            orgao["cnpj"],
                            orgao.get("razaoSocial"),
                            orgao.get("poderId"),
                            orgao.get("esferaId"),
                        ],
                    )

                # Administrative unit
                if unidade.get("codigoUnidade") and orgao.get("cnpj"):
                    con.execute(
                        self._upsert_unidade_sql,
                        [
                            unidade["codigoUnidade"],
                            orgao["cnpj"],
                            unidade.get("nomeUnidade"),
                            unidade.get("ufSigla"),
                            unidade.get("ufNome"),
                            unidade.get("municipioNome"),
                            unidade.get("codigoIbge"),
                        ],
                    )

                # Sub-rogated agency (proxy)
                if orgao_sub.get("cnpj"):
                    con.execute(
                        self._upsert_orgao_sql,
                        [
                            orgao_sub["cnpj"],
                            orgao_sub.get("razaoSocial"),
                            orgao_sub.get("poderId"),
                            orgao_sub.get("esferaId"),
                        ],
                    )
                if unidade_sub.get("codigoUnidade") and orgao_sub.get("cnpj"):
                    con.execute(
                        self._upsert_unidade_sql,
                        [
                            unidade_sub["codigoUnidade"],
                            orgao_sub["cnpj"],
                            unidade_sub.get("nomeUnidade"),
                            unidade_sub.get("ufSigla"),
                            unidade_sub.get("ufNome"),
                            unidade_sub.get("municipioNome"),
                            unidade_sub.get("codigoIbge"),
                        ],
                    )

                # Supplier
                ni = row.get("niFornecedor")
                if ni:
                    con.execute(
                        self._upsert_fornecedor_sql,
                        [
                            ni,
                            row.get("tipoPessoa"),
                            row.get("nomeRazaoSocialFornecedor"),
                            row.get("codigoPaisFornecedor"),
                        ],
                    )

        except Exception as dim_err:
            msg = scrub_url_params(str(dim_err))
            logger.warning("dim_upsert_failed", error=msg)
            # Dimension failures are non-fatal — fact data was already committed

        return len(rows)

    def _insert_page_slow(self, con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]]) -> int:
        """Fallback insertion method — pure Python, no Arrow dependency."""
        inserted = 0
        for row in rows:
            orgao = row.get("orgaoEntidade") or {}
            unidade = row.get("unidadeOrgao") or {}
            orgao_sub = row.get("orgaoSubRogado") or {}
            unidade_sub = row.get("unidadeSubRogada") or {}
            tipo_contrato = row.get("tipoContrato") or {}
            categoria = row.get("categoriaProcesso") or {}
            con.execute(
                f"""
                INSERT OR REPLACE INTO {self.dataset}.contratos VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )""",
                [
                    row.get("numeroControlePNCP"),
                    row.get("numeroControlePncpCompra"),
                    row.get("anoContrato"),
                    row.get("sequencialContrato"),
                    row.get("anoCompra"),
                    row.get("sequencialCompra"),
                    row.get("numeroContratoEmpenho"),
                    row.get("numeroRetificacao", 0),
                    row.get("processo"),
                    orgao.get("cnpj"),
                    orgao.get("razaoSocial"),
                    orgao.get("poderId"),
                    orgao.get("esferaId"),
                    unidade.get("codigoUnidade"),
                    unidade.get("nomeUnidade"),
                    unidade.get("ufSigla"),
                    unidade.get("ufNome"),
                    unidade.get("municipioNome"),
                    unidade.get("codigoIbge"),
                    orgao_sub.get("cnpj"),
                    orgao_sub.get("razaoSocial"),
                    unidade_sub.get("codigoUnidade"),
                    unidade_sub.get("nomeUnidade"),
                    unidade_sub.get("ufSigla"),
                    row.get("niFornecedor"),
                    row.get("tipoPessoa"),
                    row.get("nomeRazaoSocialFornecedor"),
                    row.get("codigoPaisFornecedor"),
                    row.get("niFornecedorSubContratado"),
                    row.get("nomeFornecedorSubContratado"),
                    row.get("tipoPessoaSubContratada"),
                    row.get("modalidadeId"),
                    row.get("modalidadeNome"),
                    tipo_contrato.get("id"),
                    tipo_contrato.get("nome"),
                    categoria.get("id"),
                    categoria.get("nome"),
                    row.get("receita"),
                    row.get("valorInicial"),
                    row.get("valorParcela"),
                    row.get("valorGlobal"),
                    row.get("valorAcumulado"),
                    row.get("numeroParcelas"),
                    row.get("dataPublicacao"),
                    row.get("dataPublicacaoPncp"),
                    row.get("dataAssinatura"),
                    row.get("dataVigenciaInicio"),
                    row.get("dataVigenciaFim"),
                    row.get("dataInclusao"),
                    row.get("dataAtualizacao"),
                    row.get("dataAtualizacaoGlobal"),
                    row.get("objetoContrato"),
                    row.get("informacaoComplementar"),
                    row.get("linkSistemaOrigem"),
                    row.get("identificadorCipi"),
                    row.get("urlCipi"),
                    row.get("usuarioNome"),
                ],
            )
            inserted += 1
        return inserted

    def _extract_range_task(  # noqa: PLR0912, PLR0915
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str,
        progress: Progress | None = None,
        deadline: datetime | None = None,
    ) -> dict[str, Any]:
        """Extract data for a date range (worker function)."""
        data_inicial = start_date.strftime("%Y%m%d")
        data_final = end_date.strftime("%Y%m%d")
        total_rows = 0
        page = 1
        total_pages = None
        expected_rows = 0

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
                        progress.update(
                            task_id,
                            description=f"Resuming {start_date.date()} p{page}/{total_pages}",
                        )

                # Prepare invariant params
                url = f"{self.base_url}/{resource}"
                params = {
                    "dataInicial": data_inicial,
                    "dataFinal": data_final,
                    "tamanhoPagina": 500,
                }

                while True:
                    # Check deadline before fetching next page
                    if deadline is not None and datetime.now() >= deadline:
                        logger.info(
                            "deadline_reached",
                            resource=resource,
                            date=str(start_date.date()),
                            pages_completed=page - 1,
                            rows=total_rows,
                        )
                        break

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
                        expected_rows = data.get("totalRegistros", 0)
                        if progress and task_id:
                            progress.update(task_id, total=total_pages)

                    # Checkpoint after each page
                    stats: CheckpointData = {
                        "current_page": page,
                        "total_pages": total_pages,  # type: ignore[required]
                        "rows_extracted": total_rows,
                    }
                    self._save_checkpoint(con, resource, start_date, stats, started_at)

                    if progress and task_id:
                        progress.update(
                            task_id,
                            completed=page,
                            description=f"Fetching {start_date.date()} {page}/{total_pages}",
                        )

                    if page >= total_pages:
                        break

                    page += 1

                # Determine completion status
                stopped_by_deadline = deadline is not None and datetime.now() >= deadline
                status = "deadline" if stopped_by_deadline else "complete"

                # Data integrity check
                if status == "complete" and total_rows != expected_rows:
                    logger.warning(
                        "extraction_count_mismatch",
                        date=str(start_date.date()),
                        expected=expected_rows,
                        actual=total_rows,
                    )
                    console.print(
                        f"[yellow]⚠ Warning: {start_date.date()} count mismatch! "
                        f"Expected {expected_rows}, got {total_rows}[/yellow]"
                    )

                # Only clear checkpoint on true completion
                if not stopped_by_deadline:
                    self._clear_checkpoint(con, resource, start_date)

                # Record coverage for this range
                with self._db_lock:
                    con.execute(
                        """
                        INSERT OR REPLACE INTO baliza_state.coverage
                        VALUES (?, ?, ?, ?, ?, ?, NOW())
                    """,
                        [resource, start_date, end_date, status, page, total_rows],
                    )

                # Update resource health (circuit breaker) for single-day extractions
                if start_date.date() == end_date.date() and status == "complete":
                    self._update_resource_health(con, resource, start_date.date(), total_rows)
        finally:
            if progress and task_id:
                progress.remove_task(task_id)

        return {
            "rows_extracted": total_rows,
            "pages": page,
            "start_date": start_date,
            "end_date": end_date,
            "failed_dates": [d.date() for d in failed_dates] if 'failed_dates' in locals() else [],
        }

    def probe_date(self, resource: str, extraction_date: datetime) -> dict[str, Any]:
        """Probe a date to find out how many pages/records it has."""
        data_inicial = extraction_date.strftime("%Y%m%d")
        data_final = extraction_date.strftime("%Y%m%d")
        url = f"{self.base_url}/{resource}"
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "tamanhoPagina": 500,
            "pagina": 1,
        }
        res = _fetch_page(self.client, url, params)
        return {
            "total_pages": res.get("totalPaginas", 0),
            "expected_rows": res.get("totalRegistros", 0),
        }

    def fetch_page(self, resource: str, extraction_date: datetime, page: int) -> int:
        """Fetch a specific page and insert into DuckDB."""
        data_inicial = extraction_date.strftime("%Y%m%d")
        data_final = extraction_date.strftime("%Y%m%d")
        url = f"{self.base_url}/{resource}"
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "tamanhoPagina": 500,
            "pagina": page,
        }
        
        data = _fetch_page(self.client, url, params)
        rows = data.get("data", [])
        if not rows:
            return 0
            
        with self._db_lock:
            with duckdb.connect(str(self.db_path)) as con:
                self._ensure_schema(con)
                return self._insert_page(con, rows)

    def extract(
        self,
        start_date: datetime,
        end_date: datetime,
        resource: str = "contratos",
        workers: int = 4,
        deadline: datetime | None = None,
    ) -> dict[str, Any]:
        """Extract data from PNCP API for a date range.

        Supports parallel extraction with multiple workers.

        Args:
            start_date: Start of date range
            end_date: End of date range
            resource: Resource type (default: contratos)
            workers: Number of concurrent workers (default: 4)
            deadline: Stop gracefully after this time (default: None = no limit)

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
                    result = self._extract_range_task(
                        start_date, end_date, resource, progress, deadline=deadline
                    )
                    total_rows = result["rows_extracted"]
                    total_pages = result["pages"]
                except Exception as e:
                    msg = scrub_url_params(str(e))
                    console.print(f"[red]✗ Failed to extract: {msg}")
                    raise
            else:
                # Parallel mode (split by day)
                dates = []
                curr = start_date
                while curr <= end_date:
                    dates.append(curr)
                    curr += timedelta(days=1)

                main_task = progress.add_task(
                    f"Overall Progress ({len(dates)} days)", total=len(dates)
                )

                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {}
                    # Calculate delay to hit target requests per second (1.0 default)
                    # This ensures we don't 'burst' all workers at once
                    delay = 1.0  # 1 request per second cadence for submission
                    
                    for date in dates:
                        f = executor.submit(
                            self._extract_range_task, date, date, resource, progress,
                            deadline
                        )
                        futures[f] = date
                        # Stagger the submissions of next tasks
                        if len(dates) > 1:
                            time.sleep(delay)

                    for future in concurrent.futures.as_completed(futures):
                        date = futures[future]
                        try:
                            result = future.result()
                            total_rows += result["rows_extracted"]
                            total_pages += result["pages"]
                            progress.update(main_task, advance=1)
                        except Exception as e:
                            msg = scrub_url_params(str(e))
                            console.print(f"[red]✗ Failed to extract {date.date()}: {msg}")
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
            "failed_dates": [d.date() for d in failed_dates],
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
                WHERE data_publicacao >= ? AND data_publicacao < ?
            """,
                [extraction_date.date(), next_day.date()],
            ).fetchone()
            row_count = result[0] if result else 0

            if row_count > 0:
                con.execute(
                    f"""
                    DELETE FROM {self.dataset}.contratos
                    WHERE data_publicacao >= ? AND data_publicacao < ?
                """,
                    [extraction_date.date(), next_day.date()],
                )

                console.print(f"[green]✓ Cleaned up {row_count} rows for {extraction_date.date()}")

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
                SELECT DISTINCT CAST(data_publicacao AS DATE) as dt
                FROM {self.dataset}.contratos
                WHERE data_publicacao < ?
                  AND CAST(data_publicacao AS DATE) NOT IN (
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
            total_rows = con.execute(f"SELECT COUNT(*) FROM {self.dataset}.contratos").fetchone()[0]

            # Rows by date
            by_date = con.execute(
                f"""
                SELECT CAST(data_publicacao AS DATE) as dt, COUNT(*) as cnt
                FROM {self.dataset}.contratos
                GROUP BY dt
                ORDER BY dt
            """
            ).fetchall()

            # Uploaded dates
            uploaded = con.execute("SELECT COUNT(*) FROM baliza_state.uploaded_to_ia").fetchone()[0]

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
