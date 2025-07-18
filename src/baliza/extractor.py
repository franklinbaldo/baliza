"""
PNCP Data Extractor V2 - True Async Architecture
Based on steel-man pseudocode: endpoint → 365-day ranges → async pagination
"""

import asyncio
import calendar
import contextlib
import json
import logging
import random
import re
import signal
import sys
import time
import uuid
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import httpx
import orjson
import typer
from filelock import FileLock, Timeout
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

# 1. Trave o runtime em UTF-8 - reconfigure streams logo no início
for std in (sys.stdin, sys.stdout, sys.stderr):
    std.reconfigure(encoding="utf-8", errors="surrogateescape")

# Configure standard logging com UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

console = Console(force_terminal=True, legacy_windows=False, stderr=False)
logger = logging.getLogger(__name__)


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

# Data directory
DATA_DIR = Path.cwd() / "data"
BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"


# 2. DuckDB: mensagens de erro - função para conectar com UTF-8
def connect_utf8(path: str) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB with UTF-8 error handling."""
    try:
        return duckdb.connect(path)
    except Exception as exc:
        # redecodifica string problema (CP‑1252 → UTF‑8)
        msg = (
            str(exc).encode("latin1", errors="ignore").decode("utf-8", errors="replace")
        )
        # Para DuckDB, usamos RuntimeError com a mensagem corrigida
        raise RuntimeError(msg) from exc


class InstrumentoConvocatorio(Enum):
    EDITAL = 1
    AVISO_CONTRATACAO_DIRETA = 2
    ATO_QUE_AUTORIZA_CONTRATACAO_DIRETA = 3


class ModalidadeContratacao(Enum):
    LEILAO_ELETRONICO = 1
    DIALOGO_COMPETITIVO = 2
    CONCURSO = 3
    CONCORRENCIA_ELETRONICA = 4
    CONCORRENCIA_PRESENCIAL = 5
    PREGAO_ELETRONICO = 6
    PREGAO_PRESENCIAL = 7
    DISPENSA_DE_LICITACAO = 8
    INEXIGIBILIDADE = 9
    MANIFESTACAO_DE_INTERESSE = 10
    PRE_QUALIFICACAO = 11
    CREDENCIAMENTO = 12
    LEILAO_PRESENCIAL = 13


class ModoDisputa(Enum):
    ABERTO = 1
    FECHADO = 2
    ABERTO_FECHADO = 3
    DISPENSA_COM_DISPUTA = 4
    NAO_SE_APLICA = 5
    FECHADO_ABERTO = 6


class CriterioJulgamento(Enum):
    MENOR_PRECO = 1
    MAIOR_DESCONTO = 2
    TECNICA_E_PRECO = 4
    MAIOR_LANCE = 5
    MAIOR_RETORNO_ECONOMICO = 6
    NAO_SE_APLICA = 7
    MELHOR_TECNICA = 8
    CONTEUDO_ARTISTICO = 9


class SituacaoContratacao(Enum):
    DIVULGADA_NO_PNCP = 1
    REVOGADA = 2
    ANULADA = 3
    SUSPENSA = 4


class SituacaoItemContratacao(Enum):
    EM_ANDAMENTO = 1
    HOMOLOGADO = 2
    ANULADO_REVOGADO_CANCELADO = 3
    DESERTO = 4
    FRACASSADO = 5


class TipoBeneficio(Enum):
    PARTICIPACAO_EXCLUSIVA_ME_EPP = 1
    SUBCONTRATACAO_PARA_ME_EPP = 2
    COTA_RESERVADA_PARA_ME_EPP = 3
    SEM_BENEFICIO = 4
    NAO_SE_APLICA = 5


class SituacaoResultadoItemContratacao(Enum):
    INFORMADO = 1
    CANCELADO = 2


class TipoContrato(Enum):
    CONTRATO = 1
    COMODATO = 2
    ARRENDAMENTO = 3
    CONCESSAO = 4
    TERMO_DE_ADESAO = 5
    CONVENIO = 6
    EMPENHO = 7
    OUTROS = 8
    TERMO_DE_EXECUCAO_DESCENTRALIZADA = 9
    ACORDO_DE_COOPERACAO_TECNICA = 10
    TERMO_DE_COMPROMISSO = 11
    CARTA_CONTRATO = 12


class TipoTermoContrato(Enum):
    TERMO_DE_RESCISAO = 1
    TERMO_ADITIVO = 2
    TERMO_DE_APOSTILamento = 3


class CategoriaProcesso(Enum):
    CESSAO = 1
    COMPRAS = 2
    INFORMATICA_TIC = 3
    INTERNACIONAL = 4
    LOCACAO_IMOVEIS = 5
    MAO_DE_OBRA = 6
    OBRAS = 7
    SERVICOS = 8
    SERVICOS_DE_ENGENHARIA = 9
    SERVICOS_DE_SAUDE = 10
    ALIENACAO_DE_BENS_MOVEIS_IMOVEIS = 11


class TipoDocumento(Enum):
    AVISO_CONTRATACAO_DIRETA = 1
    EDITAL = 2
    MINUTA_CONTRATO = 3
    TERMO_REFERENCIA = 4
    ANTEPROJETO = 5
    PROJETO_BASICO = 6
    ESTUDO_TECNICO_PRELIMINAR = 7
    PROJETO_EXECUTIVO = 8
    MAPA_RISCOS = 9
    DFD = 10
    ATA_REGISTRO_PRECO = 11
    CONTRATO = 12
    TERMO_RESCISAO = 13
    TERMO_ADITIVO = 14
    TERMO_APOSTILAMENTO = 15
    OUTROS = 16
    NOTA_EMPENHO = 17
    RELATORIO_FINAL_CONTRATO = 18


class NaturezaJuridica(Enum):
    NAO_INFORMADA = 0
    ORGAO_PUBLICO_EXECUTIVO_FEDERAL = 1015
    ORGAO_PUBLICO_EXECUTIVO_ESTADUAL_DF = 1023
    ORGAO_PUBLICO_EXECUTIVO_MUNICIPAL = 1031
    ORGAO_PUBLICO_LEGISLATIVO_FEDERAL = 1040
    ORGAO_PUBLICO_LEGISLATIVO_ESTADUAL_DF = 1058
    ORGAO_PUBLICO_LEGISLATIVO_MUNICIPAL = 1066
    ORGAO_PUBLICO_JUDICIARIO_FEDERAL = 1074
    ORGAO_PUBLICO_JUDICIARIO_ESTADUAL = 1082
    AUTARQUIA_FEDERAL = 1104
    AUTARQUIA_ESTADUAL_DF = 1112
    AUTARQUIA_MUNICIPAL = 1120
    FUNDACAO_PUBLICA_DIREITO_PUBLICO_FEDERAL = 1139
    FUNDACAO_PUBLICA_DIREITO_PUBLICO_ESTADUAL_DF = 1147
    FUNDACAO_PUBLICA_DIREITO_PUBLICO_MUNICIPAL = 1155
    ORGAO_PUBLICO_AUTONOMO_FEDERAL = 1163
    ORGAO_PUBLICO_AUTONOMO_ESTADUAL_DF = 1171
    ORGAO_PUBLICO_AUTONOMO_MUNICIPAL = 1180
    COMISSAO_POLINACIONAL = 1198
    CONSORCIO_PUBLICO_DIREITO_PUBLICO = 1210
    CONSORCIO_PUBLICO_DIREITO_PRIVADO = 1228
    ESTADO_DF = 1236
    MUNICIPIO = 1244
    FUNDACAO_PUBLICA_DIREITO_PRIVADO_FEDERAL = 1252
    FUNDACAO_PUBLICA_DIREITO_PRIVADO_ESTADUAL_DF = 1260
    FUNDACAO_PUBLICA_DIREITO_PRIVADO_MUNICIPAL = 1279
    FUNDO_PUBLICO_ADMINISTRACAO_INDIRETA_FEDERAL = 1287
    FUNDO_PUBLICO_ADMINISTRACAO_INDIRETA_ESTADUAL_DF = 1295
    FUNDO_PUBLICO_ADMINISTRACAO_INDIRETA_MUNICIPAL = 1309
    FUNDO_PUBLICO_ADMINISTRACAO_DIRETA_FEDERAL = 1317
    FUNDO_PUBLICO_ADMINISTRACAO_DIRETA_ESTADUAL_DF = 1325
    FUNDO_PUBLICO_ADMINISTRACAO_DIRETA_MUNICIPAL = 1333
    UNIAO = 1341
    EMPRESA_PUBLICA = 2011
    SOCIEDADE_ECONOMIA_MISTA = 2038
    SOCIEDADE_ANONIMA_ABERTA = 2046
    SOCIEDADE_ANONIMA_FECHADA = 2054


class PorteEmpresa(Enum):
    ME = 1
    EPP = 2
    DEMAIS = 3
    NAO_SE_APLICA = 4
    NAO_INFORMADO = 5


class AmparoLegal(Enum):
    LEI_14133_ART_28_I = 1
    LEI_14133_ART_28_II = 2
    LEI_14133_ART_28_III = 3
    LEI_14133_ART_28_IV = 4
    LEI_14133_ART_28_V = 5
    LEI_14133_ART_74_I = 6
    LEI_14133_ART_74_II = 7
    LEI_14133_ART_74_III_A = 8
    LEI_14133_ART_74_III_B = 9
    LEI_14133_ART_74_III_C = 10
    LEI_14133_ART_74_III_D = 11
    LEI_14133_ART_74_III_E = 12
    LEI_14133_ART_74_III_F = 13
    LEI_14133_ART_74_III_G = 14
    LEI_14133_ART_74_III_H = 15
    LEI_14133_ART_74_IV = 16
    LEI_14133_ART_74_V = 17
    LEI_14133_ART_75_I = 18
    LEI_14133_ART_75_II = 19
    LEI_14133_ART_75_III_A = 20
    LEI_14133_ART_75_III_B = 21
    LEI_14133_ART_75_IV_A = 22
    LEI_14133_ART_75_IV_B = 23
    LEI_14133_ART_75_IV_C = 24
    LEI_14133_ART_75_IV_D = 25
    LEI_14133_ART_75_IV_E = 26
    LEI_14133_ART_75_IV_F = 27
    LEI_14133_ART_75_IV_G = 28


class CategoriaItemPlanoContratacoes(Enum):
    MATERIAL = 1
    SERVICO = 2
    OBRAS = 3
    SERVICOS_DE_ENGENHARIA = 4
    SOLUCOES_DE_TIC = 5
    LOCACAO_DE_IMOVEIS = 6
    ALIENACAO_CONCESSAO_PERMISSAO = 7
    OBRAS_E_SERVICOS_DE_ENGENHARIA = 8


# Working endpoints (only the reliable ones) - OpenAPI compliant
PNCP_ENDPOINTS = [
    {
        "name": "contratos_publicacao",
        "path": "/v1/contratos",
        "description": "Contratos por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
        "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
    },
    {
        "name": "contratos_atualizacao",
        "path": "/v1/contratos/atualizacao",
        "description": "Contratos por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
        "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
    },
    {
        "name": "atas_periodo",
        "path": "/v1/atas",
        "description": "Atas de Registro de Preço por Período de Vigência",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
        "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
    },
    {
        "name": "atas_atualizacao",
        "path": "/v1/atas/atualizacao",
        "description": "Atas por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,  # API limit, but we use monthly chunks
        "supports_date_range": True,
        "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
    },
    {
        "name": "contratacoes_publicacao",
        "path": "/v1/contratacoes/publicacao",
        "description": "Contratações por Data de Publicação",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,
        "supports_date_range": True,
        "iterate_modalidades": [m.value for m in ModalidadeContratacao],
        "page_size": 50,  # OpenAPI spec: max 50 for contratacoes endpoints
    },
    {
        "name": "contratacoes_atualizacao",
        "path": "/v1/contratacoes/atualizacao",
        "description": "Contratações por Data de Atualização Global",
        "date_params": ["dataInicial", "dataFinal"],
        "max_days": 365,
        "supports_date_range": True,
        "iterate_modalidades": [m.value for m in ModalidadeContratacao],
        "page_size": 50,  # OpenAPI spec: max 50 for contratacoes endpoints
    },
    {
        "name": "pca_atualizacao",
        "path": "/v1/pca/atualizacao",
        "description": "PCA por Data de Atualização Global",
        "date_params": ["dataInicio", "dataFim"],  # PCA uses different parameter names
        "max_days": 365,
        "supports_date_range": True,
        "page_size": 500,  # OpenAPI spec: max 500 for this endpoint
    },
    {
        "name": "instrumentoscobranca_inclusao",
        "path": "/v1/instrumentoscobranca/inclusao",  # Correct path from OpenAPI spec
        "description": "Instrumentos de Cobrança por Data de Inclusão",
        "date_params": ["dataInicial", "dataFinal"],  # Uses date range
        "max_days": 365,
        "supports_date_range": True,  # Date range endpoint
        "page_size": 100,  # OpenAPI spec: max 100, min 10 for this endpoint
        "min_page_size": 10,  # Minimum page size required
    },
    {
        "name": "contratacoes_proposta",
        "path": "/v1/contratacoes/proposta",
        "description": "Contratações com Recebimento de Propostas Aberto",
        "date_params": ["dataFinal"],
        "max_days": 365,
        "supports_date_range": False,
        "requires_single_date": True,  # This endpoint doesn't use date chunking
        "requires_future_date": True,  # This endpoint needs current/future dates
        "future_days_offset": 365,  # Use 1 year in the future to capture most active contracts
        # No iterate_modalidades - captures more data without it
        "page_size": 50,  # OpenAPI spec: max 50 for contratacoes endpoints
    },
    # Note: PCA usuario endpoint requires anoPca and idUsuario parameters
    # This is commented out as it requires specific user/org data to be useful
    # {
    #     "name": "pca_usuario",
    #     "path": "/v1/pca/usuario",
    #     "description": "PCA por Usuário e Ano",
    #     "date_params": [],  # Uses anoPca instead of date ranges
    #     "max_days": 0,
    #     "supports_date_range": False,
    #     "requires_specific_params": True,  # Requires anoPca, idUsuario
    #     "extra_params": {"anoPca": 2024, "idUsuario": "example"},
    #     "page_size": 500,
    # },
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

        # Queue-based processing
        queue_size = max(32, concurrency * 10)
        self.page_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(
            maxsize=queue_size
        )
        self.writer_running = False

        # Graceful shutdown handling
        self.shutdown_event = asyncio.Event()
        self.running_tasks = set()

        self._init_database()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown - Windows compatible."""

        def signal_handler(signum, frame):
            console.print(
                "\n⚠️ [yellow]Received Ctrl+C, initiating graceful shutdown...[/yellow]"
            )
            self.shutdown_event.set()
            # Cancel all running tasks
            for task in self.running_tasks:
                if not task.done():
                    task.cancel()

        # Windows-compatible signal handlers
        try:
            if hasattr(signal, "SIGINT"):
                signal.signal(signal.SIGINT, signal_handler)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, signal_handler)
        except Exception as e:
            logger.warning(f"Could not setup signal handlers: {e}")

    def _init_database(self):
        """Initialize DuckDB with PSA schema."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        # 3. Evite locks fantasmas
        self.db_lock = FileLock(str(BALIZA_DB_PATH) + ".lock")
        try:
            self.db_lock.acquire(timeout=0.5)
        except Timeout:
            raise RuntimeError("Outra instância está usando pncp_new.db")

        self.conn = connect_utf8(str(BALIZA_DB_PATH))

        # Create PSA schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS psa")

        # Create raw responses table with ZSTD compression for response_content
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
        except Exception:
            # Fallback: try to create the index and catch the error
            try:
                self.conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name}_test ON psa.pncp_raw_responses(endpoint_name)"
                )
                self.conn.execute(f"DROP INDEX IF EXISTS {index_name}_test")
                return False  # If we can create a test index, the target doesn't exist
            except Exception:
                return True  # If we can't create test index, assume target exists

    def _create_indexes_if_not_exist(self):
        """Create indexes only if they do not already exist."""
        indexes_to_create = {
            "idx_endpoint_date_page": "CREATE INDEX IF NOT EXISTS idx_endpoint_date_page ON psa.pncp_raw_responses(endpoint_name, data_date, current_page)",
            "idx_response_code": "CREATE INDEX IF NOT EXISTS idx_response_code ON psa.pncp_raw_responses(response_code)",
        }

        for idx_name, create_sql in indexes_to_create.items():
            try:
                self.conn.execute(create_sql)
                logger.info(f"Index '{idx_name}' ensured.")
            except Exception:
                logger.exception(f"Failed to create index '{idx_name}'")

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
            if hasattr(self, "writer_running") and self.writer_running:
                await self.page_queue.put(None)  # Send sentinel

            # Close HTTP client
            if hasattr(self, "client") and self.client:
                await self.client.aclose()

            # Close database connection
            if hasattr(self, "conn") and self.conn:
                try:
                    self.conn.commit()  # Commit any pending changes
                    self.conn.close()
                except (duckdb.Error, AttributeError) as db_err:
                    logger.warning(f"Error during database cleanup: {db_err}")

            # Release database lock
            if hasattr(self, "db_lock") and self.db_lock:
                try:
                    self.db_lock.release()
                except Exception as lock_err:
                    logger.warning(f"Error releasing database lock: {lock_err}")

            console.print(
                "✅ [bold green]Graceful shutdown completed successfully![/bold green]"
            )

        except Exception as e:
            console.print(f"⚠️ Shutdown error: {e}")

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

            except Exception as create_error:
                # If table already exists with ZSTD, clean up
                with contextlib.suppress(Exception):
                    self.conn.execute("DROP TABLE psa.pncp_raw_responses_zstd")

                # This likely means the table already has ZSTD or migration already happened
                if "already exists" in str(create_error):
                    pass  # Expected, migration already done
                else:
                    raise

        except Exception as e:
            console.print(f"⚠️ ZSTD migration error: {e}")
            # Rollback on error
            with contextlib.suppress(Exception):
                self.conn.rollback()

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
            logger.info("HTTP/2 client initialized")

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
            logger.warning("HTTP/2 not available, using HTTP/1.1")

    async def _verify_http2_status(self):
        """Verify that HTTP/2 is actually being used."""
        try:
            # Make a test request to check protocol
            response = await self.client.get("/", timeout=5)

            # Check if HTTP/2 was actually used
            if hasattr(response, "http_version") and response.http_version == "HTTP/2":
                logger.info("HTTP/2 protocol confirmed")
            else:
                protocol = getattr(response, "http_version", "HTTP/1.1")
                logger.warning(
                    "Using protocol: {protocol} (fallback from HTTP/2)",
                    protocol=protocol,
                )

        except Exception:
            logger.exception("HTTP/2 verification failed")

    async def _complete_task_and_print(
        self, progress: Progress, task_id: int, final_message: str
    ):
        """Complete task and print final message, letting it scroll up."""
        # Update to final state
        progress.update(task_id, description=final_message)

        # Small delay to show final state
        await asyncio.sleep(0.5)

        # Print final message to console (will scroll up)
        console.print(f"{final_message}")

        # Remove from progress after printing
        with contextlib.suppress(Exception):
            progress.remove_task(task_id)

    async def _fetch_with_backpressure(
        self, url: str, params: dict[str, Any], task_id: str | None = None
    ) -> dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        async with self.semaphore:
            for attempt in range(3):
                try:
                    self.total_requests += 1
                    response = await self.client.get(url, params=params)

                    # Common success data
                    if response.status_code in [200, 204]:
                        self.successful_requests += 1
                        content_text = response.text
                        data = parse_json_robust(content_text) if content_text else {}
                        return {
                            "success": True,
                            "status_code": response.status_code,
                            "data": data,
                            "headers": dict(response.headers),
                            "total_records": data.get("totalRegistros", 0),
                            "total_pages": data.get("totalPaginas", 1),
                            "content": content_text,
                            "task_id": task_id,  # Pass through task_id
                            "url": url,
                            "params": params,
                        }

                    # Handle failures
                    self.failed_requests += 1
                    if 400 <= response.status_code < 500:
                        # Don't retry client errors
                        return {
                            "success": False,
                            "status_code": response.status_code,
                            "error": f"HTTP {response.status_code}",
                            "content": response.text,
                            "headers": dict(response.headers),
                            "task_id": task_id,
                        }

                    # Retry on 5xx or other transient errors
                    if attempt < 2:
                        delay = (2**attempt) * random.uniform(0.5, 1.5)  # noqa: S311
                        await asyncio.sleep(delay)
                        continue

                    # Final failure after retries
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "error": f"HTTP {response.status_code} after {attempt + 1} attempts",
                        "content": response.text,
                        "headers": dict(response.headers),
                        "task_id": task_id,
                    }

                except Exception as e:
                    if attempt < 2:
                        delay = (2**attempt) * random.uniform(0.5, 1.5)  # noqa: S311
                        await asyncio.sleep(delay)
                        continue

                    self.failed_requests += 1
                    return {
                        "success": False,
                        "status_code": 0,
                        "error": str(e),
                        "content": "",
                        "headers": {},
                        "task_id": task_id,
                    }

    def _format_date(self, date_obj: date) -> str:
        """Format date for PNCP API (YYYYMMDD)."""
        return date_obj.strftime("%Y%m%d")

    def _monthly_chunks(
        self, start_date: date, end_date: date
    ) -> list[tuple[date, date]]:
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
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

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
                if counter % commit_every == 0 and batch_buffer:
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

    async def _plan_tasks(self, start_date: date, end_date: date):
        """Phase 1: Populate the control table with all necessary tasks."""
        console.print("Phase 1: Planning tasks...")
        date_chunks = self._monthly_chunks(start_date, end_date)
        tasks_to_create = []

        for endpoint in PNCP_ENDPOINTS:
            modalidades = endpoint.get(
                "iterate_modalidades", [None]
            )  # None means no modalidade iteration

            for modalidade in modalidades:
                if endpoint.get("requires_single_date", False):
                    # For single-date endpoints, create only one task with the end_date
                    # Special handling for endpoints that need future dates
                    if endpoint.get("requires_future_date", False):
                        # Use a future date for endpoints that need current/future dates
                        from datetime import date, timedelta

                        future_days = endpoint.get("future_days_offset", 365)
                        future_date = date.today() + timedelta(days=future_days)
                        task_suffix = (
                            f"_modalidade_{modalidade}"
                            if modalidade is not None
                            else ""
                        )
                        task_id = (
                            f"{endpoint['name']}_{future_date.isoformat()}{task_suffix}"
                        )
                        tasks_to_create.append(
                            (task_id, endpoint["name"], future_date, modalidade)
                        )
                    else:
                        task_suffix = (
                            f"_modalidade_{modalidade}"
                            if modalidade is not None
                            else ""
                        )
                        task_id = (
                            f"{endpoint['name']}_{end_date.isoformat()}{task_suffix}"
                        )
                        tasks_to_create.append(
                            (task_id, endpoint["name"], end_date, modalidade)
                        )
                else:
                    # For range endpoints, use monthly chunking
                    for chunk_start, _ in date_chunks:
                        task_suffix = (
                            f"_modalidade_{modalidade}"
                            if modalidade is not None
                            else ""
                        )
                        task_id = (
                            f"{endpoint['name']}_{chunk_start.isoformat()}{task_suffix}"
                        )
                        tasks_to_create.append(
                            (task_id, endpoint["name"], chunk_start, modalidade)
                        )

        if tasks_to_create:
            # Schema migration - update constraint to include modalidade
            try:
                # Check if we need to migrate the constraint
                existing_constraints = self.conn.execute(
                    "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'pncp_extraction_tasks' AND constraint_type = 'UNIQUE'"
                ).fetchall()

                # If we have the old constraint, we need to migrate
                for constraint in existing_constraints:
                    if constraint[0] == "unique_task":
                        # Check if constraint includes modalidade
                        constraint_cols = self.conn.execute(
                            "SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name = 'unique_task' AND table_name = 'pncp_extraction_tasks'"
                        ).fetchall()

                        if (
                            len(constraint_cols) == 2
                        ):  # Old constraint (endpoint_name, data_date)
                            # Drop old constraint and recreate with modalidade
                            self.conn.execute(
                                "ALTER TABLE psa.pncp_extraction_tasks DROP CONSTRAINT unique_task"
                            )
                            self.conn.execute(
                                "ALTER TABLE psa.pncp_extraction_tasks ADD CONSTRAINT unique_task UNIQUE (endpoint_name, data_date, modalidade)"
                            )
                            self.conn.commit()
                            break

            except Exception:
                # If migration fails, continue - the new schema will handle it
                pass

            self.conn.executemany(
                """
                INSERT INTO psa.pncp_extraction_tasks (task_id, endpoint_name, data_date, modalidade)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (task_id) DO NOTHING
                """,
                tasks_to_create,
            )
            self.conn.commit()
        console.print(
            f"Planning complete. {len(tasks_to_create)} potential tasks identified."
        )

    async def _discover_tasks(self, progress: Progress):
        """Phase 2: Get metadata for all PENDING tasks."""
        pending_tasks = self.conn.execute(
            "SELECT task_id, endpoint_name, data_date, modalidade FROM psa.pncp_extraction_tasks WHERE status = 'PENDING'"
        ).fetchall()

        if not pending_tasks:
            console.print("Phase 2: Discovery - No pending tasks to discover.")
            return

        discovery_progress = progress.add_task(
            "[cyan]Phase 2: Discovery", total=len(pending_tasks)
        )

        discovery_jobs = []
        for task_id, endpoint_name, data_date, modalidade in pending_tasks:
            # Mark as DISCOVERING
            self.conn.execute(
                "UPDATE psa.pncp_extraction_tasks SET status = 'DISCOVERING', updated_at = now() WHERE task_id = ?",
                [task_id],
            )

            endpoint = next(
                (ep for ep in PNCP_ENDPOINTS if ep["name"] == endpoint_name), None
            )
            if not endpoint:
                continue

            # Respect minimum page size requirements
            page_size = endpoint.get("page_size", PAGE_SIZE)
            min_page_size = endpoint.get("min_page_size", 1)
            actual_page_size = max(page_size, min_page_size)

            params = {
                "tamanhoPagina": actual_page_size,
                "pagina": 1,
            }
            if endpoint["supports_date_range"]:
                params[endpoint["date_params"][0]] = self._format_date(data_date)
                params[endpoint["date_params"][1]] = self._format_date(
                    self._monthly_chunks(data_date, data_date)[0][1]
                )
            elif endpoint.get("requires_single_date", False):
                # For single-date endpoints, use the data_date directly (should be end_date)
                # The data_date should already be correct (future date if needed) from task planning
                params[endpoint["date_params"][0]] = self._format_date(data_date)
            else:
                # For endpoints that don't support date ranges, use end of month chunk
                params[endpoint["date_params"][0]] = self._format_date(
                    self._monthly_chunks(data_date, data_date)[0][1]
                )

            # Add modalidade if this task has one
            if modalidade is not None:
                params["codigoModalidadeContratacao"] = modalidade

            discovery_jobs.append(
                self._fetch_with_backpressure(endpoint["path"], params, task_id=task_id)
            )

        self.conn.commit()  # Commit status change to DISCOVERING

        for future in asyncio.as_completed(discovery_jobs):
            response = await future
            task_id = response.get("task_id")

            if response["success"]:
                total_records = response.get("total_records", 0)
                total_pages = response.get("total_pages", 1)

                # If total_pages is 0, it means no records, so it's 1 page of empty results.
                if total_pages == 0:
                    total_pages = 1

                missing_pages = list(range(2, total_pages + 1))

                self.conn.execute(
                    """
                    UPDATE psa.pncp_extraction_tasks
                    SET status = ?, total_pages = ?, total_records = ?, missing_pages = ?, updated_at = now()
                    WHERE task_id = ?
                    """,
                    [
                        "FETCHING" if missing_pages else "COMPLETE",
                        total_pages,
                        total_records,
                        json.dumps(missing_pages),
                        task_id,
                    ],
                )

                # Enqueue page 1 response
                # Task ID format: {endpoint_name}_{YYYY-MM-DD} or {endpoint_name}_{YYYY-MM-DD}_modalidade_{N}
                # Since endpoint names can contain underscores, we need to extract the date part from the end
                match = re.match(
                    r"^(.+)_(\d{4}-\d{2}-\d{2})(?:_modalidade_\d+)?$", task_id
                )
                if match:
                    endpoint_name_part = match.group(1)
                    data_date_str = match.group(2)
                    data_date = datetime.fromisoformat(data_date_str).date()

                page_1_response = {
                    "endpoint_url": f"{PNCP_BASE_URL}{response['url']}",
                    "endpoint_name": endpoint_name_part,
                    "request_parameters": response["params"],
                    "response_code": response["status_code"],
                    "response_content": response["content"],
                    "response_headers": response["headers"],
                    "data_date": data_date,
                    "run_id": self.run_id,
                    "total_records": total_records,  # This might not be accurate for pages > 1
                    "total_pages": total_pages,  # This might not be accurate for pages > 1
                    "current_page": 1,
                    "page_size": endpoint.get("page_size", PAGE_SIZE),
                }
                await self.page_queue.put(page_1_response)

            else:
                error_message = f"HTTP {response.get('status_code')}: {response.get('error', 'Unknown')}"
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'FAILED', last_error = ?, updated_at = now() WHERE task_id = ?",
                    [error_message, task_id],
                )

            self.conn.commit()
            progress.update(discovery_progress, advance=1)

    async def _fetch_page(
        self,
        endpoint_name: str,
        data_date: date,
        modalidade: int | None,
        page_number: int,
    ):
        """Helper to fetch a single page and enqueue it."""
        endpoint = next(
            (ep for ep in PNCP_ENDPOINTS if ep["name"] == endpoint_name), None
        )
        if not endpoint:
            return

        # Respect minimum page size requirements
        page_size = endpoint.get("page_size", PAGE_SIZE)
        min_page_size = endpoint.get("min_page_size", 1)
        actual_page_size = max(page_size, min_page_size)

        params = {
            "tamanhoPagina": actual_page_size,
            "pagina": page_number,
        }

        if endpoint["supports_date_range"]:
            params[endpoint["date_params"][0]] = self._format_date(data_date)
            params[endpoint["date_params"][1]] = self._format_date(
                self._monthly_chunks(data_date, data_date)[0][1]
            )
        elif endpoint.get("requires_single_date", False):
            # For single-date endpoints, use the data_date directly (should be end_date)
            # The data_date should already be correct (future date if needed) from task planning
            params[endpoint["date_params"][0]] = self._format_date(data_date)
        else:
            # For endpoints that don't support date ranges, use end of month chunk
            params[endpoint["date_params"][0]] = self._format_date(
                self._monthly_chunks(data_date, data_date)[0][1]
            )

        # Add modalidade if this endpoint uses it
        if modalidade is not None:
            params["codigoModalidadeContratacao"] = modalidade

        response = await self._fetch_with_backpressure(endpoint["path"], params)

        # Enqueue the response for the writer worker
        page_response = {
            "endpoint_url": f"{PNCP_BASE_URL}{endpoint['path']}",
            "endpoint_name": endpoint_name,
            "request_parameters": params,
            "response_code": response["status_code"],
            "response_content": response["content"],
            "response_headers": response["headers"],
            "data_date": data_date,
            "run_id": self.run_id,
            "total_records": response.get(
                "total_records", 0
            ),  # This might not be accurate for pages > 1
            "total_pages": response.get(
                "total_pages", 0
            ),  # This might not be accurate for pages > 1
            "current_page": page_number,
            "page_size": endpoint.get("page_size", PAGE_SIZE),
        }
        await self.page_queue.put(page_response)
        return response

    async def _execute_tasks(self, progress: Progress):
        """Phase 3: Fetch all missing pages for FETCHING and PARTIAL tasks."""

        # Use unnest to get a list of all pages to fetch
        pages_to_fetch_query = """
SELECT t.task_id, t.endpoint_name, t.data_date, t.modalidade, CAST(p.page_number AS INTEGER) as page_number
FROM psa.pncp_extraction_tasks t,
     unnest(json_extract(t.missing_pages, '$')::INTEGER[]) AS p(page_number)
WHERE t.status IN ('FETCHING', 'PARTIAL');
        """
        pages_to_fetch = self.conn.execute(pages_to_fetch_query).fetchall()

        if not pages_to_fetch:
            console.print("Phase 3: Execution - No pages to fetch.")
            return

        # Group pages by endpoint only
        endpoint_pages = {}
        for (
            task_id,
            endpoint_name,
            data_date,
            modalidade,
            page_number,
        ) in pages_to_fetch:
            if endpoint_name not in endpoint_pages:
                endpoint_pages[endpoint_name] = []
            endpoint_pages[endpoint_name].append(
                (task_id, data_date, modalidade, page_number)
            )

        # Create progress bars for each endpoint with beautiful colors
        progress_bars = {}
        endpoint_colors = {
            "contratos_publicacao": "green",
            "contratos_atualizacao": "blue",
            "atas_periodo": "cyan",
            "atas_atualizacao": "bright_cyan",
            "contratacoes_publicacao": "yellow",
            "contratacoes_atualizacao": "magenta",
            "pca_atualizacao": "bright_blue",
            "instrumentoscobranca_inclusao": "bright_green",
            "contratacoes_proposta": "bright_yellow",
        }

        console.print("\n🚀 [bold blue]PNCP Data Extraction Progress[/bold blue]\n")

        for endpoint_name, pages in endpoint_pages.items():
            # Get endpoint description and color
            endpoint_desc = next(
                (
                    ep["description"]
                    for ep in PNCP_ENDPOINTS
                    if ep["name"] == endpoint_name
                ),
                endpoint_name,
            )
            color = endpoint_colors.get(endpoint_name, "white")

            # Create beautiful, colorful progress description
            task_description = (
                f"[{color}]{endpoint_desc}[/{color}] - [dim]{len(pages):,} pages[/dim]"
            )
            progress_bars[endpoint_name] = progress.add_task(
                task_description, total=len(pages)
            )

        # Execute all fetches
        fetch_tasks = []
        for endpoint_name, pages in endpoint_pages.items():
            for task_id, data_date, modalidade, page_number in pages:
                # Check for shutdown before creating new tasks
                if self.shutdown_event.is_set():
                    console.print(
                        "⚠️ [yellow]Shutdown requested, stopping task creation...[/yellow]"
                    )
                    break

                task = asyncio.create_task(
                    self._fetch_page_with_progress(
                        endpoint_name,
                        data_date,
                        modalidade,
                        page_number,
                        progress,
                        progress_bars[endpoint_name],
                    )
                )
                fetch_tasks.append(task)
                self.running_tasks.add(task)

        # Wait for all tasks to complete with graceful shutdown support
        try:
            await asyncio.gather(*fetch_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            console.print(
                "⚠️ [yellow]Tasks cancelled during shutdown, cleaning up...[/yellow]"
            )
            raise
        finally:
            # Clean up completed tasks
            for task in fetch_tasks:
                self.running_tasks.discard(task)

        # Print beautiful overall summary
        total_pages = sum(len(pages) for pages in endpoint_pages.values())
        console.print(
            f"\n✅ [bold green]Overall: {total_pages:,} pages completed successfully![/bold green]"
        )
        console.print("")

    async def _fetch_page_with_progress(
        self,
        endpoint_name: str,
        data_date: date,
        modalidade: int | None,
        page_number: int,
        progress: Progress,
        progress_bar_id: int,
    ):
        """Fetch a page and update the progress bar with graceful shutdown support."""
        try:
            # Check for shutdown signal
            if self.shutdown_event.is_set():
                console.print(
                    "⚠️ [yellow]Shutdown requested, skipping page fetch...[/yellow]"
                )
                return

            await self._fetch_page(endpoint_name, data_date, modalidade, page_number)
            progress.update(progress_bar_id, advance=1)
        except asyncio.CancelledError:
            console.print("⚠️ [yellow]Page fetch cancelled during shutdown[/yellow]")
            raise
        except Exception as e:
            logger.error(
                f"Failed to fetch page {page_number} for {endpoint_name} {data_date} modalidade {modalidade}: {e}"
            )
            progress.update(
                progress_bar_id, advance=1
            )  # Still advance to show completion

    async def _reconcile_tasks(self):
        """Phase 4: Update task status based on downloaded data."""
        console.print("Phase 4: Reconciling tasks...")

        tasks_to_reconcile = self.conn.execute(
            "SELECT task_id, endpoint_name, data_date, modalidade, total_pages FROM psa.pncp_extraction_tasks WHERE status IN ('FETCHING', 'PARTIAL')"
        ).fetchall()

        for (
            task_id,
            endpoint_name,
            data_date,
            modalidade,
            total_pages,
        ) in tasks_to_reconcile:
            # Find out which pages were successfully downloaded for this task
            # We need to check the request_parameters for modalidade if it exists
            if modalidade is not None:
                downloaded_pages_result = self.conn.execute(
                    """
                    SELECT DISTINCT current_page
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
                    AND json_extract(request_parameters, '$.codigoModalidadeContratacao') = ?
                    """,
                    [endpoint_name, data_date, modalidade],
                ).fetchall()
            else:
                downloaded_pages_result = self.conn.execute(
                    """
                    SELECT DISTINCT current_page
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
                    AND json_extract(request_parameters, '$.codigoModalidadeContratacao') IS NULL
                    """,
                    [endpoint_name, data_date],
                ).fetchall()

            downloaded_pages = {row[0] for row in downloaded_pages_result}

            # Generate the full set of expected pages
            all_pages = set(range(1, total_pages + 1))

            # Calculate the new set of missing pages
            new_missing_pages = sorted(all_pages - downloaded_pages)

            if not new_missing_pages:
                # All pages are downloaded
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'COMPLETE', missing_pages = '[]', updated_at = now() WHERE task_id = ?",
                    [task_id],
                )
            else:
                # Some pages are still missing
                self.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'PARTIAL', missing_pages = ?, updated_at = now() WHERE task_id = ?",
                    [json.dumps(new_missing_pages), task_id],
                )

        self.conn.commit()
        console.print("Reconciliation complete.")

    async def extract_data(
        self, start_date: date, end_date: date, force: bool = False
    ) -> dict[str, Any]:
        """Main extraction method using a task-based, phased architecture."""
        logger.info(
            f"Extraction started: {start_date.isoformat()} to {end_date.isoformat()}, "
            f"concurrency={self.concurrency}, run_id={self.run_id}, force={force}"
        )
        start_time = time.time()

        if force:
            console.print(
                "[yellow]Force mode enabled - will reset tasks and re-extract all data.[/yellow]"
            )
            self.conn.execute("DELETE FROM psa.pncp_extraction_tasks")
            self.conn.execute("DELETE FROM psa.pncp_raw_responses")
            self.conn.commit()

        # Setup signal handlers now that we're in async context
        self.setup_signal_handlers()

        # Client initialized in __aenter__
        if not hasattr(self, "client") or self.client is None:
            await self._init_client()

        # Start writer worker
        self.writer_running = True
        writer_task = asyncio.create_task(self.writer_worker(commit_every=100))
        self.running_tasks.add(writer_task)

        # --- Main Execution Flow ---

        # Phase 1: Planning
        await self._plan_tasks(start_date, end_date)

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            # Phase 2: Discovery
            await self._discover_tasks(progress)

            # Phase 3: Execution
            await self._execute_tasks(progress)

        # Wait for writer to process all enqueued pages
        try:
            await self.page_queue.join()
            await self.page_queue.put(None)  # Send sentinel
            await writer_task
        except asyncio.CancelledError:
            console.print("⚠️ [yellow]Writer task cancelled during shutdown[/yellow]")
            # Ensure writer task is cancelled
            if not writer_task.done():
                writer_task.cancel()
                try:
                    await writer_task
                except asyncio.CancelledError:
                    pass
        finally:
            self.running_tasks.discard(writer_task)

        # Phase 4: Reconciliation
        await self._reconcile_tasks()

        # --- Final Reporting ---
        duration = time.time() - start_time

        # Fetch final stats from the control table
        total_tasks = self.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks"
        ).fetchone()[0]
        complete_tasks = self.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE'"
        ).fetchone()[0]
        failed_tasks = self.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'FAILED'"
        ).fetchone()[0]

        # Fetch stats from raw responses
        total_records_sum = (
            self.conn.execute(
                "SELECT SUM(total_records) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE'"
            ).fetchone()[0]
            or 0
        )

        total_results = {
            "run_id": self.run_id,
            "start_date": start_date,
            "end_date": end_date,
            "total_tasks": total_tasks,
            "complete_tasks": complete_tasks,
            "failed_tasks": failed_tasks,
            "total_records_extracted": total_records_sum,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "duration": duration,
        }

        console.print("\n🎉 Extraction Complete!")
        console.print(
            f"Total Tasks: {total_tasks:,} ({complete_tasks:,} complete, {failed_tasks:,} failed)"
        )
        console.print(f"Total Records: {total_records_sum:,}")
        console.print(f"Duration: {duration:.1f}s")

        await self.client.aclose()
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
    start_date: str = typer.Option(
        "2021-01-01", help="Start date in YYYY-MM-DD format"
    ),
    end_date: str = typer.Option(
        _get_current_month_end(), help="End date in YYYY-MM-DD format"
    ),
    concurrency: int = typer.Option(CONCURRENCY, help="Number of concurrent requests"),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
):
    """Extract data using true async architecture."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    async def main():
        async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
            results = await extractor.extract_data(start_dt, end_dt, force)

            # Save results
            results_file = (
                DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
            )
            with Path(results_file).open("w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, default=str)

            console.print(f"Results saved to: {results_file}")

    asyncio.run(main())


@app.command()
def stats():
    """Show extraction statistics."""
    conn = connect_utf8(str(BALIZA_DB_PATH))

    # Overall stats
    total_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses"
    ).fetchone()[0]
    success_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200"
    ).fetchone()[0]

    console.print(f"=== Total Responses: {total_responses:,} ===")
    console.print(f"Successful: {success_responses:,}")
    console.print(f"❌ Failed: {total_responses - success_responses:,}")

    if total_responses > 0:
        console.print(f"Success Rate: {success_responses / total_responses * 100:.1f}%")

    # Endpoint breakdown
    endpoint_stats = conn.execute(
        """
        SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM psa.pncp_raw_responses
        WHERE response_code = 200
        GROUP BY endpoint_name
        ORDER BY total_records DESC
    """
    ).fetchall()

    console.print("\n=== Endpoint Statistics ===")
    for name, responses, records in endpoint_stats:
        console.print(f"  {name}: {responses:,} responses, {records:,} records")

    conn.close()


if __name__ == "__main__":
    app()
