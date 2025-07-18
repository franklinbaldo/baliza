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
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

# 1. Trave o runtime em UTF-8 - reconfigure streams logo no início
for std in (sys.stdin, sys.stdout, sys.stderr):
    try:
        std.reconfigure(encoding="utf-8", errors="surrogateescape")
    except AttributeError:
        # This can happen in test environments where std streams are mocked
        pass

# Configure standard logging com UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
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
        "iterate_modalidades": [m.value for m in ModalidadeContratacao],
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


from pybloom_live import ScalableBloomFilter

class AsyncPNCPExtractor:
    """True async PNCP extractor with semaphore back-pressure."""

    def __init__(self, concurrency: int = CONCURRENCY, output_dir: str = "data"):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.run_id = str(uuid.uuid4())
        self.client = None
        self.output_dir = Path(output_dir)
        self.request_log_dir = self.output_dir / "request_log"
        self.content_dir = self.output_dir / "response_content"
        self.bloom_filter_file = self.output_dir / "seen_content.bloom"

        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.request_log_dir.mkdir(exist_ok=True)
        self.content_dir.mkdir(exist_ok=True)

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

        self._init_bloom_filter()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown - Windows compatible."""
        def signal_handler(signum, frame):
            console.print(f"\n⚠️ [yellow]Received Ctrl+C, initiating graceful shutdown...[/yellow]")
            self.shutdown_event.set()
            # Cancel all running tasks
            for task in self.running_tasks:
                if not task.done():
                    task.cancel()
        
        # Windows-compatible signal handlers
        try:
            if hasattr(signal, 'SIGINT'):
                signal.signal(signal.SIGINT, signal_handler)
            if hasattr(signal, 'SIGTERM'):
                signal.signal(signal.SIGTERM, signal_handler)
        except Exception as e:
            logger.warning(f"Could not setup signal handlers: {e}")

    def _init_bloom_filter(self, capacity=1000000, error_rate=0.001):
        """Initialize the Bloom Filter, loading from file if it exists."""
        if self.bloom_filter_file.exists():
            with open(self.bloom_filter_file, "rb") as f:
                self.seen_content_filter = ScalableBloomFilter.fromfile(f)
            console.print(f"Bloom filter loaded from {self.bloom_filter_file}")
        else:
            self.seen_content_filter = ScalableBloomFilter(
                initial_capacity=capacity, error_rate=error_rate
            )
            console.print("New Bloom filter created.")

    def _save_bloom_filter(self):
        """Save the Bloom Filter state to a file."""
        with open(self.bloom_filter_file, "wb") as f:
            self.seen_content_filter.tofile(f)
        console.print(f"Bloom filter saved to {self.bloom_filter_file}")

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

            # Save the bloom filter state
            self._save_bloom_filter()

            console.print("✅ [bold green]Graceful shutdown completed successfully![/bold green]")

        except Exception as e:
            console.print(f"⚠️ Shutdown error: {e}")

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

        except Exception as e:
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

    async def writer_worker(self, batch_size: int = 100) -> None:
        """
        A dedicated writer coroutine that consumes responses from a queue and
        writes them to Parquet files in batches.
        """
        import pandas as pd

        self.writer_running = True
        request_log_buffer = []
        content_buffer = []

        while True:
            try:
                page = await self.page_queue.get()
                if page is None:
                    if request_log_buffer:
                        self._write_batch_to_parquet(request_log_buffer, self.request_log_dir, "request_log")
                    if content_buffer:
                        self._write_batch_to_parquet(content_buffer, self.content_dir, "content")
                    break

                content_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, page["response_content"])

                request_log_buffer.append({
                    "id": str(uuid.uuid4()),
                    "extracted_at": datetime.now(),
                    "endpoint_name": page["endpoint_name"],
                    "data_date": page["data_date"],
                    "run_id": self.run_id,
                    "current_page": page["current_page"],
                    "response_content_uuid": str(content_uuid),
                })

                if str(content_uuid) not in self.seen_content_filter:
                    self.seen_content_filter.add(str(content_uuid))
                    content_buffer.append({
                        "response_content_uuid": str(content_uuid),
                        "response_content": page["response_content"],
                    })

                if len(request_log_buffer) >= batch_size:
                    self._write_batch_to_parquet(request_log_buffer, self.request_log_dir, "request_log")
                    request_log_buffer.clear()

                if len(content_buffer) >= batch_size:
                    self._write_batch_to_parquet(content_buffer, self.content_dir, "content")
                    content_buffer.clear()

                self.page_queue.task_done()

            except Exception as e:
                console.print(f"❌ Writer error: {e}")
                self.page_queue.task_done()
                break
        
        self.writer_running = False

    def _write_batch_to_parquet(self, buffer: list[dict], directory: Path, file_prefix: str):
        """Writes a list of dictionaries to a Parquet file."""
        import pandas as pd

        if not buffer:
            return

        df = pd.DataFrame(buffer)
        
        # Create a unique filename for each batch
        batch_filename = f"{file_prefix}_{self.run_id}_{time.time()}.parquet"
        output_file = directory / batch_filename
        
        df.to_parquet(output_file, engine="pyarrow", index=False)

    async def extract(self, start_date: date, end_date: date, force: bool = False):
        """
        Main extraction method.
        - Iterates through endpoints and date ranges.
        - Fetches data asynchronously.
        - Uses a writer coroutine to save data to Parquet files.
        """
        logger.info(
            f"Extraction started: {start_date.isoformat()} to {end_date.isoformat()}, "
            f"concurrency={self.concurrency}, run_id={self.run_id}, force={force}"
        )
        start_time = time.time()

        if force:
            console.print("[yellow]Force mode enabled - will reset Bloom filter and re-extract all data.[/yellow]")
            if self.bloom_filter_file.exists():
                self.bloom_filter_file.unlink()
            self._init_bloom_filter()

        # Setup signal handlers
        self.setup_signal_handlers()

        # Start writer worker
        writer_task = asyncio.create_task(self.writer_worker())
        self.running_tasks.add(writer_task)

        # Create tasks for all endpoints and date ranges
        tasks = []
        date_chunks = self._monthly_chunks(start_date, end_date)

        for endpoint in PNCP_ENDPOINTS:
            for start, end in date_chunks:
                task = asyncio.create_task(
                    self._fetch_endpoint_for_date_range(endpoint, start, end)
                )
                tasks.append(task)
                self.running_tasks.add(task)

        # Wait for all extraction tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)

        # Signal writer to finish
        await self.page_queue.join()
        await self.page_queue.put(None)
        await writer_task
        self.running_tasks.discard(writer_task)

        duration = time.time() - start_time
        console.print(f"\n🎉 Extraction Complete! Duration: {duration:.1f}s")

    async def _fetch_endpoint_for_date_range(self, endpoint: dict, start_date: date, end_date: date):
        """Fetches all pages for a given endpoint and date range."""
        # Initial request to get total pages
        params = {
            "tamanhoPagina": endpoint.get("page_size", PAGE_SIZE),
            "pagina": 1,
        }
        if endpoint["supports_date_range"]:
            params[endpoint["date_params"][0]] = self._format_date(start_date)
            params[endpoint["date_params"][1]] = self._format_date(end_date)
        else:
            # For non-range endpoints, just use the end date
            params[endpoint["date_params"][0]] = self._format_date(end_date)

        initial_response = await self._fetch_with_backpressure(endpoint["path"], params)

        if not initial_response["success"]:
            return

        await self.page_queue.put({
            "endpoint_name": endpoint["name"],
            "data_date": start_date,
            "current_page": 1,
            "response_content": initial_response["content"],
        })

        total_pages = initial_response.get("total_pages", 1)
        if total_pages > 1:
            page_tasks = []
            for page_num in range(2, total_pages + 1):
                task = asyncio.create_task(
                    self._fetch_page(endpoint, start_date, end_date, page_num)
                )
                page_tasks.append(task)
            await asyncio.gather(*page_tasks, return_exceptions=True)

    async def _fetch_page(self, endpoint: dict, start_date: date, end_date: date, page_num: int):
        """Fetches a single page and puts it on the queue."""
        params = {
            "tamanhoPagina": endpoint.get("page_size", PAGE_SIZE),
            "pagina": page_num,
        }
        if endpoint["supports_date_range"]:
            params[endpoint["date_params"][0]] = self._format_date(start_date)
            params[endpoint["date_params"][1]] = self._format_date(end_date)
        else:
            params[endpoint["date_params"][0]] = self._format_date(end_date)

        response = await self._fetch_with_backpressure(endpoint["path"], params)

        if response["success"]:
            await self.page_queue.put({
                "endpoint_name": endpoint["name"],
                "data_date": start_date,
                "current_page": page_num,
                "response_content": response["content"],
            })

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
    start_date: str = typer.Option("2021-01-01", help="Start date in YYYY-MM-DD format"),
    end_date: str = typer.Option(
        _get_current_month_end(), help="End date in YYYY-MM-DD format"
    ),
    concurrency: int = typer.Option(CONCURRENCY, help="Number of concurrent requests"),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
    output_dir: str = typer.Option("data", help="Directory to save the output files"),
):
    """Extract data using true async architecture."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    async def main():
        async with AsyncPNCPExtractor(concurrency=concurrency, output_dir=output_dir) as extractor:
            await extractor.extract(start_dt, end_dt, force)

    asyncio.run(main())


if __name__ == "__main__":
    app()
