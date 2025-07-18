PNCP Data Extractor V2 - True Async Architecture
Based on steel-man pseudocode: endpoint 
365-day ranges 
async pagination
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
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from baliza.pncp_client import PNCPClient
from baliza.pncp_task_planner import PNCPTaskPlanner
from baliza.pncp_writer import PNCPWriter, connect_utf8, BALIZA_DB_PATH, DATA_DIR

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
    except orjson.JSONDecodeError as e:
        logger.warning(f"orjson failed to parse JSON, falling back to standard json: {e}")
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON with both orjson and standard json: {e}")
            raise


# Configuration
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta"
CONCURRENCY = 8  # Concurrent requests limit
PAGE_SIZE = 500  # Maximum page size
REQUEST_TIMEOUT = 30
USER_AGENT = "BALIZA/3.0 (Backup Aberto de Licitacoes)"


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
        "future_days_offset": 1825,  # Use 5 years in the future to capture most active contracts
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
        self.client = PNCPClient(concurrency=concurrency)
        self.task_planner = PNCPTaskPlanner() # Instantiate the task planner
        self.writer = PNCPWriter() # Instantiate the writer
        self.run_id = str(uuid.uuid4())

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
        except (ValueError, AttributeError) as e:
            logger.warning(f"Could not setup signal handlers: {e}")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.client.__aenter__()
        await self.writer.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        await self.writer.__aexit__(exc_type, exc_val, exc_tb)
        await self._graceful_shutdown()

    async def _graceful_shutdown(self):
        """Graceful shutdown of all connections and resources."""
        try:
            # Signal writer to stop gracefully
            if hasattr(self, "writer_running") and self.writer_running:
                await self.page_queue.put(None)  # Send sentinel

            console.print(
                "✅ [bold green]Graceful shutdown completed successfully![/bold green]"
            )

        except Exception as e:
            console.print(f"⚠️ Shutdown error: {e}")

    async def _fetch_with_backpressure(
        self, url: str, params: dict[str, Any], task_id: str | None = None
    ) -> dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        self.total_requests += 1
        response = await self.client.fetch_with_backpressure(url, params, task_id)

        if response["success"]:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        return response

    async def _plan_tasks(self, start_date: date, end_date: date):
        """Phase 1: Populate the control table with all necessary tasks."""
        console.print("Phase 1: Planning tasks...")
        tasks_to_create = await self.task_planner.plan_tasks(start_date, end_date)

        if tasks_to_create:
            # Schema migration - update constraint to include modalidade
            try:
                # Check if we need to migrate the constraint
                existing_constraints = self.writer.conn.execute(
                    "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'pncp_extraction_tasks' AND constraint_type = 'UNIQUE'"
                ).fetchall()

                # If we have the old constraint, we need to migrate
                for constraint in existing_constraints:
                    if constraint[0] == "unique_task":
                        # Check if constraint includes modalidade
                        constraint_cols = self.writer.conn.execute(
                            "SELECT column_name FROM information_schema.key_column_usage WHERE constraint_name = 'unique_task' AND table_name = 'pncp_extraction_tasks'"
                        ).fetchall()

                        if (
                            len(constraint_cols) == 2
                        ):  # Old constraint (endpoint_name, data_date)
                            # Drop old constraint and recreate with modalidade
                            self.writer.conn.execute(
                                "ALTER TABLE psa.pncp_extraction_tasks DROP CONSTRAINT unique_task"
                            )
                            self.writer.conn.execute(
                                "ALTER TABLE psa.pncp_extraction_tasks ADD CONSTRAINT unique_task UNIQUE (endpoint_name, data_date, modalidade)"
                            )
                            self.writer.conn.commit()
                            break

            except duckdb.Error:
                # If migration fails, continue - the new schema will handle it
                pass

            self.writer.conn.executemany(
                """
                INSERT INTO psa.pncp_extraction_tasks (task_id, endpoint_name, data_date, modalidade)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (task_id) DO NOTHING
                """,
                tasks_to_create,
            )
            self.writer.conn.commit()
        console.print(
            f"Planning complete. {len(tasks_to_create)} potential tasks identified."
        )

    async def _discover_tasks(self, progress: Progress):
        """Phase 2: Get metadata for all PENDING tasks."""
        pending_tasks = self.writer.conn.execute(
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
            self.writer.conn.execute(
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
                params[endpoint["date_params"][0]] = self.task_planner._format_date(data_date)
                params[endpoint["date_params"][1]] = self.task_planner._format_date(
                    self.task_planner._monthly_chunks(data_date, data_date)[0][1]
                )
            elif endpoint.get("requires_single_date", False):
                # For single-date endpoints, use the data_date directly (should be end_date)
                # The data_date should already be correct (future date if needed) from task planning
                params[endpoint["date_params"][0]] = self.task_planner._format_date(data_date)
            else:
                # For endpoints that don't support date ranges, use end of month chunk
                params[endpoint["date_params"][0]] = self.task_planner._format_date(
                    self.task_planner._monthly_chunks(data_date, data_date)[0][1]
                )

            # Add modalidade if this task has one
            if modalidade is not None:
                params["codigoModalidadeContratacao"] = modalidade

            discovery_jobs.append(
                self._fetch_with_backpressure(endpoint["path"], params, task_id=task_id)
            )

        self.writer.conn.commit()  # Commit status change to DISCOVERING

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

                self.writer.conn.execute(
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
                self.writer.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'FAILED', last_error = ?, updated_at = now() WHERE task_id = ?",
                    [error_message, task_id],
                )

            self.writer.conn.commit()
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
            params[endpoint["date_params"][0]] = self.task_planner._format_date(data_date)
            params[endpoint["date_params"][1]] = self.task_planner._format_date(
                self.task_planner._monthly_chunks(data_date, data_date)[0][1]
            )
        elif endpoint.get("requires_single_date", False):
            # For single-date endpoints, use the data_date directly (should be end_date)
            # The data_date should already be correct (future date if needed) from task planning
            params[endpoint["date_params"][0]] = self.task_planner._format_date(data_date)
        else:
            # For endpoints that don't support date ranges, use end of month chunk
            params[endpoint["date_params"][0]] = self.task_planner._format_date(
                self.task_planner._monthly_chunks(data_date, data_date)[0][1]
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
        pages_to_fetch = self.writer.conn.execute(pages_to_fetch_query).fetchall()

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

        tasks_to_reconcile = self.writer.conn.execute(
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
                downloaded_pages_result = self.writer.conn.execute(
                    """
                    SELECT DISTINCT current_page
                    FROM psa.pncp_raw_responses
                    WHERE endpoint_name = ? AND data_date = ? AND response_code = 200
                    AND json_extract(request_parameters, '$.codigoModalidadeContratacao') = ?
                    """,
                    [endpoint_name, data_date, modalidade],
                ).fetchall()
            else:
                downloaded_pages_result = self.writer.conn.execute(
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
                self.writer.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'COMPLETE', missing_pages = '[]', updated_at = now() WHERE task_id = ?",
                    [task_id],
                )
            else:
                # Some pages are still missing
                self.writer.conn.execute(
                    "UPDATE psa.pncp_extraction_tasks SET status = 'PARTIAL', missing_pages = ?, updated_at = now() WHERE task_id = ?",
                    [json.dumps(new_missing_pages), task_id],
                )

        self.writer.conn.commit()
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
            self.writer.conn.execute("DELETE FROM psa.pncp_extraction_tasks")
            self.writer.conn.execute("DELETE FROM psa.pncp_raw_responses")
            self.writer.conn.commit()

        # Setup signal handlers now that we're in async context
        self.setup_signal_handlers()

        # Start writer worker
        self.writer_running = True
        writer_task = asyncio.create_task(self.writer.writer_worker(self.page_queue, commit_every=100))
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
            refresh_per_second=10,
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
        total_tasks = self.writer.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks"
        ).fetchone()[0]
        complete_tasks = self.writer.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE'"
        ).fetchone()[0]
        failed_tasks = self.writer.conn.execute(
            "SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'FAILED'"
        ).fetchone()[0]

        # Fetch stats from raw responses
        total_records_sum = (
            self.writer.conn.execute(
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

        return total_results


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
    # 1. Trave o runtime em UTF-8 - reconfigure streams logo no início
    for std in (sys.stdin, sys.stdout, sys.stderr):
        std.reconfigure(encoding="utf-8", errors="surrogateescape")
    app()