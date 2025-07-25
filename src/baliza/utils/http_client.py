"""
HTTP client for PNCP API with Prefect integration, rate limiting and circuit breaker
"""

import json
import hashlib
import zlib
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
import httpx
from prefect import get_run_logger

from ..config import PNCPAPISettings, settings
from ..enums import ModalidadeContratacao, get_enum_by_value
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    AdaptiveRateLimiter,
    retry_with_backoff,
    RetryConfig,
)


from pydantic import ConfigDict


class PNCPResponse(BaseModel):
    """Pydantic model for PNCP API responses"""

    data: List[Dict[str, Any]] = Field(default_factory=list)
    totalRegistros: int = Field(alias="totalRegistros")
    totalPaginas: int = Field(alias="totalPaginas")
    numeroPagina: int = Field(alias="numeroPagina")
    paginasRestantes: int = Field(alias="paginasRestantes")
    empty: bool = Field(default=False)

    model_config = ConfigDict(populate_by_name=True)


class RequestMetadata(BaseModel):
    """Metadata for API request tracking"""

    endpoint: str
    url: str
    method: str = "GET"
    request_id: str
    timestamp: datetime
    modalidade: Optional[ModalidadeContratacao] = None
    date_range: Optional[str] = None


class APIRequest(BaseModel):
    """Model for storing API request data"""

    request_id: str
    ingestion_date: date
    endpoint: str
    http_status: int
    etag: Optional[str] = None
    payload_sha256: str
    payload_size: int
    collected_at: datetime
    payload_compressed: Optional[bytes] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class PNCPClient:
    """HTTP client for PNCP API with resilience patterns"""

    def __init__(self):
        self.pncp_settings = PNCPAPISettings()
        self.base_url = self.pncp_settings.base_url
        self.rate_limiter = AdaptiveRateLimiter(settings.requests_per_minute)
        self.circuit_breaker = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=settings.circuit_breaker_failure_threshold,
                recovery_timeout=settings.circuit_breaker_recovery_timeout,
            )
        )
        self.retry_config = RetryConfig(
            max_attempts=settings.max_retry_attempts,
            backoff_factor=settings.retry_backoff_factor,
            backoff_max=settings.retry_backoff_max,
        )
        self.logger = get_run_logger()

        # HTTP client configuration
        client_config = {
            "timeout": httpx.Timeout(
                connect=10.0,
                read=self.pncp_settings.timeout_seconds,
                write=10.0,
                pool=5.0,
            ),
            "limits": httpx.Limits(
                max_keepalive_connections=20, max_connections=100, keepalive_expiry=60
            ),
            "http2": True,
            "follow_redirects": True,
        }
        self.client = httpx.AsyncClient(**client_config)

    async def fetch_endpoint_data(
        self, endpoint_name: str, url: str, params: Dict[str, Any] = None
    ) -> (APIRequest, bytes):
        """Fetch data from PNCP endpoint with resilience patterns"""

        modalidade_code = params.get("codigoModalidadeContratacao") if params else None
        modalidade = (
            get_enum_by_value(ModalidadeContratacao, modalidade_code)
            if modalidade_code
            else None
        )

        request_metadata = RequestMetadata(
            endpoint=endpoint_name,
            url=url,
            request_id=self._generate_request_id(url),
            timestamp=datetime.now(),
            modalidade=modalidade,
            date_range=f"{params.get('dataInicial')}-{params.get('dataFinal')}"
            if params
            else None,
        )

        self.logger.info(f"Fetching {endpoint_name}: {url}")

        # Apply rate limiting
        await self.rate_limiter.acquire()

        # Use circuit breaker and retry
        api_request, payload_compressed = await retry_with_backoff(
            self._make_request_with_circuit_breaker,
            self.retry_config,
            retryable_exceptions=(httpx.HTTPStatusError, httpx.TimeoutException),
            url=url,
            metadata=request_metadata,
        )

        return api_request, payload_compressed

    async def _make_request_with_circuit_breaker(
        self, url: str, metadata: RequestMetadata
    ) -> (APIRequest, bytes):
        """Make HTTP request with circuit breaker protection"""

        return await self.circuit_breaker.call(self._make_http_request, url, metadata)

    async def _make_http_request(
        self, url: str, metadata: RequestMetadata
    ) -> (APIRequest, bytes):
        """Make actual HTTP request"""

        start_time = datetime.now()

        try:
            response = await self.client.get(url)
            response_time = (datetime.now() - start_time).total_seconds()

            # Adapt rate limiting based on response
            self.rate_limiter.adapt_rate(response_time, response.status_code)

            # Validate response
            if response.status_code not in [200, 204]:
                self.logger.warning(
                    f"HTTP {response.status_code} for {url}: {response.text[:200]}"
                )
                response.raise_for_status()

            # Handle empty response (204 No Content)
            if response.status_code == 204 or not response.content:
                payload_json = {
                    "data": [],
                    "totalRegistros": 0,
                    "totalPaginas": 0,
                    "numeroPagina": 1,
                    "paginasRestantes": 0,
                    "empty": True,
                }
            else:
                payload_json = response.json()

            # Validate with Pydantic
            pncp_response = PNCPResponse(**payload_json)

            # Create API request record
            api_request, payload_compressed = self._create_api_request(
                metadata=metadata,
                response=response,
                payload_json=payload_json,
                pncp_response=pncp_response,
            )

            self.logger.info(
                f"Successfully fetched {len(pncp_response.data)} records "
                f"from {metadata.endpoint} (page {pncp_response.numeroPagina}/"
                f"{pncp_response.totalPaginas})"
            )

            return api_request, payload_compressed

        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error for {url}: {e}")
            raise
        except httpx.TimeoutException as e:
            self.logger.error(f"Timeout for {url}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error for {url}: {e}")
            raise

    def _create_api_request(
        self,
        metadata: RequestMetadata,
        response: httpx.Response,
        payload_json: Dict[str, Any],
        pncp_response: PNCPResponse,
    ) -> (APIRequest, bytes):
        """Create APIRequest model from response data"""

        # Serialize and compress payload
        payload_bytes = json.dumps(
            payload_json, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        payload_compressed = zlib.compress(payload_bytes, level=6)

        # Calculate SHA-256 hash
        payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()

        api_request = APIRequest(
            request_id=metadata.request_id,
            ingestion_date=metadata.timestamp.date(),
            endpoint=metadata.endpoint,
            http_status=response.status_code,
            etag=response.headers.get("etag"),
            payload_sha256=payload_sha256,
            payload_size=len(payload_bytes),
            collected_at=metadata.timestamp,
        )
        return api_request, payload_compressed

    def _generate_request_id(self, url: str) -> str:
        """Generate unique request ID"""
        timestamp = datetime.now().isoformat()
        content = f"{url}:{timestamp}"
        return hashlib.md5(content.encode()).hexdigest()

    async def close(self):
        """Cleanup resources"""
        await self.client.aclose()


class EndpointExtractor:
    """High-level interface for extracting data from PNCP endpoints"""

    def __init__(self, client: PNCPClient = None):
        self.client = client or PNCPClient()
        self.logger = get_run_logger()

    async def extract_contratacoes_publicacao(
        self,
        data_inicial: str,
        data_final: str,
        modalidade: ModalidadeContratacao,
        **filters,
    ) -> List[APIRequest]:
        """Extract contratações by publicação date and modalidade"""

        from .endpoints import build_contratacao_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: contratacoes_publicacao "
            f"({data_inicial} to {data_final}, modalidade {modalidade.name})"
        )

        while True:
            url = build_contratacao_url(
                data_inicial=data_inicial,
                data_final=data_final,
                modalidade=modalidade,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "contratacoes_publicacao", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(
            f"Completed extraction: {len(requests)} pages, modalidade {modalidade.name}"
        )

        return requests

    async def extract_contratos(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract contratos by publication date"""

        from .endpoints import build_contratos_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: contratos ({data_inicial} to {data_final})"
        )

        while True:
            url = build_contratos_url(
                data_inicial=data_inicial,
                data_final=data_final,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "contratos", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_atas(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract atas by vigência period"""

        from .endpoints import build_atas_url

        requests = []
        pagina = 1

        self.logger.info(f"Starting extraction: atas ({data_inicial} to {data_final})")

        while True:
            url = build_atas_url(
                data_inicial=data_inicial,
                data_final=data_final,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "atas", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    # MISSING ENDPOINT IMPLEMENTATIONS FOR 100% COVERAGE

    async def extract_contratacoes_atualizacao(
        self,
        data_inicial: str,
        data_final: str,
        modalidade: ModalidadeContratacao,
        **filters,
    ) -> List[APIRequest]:
        """Extract contratações by atualizacao date and modalidade"""

        from .endpoints import build_contratacoes_atualizacao_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: contratacoes_atualizacao "
            f"({data_inicial} to {data_final}, modalidade {modalidade.name})"
        )

        while True:
            url = build_contratacoes_atualizacao_url(
                data_inicial=data_inicial,
                data_final=data_final,
                modalidade=modalidade,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "contratacoes_atualizacao", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(
            f"Completed extraction: {len(requests)} pages, modalidade {modalidade.name}"
        )

        return requests

    async def extract_contratacoes_proposta(
        self,
        data_final: str,
        modalidade: Optional[ModalidadeContratacao] = None,
        **filters,
    ) -> List[APIRequest]:
        """Extract contratações with open proposal period"""

        from .endpoints import build_contratacoes_proposta_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: contratacoes_proposta (up to {data_final})"
        )

        while True:
            url = build_contratacoes_proposta_url(
                data_final=data_final,
                pagina=pagina,
                modalidade=modalidade,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "contratacoes_proposta", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_contratos_atualizacao(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract contratos by update date"""

        from .endpoints import build_contratos_atualizacao_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: contratos_atualizacao ({data_inicial} to {data_final})"
        )

        while True:
            url = build_contratos_atualizacao_url(
                data_inicial=data_inicial,
                data_final=data_final,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "contratos_atualizacao", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_atas_atualizacao(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract atas by update date"""

        from .endpoints import build_atas_atualizacao_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: atas_atualizacao ({data_inicial} to {data_final})"
        )

        while True:
            url = build_atas_atualizacao_url(
                data_inicial=data_inicial,
                data_final=data_final,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "atas_atualizacao", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_instrumentos_cobranca(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract instrumentos de cobrança by inclusion date"""

        from .endpoints import build_instrumentos_cobranca_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: instrumentos_cobranca ({data_inicial} to {data_final})"
        )

        while True:
            url = build_instrumentos_cobranca_url(
                data_inicial=data_inicial,
                data_final=data_final,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "instrumentoscobranca_inclusao", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_pca(
        self, ano_pca: int, codigo_classificacao: str, **filters
    ) -> List[APIRequest]:
        """Extract PCA (Plano de Contratações Anuais) data"""

        from .endpoints import build_pca_url

        requests = []
        pagina = 1

        self.logger.info(f"Starting extraction: pca (year {ano_pca})")

        while True:
            url = build_pca_url(
                ano_pca=ano_pca,
                codigo_classificacao=codigo_classificacao,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "pca", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_pca_usuario(
        self, ano_pca: int, id_usuario: int, **filters
    ) -> List[APIRequest]:
        """Extract PCA data by user"""

        from .endpoints import build_pca_usuario_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: pca_usuario (year {ano_pca}, user {id_usuario})"
        )

        while True:
            url = build_pca_usuario_url(
                ano_pca=ano_pca,
                id_usuario=id_usuario,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "pca_usuario", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def extract_pca_atualizacao(
        self, data_inicio: str, data_fim: str, **filters
    ) -> List[APIRequest]:
        """Extract PCA data by update date"""

        from .endpoints import build_pca_atualizacao_url

        requests = []
        pagina = 1

        self.logger.info(
            f"Starting extraction: pca_atualizacao ({data_inicio} to {data_fim})"
        )

        while True:
            url = build_pca_atualizacao_url(
                data_inicio=data_inicio,
                data_fim=data_fim,
                pagina=pagina,
                **filters,
            )

            api_request, payload_compressed = await self.client.fetch_endpoint_data(
                "pca_atualizacao", url
            )
            api_request.payload_compressed = payload_compressed
            requests.append(api_request)

            # Parse response to check pagination
            payload_json = json.loads(
                zlib.decompress(api_request.payload_compressed).decode("utf-8")
            )
            pncp_response = PNCPResponse(**payload_json)

            if pncp_response.paginasRestantes == 0 or pncp_response.empty:
                break

            pagina += 1

        self.logger.info(f"Completed extraction: {len(requests)} pages")
        return requests

    async def close(self):
        """Cleanup resources"""
        await self.client.close()
