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
        self, endpoint_name: str, url: str, params: Optional[Dict[str, Any]] = None
    ) -> tuple[APIRequest, bytes]:
        """Fetch data from PNCP endpoint with resilience patterns and URL-based deduplication"""

        # Check if we already have this URL with a successful response
        existing_request = await self._check_existing_request(url)
        if existing_request:
            self.logger.info(f"Found existing successful request for {url}, skipping HTTP call")
            return existing_request

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

    async def _check_existing_request(self, url: str) -> Optional[tuple[APIRequest, bytes]]:
        """Check if we already have a successful request for this URL"""
        try:
            from ..backend import connect
            
            con = connect()
            
            # Use Ibis to query for existing successful requests for this URL
            api_requests = con.table("raw.api_requests")
            hot_payloads = con.table("raw.hot_payloads")
            
            # Join and filter using Ibis
            query = (
                api_requests
                .join(hot_payloads, api_requests.payload_sha256 == hot_payloads.payload_sha256)
                .filter(
                    (api_requests.endpoint == url) & 
                    (api_requests.http_status >= 200) & 
                    (api_requests.http_status <= 299)
                )
                .order_by(api_requests.collected_at.desc())
                .limit(1)
                .select([
                    api_requests.request_id,
                    api_requests.ingestion_date,
                    api_requests.endpoint,
                    api_requests.http_status,
                    api_requests.etag,
                    api_requests.payload_sha256,
                    api_requests.payload_size,
                    api_requests.collected_at,
                    hot_payloads.payload_compressed
                ])
            )
            
            result_df = query.execute()
            
            if len(result_df) == 0:
                return None
                
            row = result_df.iloc[0]
            
            # Reconstruct APIRequest object
            api_request = APIRequest(
                request_id=str(row['request_id']),
                ingestion_date=row['ingestion_date'].date() if hasattr(row['ingestion_date'], 'date') else row['ingestion_date'],
                endpoint=row['endpoint'],
                http_status=row['http_status'],
                etag=row['etag'],
                payload_sha256=row['payload_sha256'],
                payload_size=row['payload_size'],
                collected_at=row['collected_at'],
                payload_compressed=row['payload_compressed']
            )
            
            return api_request, row['payload_compressed']
            
        except Exception as e:
            # If there's any error checking existing requests, just continue with new request
            self.logger.debug(f"Could not check existing requests for {url}: {e}")
            return None

    async def _make_request_with_circuit_breaker(
        self, url: str, metadata: RequestMetadata
    ) -> tuple[APIRequest, bytes]:
        """Make HTTP request with circuit breaker protection"""

        return await self.circuit_breaker.call(self._make_http_request, url, metadata)

    async def _make_http_request(
        self, url: str, metadata: RequestMetadata
    ) -> tuple[APIRequest, bytes]:
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
    ) -> tuple[APIRequest, bytes]:
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

    def __init__(self, client: Optional[PNCPClient] = None):
        self.client = client or PNCPClient()
        self.logger = get_run_logger()

    async def extract_contratacoes_publicacao(
        self,
        data_inicial: str,
        data_final: str,
        modalidade: ModalidadeContratacao,
        **filters,
    ) -> List[APIRequest]:
        """Extract contratações by publicação date and modalidade with pagination optimization"""

        from .endpoints import build_contratacao_url

        requests = []

        self.logger.info(
            f"Starting extraction: contratacoes_publicacao "
            f"({data_inicial} to {data_final}, modalidade {modalidade.name})"
        )

        # Analyze existing pagination data for this endpoint/date/modalidade combination
        pagination_info = await self._analyze_existing_pagination(
            endpoint_pattern="contratacoes_publicacao",
            data_inicial=data_inicial,
            data_final=data_final,
            modalidade=modalidade,
            **filters
        )

        if pagination_info["has_complete_extraction"]:
            self.logger.info(
                f"Found complete extraction for {modalidade.name} "
                f"({pagination_info['total_pages']} pages), returning cached data"
            )
            return pagination_info["existing_requests"]

        # Determine which pages to fetch
        pages_to_fetch = pagination_info["missing_pages"]
        if not pages_to_fetch:
            # If we don't know pagination yet, start with page 1
            pages_to_fetch = [1]

        # If we have partial data, we know total pages; otherwise discover by fetching page 1
        total_pages_known = pagination_info["total_pages"] if pagination_info["total_pages"] > 0 else None
        
        for pagina in pages_to_fetch:
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

            # Parse response to discover total pages if not known
            if total_pages_known is None:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                pncp_response = PNCPResponse(**payload_json)
                total_pages_known = pncp_response.totalPaginas
                
                # Now we know total pages, add remaining missing pages
                if total_pages_known > 1:
                    existing_pages = set(pagination_info["existing_pages"])
                    all_pages = set(range(1, total_pages_known + 1))
                    remaining_missing = all_pages - existing_pages - {pagina}  # Exclude current page
                    pages_to_fetch.extend(sorted(remaining_missing))

        # Add any existing requests to the result
        requests.extend(pagination_info["existing_requests"])

        self.logger.info(
            f"Completed extraction: {len(requests)} total pages for {modalidade.name} "
            f"({len([r for r in requests if r not in pagination_info['existing_requests']])} new pages fetched)"
        )

        return requests

    async def _analyze_existing_pagination(
        self, 
        endpoint_pattern: str, 
        data_inicial: str, 
        data_final: str, 
        modalidade: Optional[ModalidadeContratacao] = None,
        **filters
    ) -> Dict[str, Any]:
        """Analyze existing pagination data for an endpoint/date/modalidade combination"""
        
        try:
            from ..backend import connect
            from .endpoints import build_contratacao_url, build_contratos_url, build_atas_url
            import re
            
            con = connect()
            api_requests = con.table("raw.api_requests")
            hot_payloads = con.table("raw.hot_payloads")
            
            # Build URL pattern to match this extraction
            if endpoint_pattern == "contratacoes_publicacao" and modalidade:
                # Build sample URL to extract pattern
                sample_url = build_contratacao_url(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    modalidade=modalidade,
                    pagina=1,
                    **filters
                )
                # Create pattern that matches any page number for this combination
                url_pattern = re.sub(r'pagina=\d+', 'pagina=', sample_url)
            elif endpoint_pattern == "contratos":
                sample_url = build_contratos_url(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    pagina=1,
                    **filters
                )
                url_pattern = re.sub(r'pagina=\d+', 'pagina=', sample_url)
            elif endpoint_pattern == "atas":
                sample_url = build_atas_url(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    pagina=1,
                    **filters
                )
                url_pattern = re.sub(r'pagina=\d+', 'pagina=', sample_url)
            else:
                # Fallback - no optimization
                return {
                    "has_complete_extraction": False,
                    "total_pages": 0,
                    "existing_pages": [],
                    "missing_pages": [],
                    "existing_requests": []
                }
            
            # Find existing requests that match this pattern
            query = (
                api_requests
                .join(hot_payloads, api_requests.payload_sha256 == hot_payloads.payload_sha256)
                .filter(
                    (api_requests.http_status >= 200) & 
                    (api_requests.http_status <= 299) &
                    api_requests.endpoint.like(f"{url_pattern}%")
                )
                .select([
                    api_requests.request_id,
                    api_requests.ingestion_date,
                    api_requests.endpoint,
                    api_requests.http_status,
                    api_requests.etag,
                    api_requests.payload_sha256,
                    api_requests.payload_size,
                    api_requests.collected_at,
                    hot_payloads.payload_compressed
                ])
            )
            
            existing_df = query.execute()
            
            if len(existing_df) == 0:
                return {
                    "has_complete_extraction": False,
                    "total_pages": 0,
                    "existing_pages": [],
                    "missing_pages": [],
                    "existing_requests": []
                }
            
            # Extract page numbers from URLs and analyze pagination
            existing_pages = []
            existing_requests = []
            total_pages = 0
            
            for _, row in existing_df.iterrows():
                # Extract page number from URL
                page_match = re.search(r'pagina=(\d+)', row['endpoint'])
                if page_match:
                    page_num = int(page_match.group(1))
                    existing_pages.append(page_num)
                
                # Create APIRequest object
                api_request = APIRequest(
                    request_id=str(row['request_id']),
                    ingestion_date=row['ingestion_date'].date() if hasattr(row['ingestion_date'], 'date') else row['ingestion_date'],
                    endpoint=row['endpoint'],
                    http_status=row['http_status'],
                    etag=row['etag'],
                    payload_sha256=row['payload_sha256'],
                    payload_size=row['payload_size'],
                    collected_at=row['collected_at'],
                    payload_compressed=row['payload_compressed']
                )
                existing_requests.append(api_request)
                
                # Try to determine total pages from any existing payload
                try:
                    payload_json = json.loads(
                        zlib.decompress(row['payload_compressed']).decode("utf-8")
                    )
                    pncp_response = PNCPResponse(**payload_json)
                    if pncp_response.totalPaginas > total_pages:
                        total_pages = pncp_response.totalPaginas
                except Exception:
                    pass
            
            existing_pages = sorted(set(existing_pages))
            
            # Determine missing pages
            missing_pages = []
            has_complete_extraction = False
            
            if total_pages > 0:
                all_pages = set(range(1, total_pages + 1))
                existing_pages_set = set(existing_pages)
                missing_pages = sorted(all_pages - existing_pages_set)
                has_complete_extraction = len(missing_pages) == 0
            
            self.logger.info(
                f"Pagination analysis for {endpoint_pattern}: "
                f"total_pages={total_pages}, existing_pages={len(existing_pages)}, "
                f"missing_pages={len(missing_pages)}, complete={has_complete_extraction}"
            )
            
            return {
                "has_complete_extraction": has_complete_extraction,
                "total_pages": total_pages,
                "existing_pages": existing_pages,
                "missing_pages": missing_pages,
                "existing_requests": existing_requests
            }
            
        except Exception as e:
            self.logger.debug(f"Could not analyze pagination for {endpoint_pattern}: {e}")
            # Fallback - no optimization
            return {
                "has_complete_extraction": False,
                "total_pages": 0,
                "existing_pages": [],
                "missing_pages": [],
                "existing_requests": []
            }

    async def extract_contratos(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract contratos by publication date with pagination optimization"""

        from .endpoints import build_contratos_url

        requests = []

        self.logger.info(
            f"Starting extraction: contratos ({data_inicial} to {data_final})"
        )

        # Analyze existing pagination data
        pagination_info = await self._analyze_existing_pagination(
            endpoint_pattern="contratos",
            data_inicial=data_inicial,
            data_final=data_final,
            **filters
        )

        if pagination_info["has_complete_extraction"]:
            self.logger.info(
                f"Found complete extraction for contratos "
                f"({pagination_info['total_pages']} pages), returning cached data"
            )
            return pagination_info["existing_requests"]

        # Determine which pages to fetch
        pages_to_fetch = pagination_info["missing_pages"]
        if not pages_to_fetch:
            pages_to_fetch = [1]

        # If we have partial data, we know total pages; otherwise discover by fetching page 1
        total_pages_known = pagination_info["total_pages"] if pagination_info["total_pages"] > 0 else None
        
        for pagina in pages_to_fetch:
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

            # Parse response to discover total pages if not known
            if total_pages_known is None:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                pncp_response = PNCPResponse(**payload_json)
                total_pages_known = pncp_response.totalPaginas
                
                # Now we know total pages, add remaining missing pages
                if total_pages_known > 1:
                    existing_pages = set(pagination_info["existing_pages"])
                    all_pages = set(range(1, total_pages_known + 1))
                    remaining_missing = all_pages - existing_pages - {pagina}
                    pages_to_fetch.extend(sorted(remaining_missing))

        # Add any existing requests to the result
        requests.extend(pagination_info["existing_requests"])

        self.logger.info(
            f"Completed extraction: {len(requests)} total pages for contratos "
            f"({len([r for r in requests if r not in pagination_info['existing_requests']])} new pages fetched)"
        )
        return requests

    async def extract_atas(
        self, data_inicial: str, data_final: str, **filters
    ) -> List[APIRequest]:
        """Extract atas by vigência period with pagination optimization"""

        from .endpoints import build_atas_url

        requests = []

        self.logger.info(f"Starting extraction: atas ({data_inicial} to {data_final})")

        # Analyze existing pagination data
        pagination_info = await self._analyze_existing_pagination(
            endpoint_pattern="atas",
            data_inicial=data_inicial,
            data_final=data_final,
            **filters
        )

        if pagination_info["has_complete_extraction"]:
            self.logger.info(
                f"Found complete extraction for atas "
                f"({pagination_info['total_pages']} pages), returning cached data"
            )
            return pagination_info["existing_requests"]

        # Determine which pages to fetch
        pages_to_fetch = pagination_info["missing_pages"]
        if not pages_to_fetch:
            pages_to_fetch = [1]

        # If we have partial data, we know total pages; otherwise discover by fetching page 1
        total_pages_known = pagination_info["total_pages"] if pagination_info["total_pages"] > 0 else None
        
        for pagina in pages_to_fetch:
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

            # Parse response to discover total pages if not known
            if total_pages_known is None:
                payload_json = json.loads(
                    zlib.decompress(api_request.payload_compressed).decode("utf-8")
                )
                pncp_response = PNCPResponse(**payload_json)
                total_pages_known = pncp_response.totalPaginas
                
                # Now we know total pages, add remaining missing pages
                if total_pages_known > 1:
                    existing_pages = set(pagination_info["existing_pages"])
                    all_pages = set(range(1, total_pages_known + 1))
                    remaining_missing = all_pages - existing_pages - {pagina}
                    pages_to_fetch.extend(sorted(remaining_missing))

        # Add any existing requests to the result
        requests.extend(pagination_info["existing_requests"])

        self.logger.info(
            f"Completed extraction: {len(requests)} total pages for atas "
            f"({len([r for r in requests if r not in pagination_info['existing_requests']])} new pages fetched)"
        )
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
