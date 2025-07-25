"""
Circuit breaker implementation for PNCP API resilience
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Callable, Any
from enum import Enum
import logging
import threading

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: int = 300  # 5 minutes
    success_threshold: int = 2  # For half-open state
    timeout: int = 60  # Request timeout


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open"""

    pass


from typing import List


class CircuitBreaker:
    """Circuit breaker with adaptive behavior for PNCP API"""

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_success_time: Optional[datetime] = None
        self._lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""

        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerError(
                        f"Circuit breaker is OPEN. Last failure: {self.last_failure_time}"
                    )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure(e)
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt to reset"""
        if not self.last_failure_time:
            return True

        time_since_failure = datetime.now() - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.recovery_timeout

    def _on_success(self):
        """Handle successful call"""
        with self._lock:
            self.last_success_time = datetime.now()

            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info(
                        "Circuit breaker reset to CLOSED after successful recovery"
                    )
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = max(0, self.failure_count - 1)

    def _on_failure(self, exception: Exception):
        """Handle failed call"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            self.success_count = 0  # Reset success count

            if self.failure_count >= self.config.failure_threshold:
                if self.state != CircuitState.OPEN:
                    self.state = CircuitState.OPEN
                    logger.warning(
                        f"Circuit breaker OPENED after {self.failure_count} failures. "
                        f"Last error: {exception}"
                    )


class AdaptiveRateLimiter:
    """Rate limiter that adapts based on server responses"""

    def __init__(self, requests_per_minute: int = 120):
        self.requests_per_minute = requests_per_minute
        self.request_times: List[datetime] = []
        self.adaptive_factor = 1.0  # 1.0 = normal, < 1.0 = reduced rate
        self.last_adaptive_check = datetime.now()

    async def acquire(self):
        """Acquire permission to make a request"""
        now = datetime.now()

        # Clean old request times (older than 1 minute)
        cutoff = now - timedelta(minutes=1)
        self.request_times = [t for t in self.request_times if t > cutoff]

        # Calculate current rate limit with adaptive factor
        current_limit = int(self.requests_per_minute * self.adaptive_factor)

        if len(self.request_times) >= current_limit:
            # Need to wait
            oldest_request = min(self.request_times)
            wait_time = 60 - (now - oldest_request).total_seconds()

            if wait_time > 0:
                await self._sleep(wait_time)

        self.request_times.append(datetime.now())

    async def _sleep(self, seconds: float):
        """Sleep function - can be overridden for testing"""
        import asyncio

        await asyncio.sleep(seconds)

    def adapt_rate(self, response_time: float, status_code: int):
        """Adapt rate based on server response"""
        if status_code >= 500 or response_time > 10.0 or status_code == 429:
            self.adaptive_factor = 0.8
        elif status_code == 200 and response_time < 2.0:
            self.adaptive_factor = 1.0


class RetryConfig:
    """Configuration for exponential backoff retry"""

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_factor: float = 2.0,
        backoff_max: int = 300,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.backoff_max = backoff_max
        self.jitter = jitter


async def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    retryable_exceptions: tuple = (Exception,),
    *args,
    **kwargs,
):
    """Execute function with exponential backoff retry"""

    last_exception: Optional[Exception] = None

    for attempt in range(config.max_attempts):
        try:
            return await func(*args, **kwargs)

        except retryable_exceptions as e:
            last_exception = e

            if attempt == config.max_attempts - 1:
                # Last attempt failed
                break

            # Calculate backoff delay
            delay = min(config.backoff_factor**attempt, config.backoff_max)

            # Add jitter to avoid thundering herd
            if config.jitter:
                delay *= 0.5 + random.random() * 0.5

            logger.warning(
                f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f} seconds..."
            )

            import asyncio

            await asyncio.sleep(delay)

    # All attempts failed
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry failed without an exception")
"""
Endpoint utilities for PNCP API integration
"""

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from pydantic import BaseModel, validator
from ..config import ENDPOINT_CONFIG, settings, PNCPAPISettings
from .enums import ModalidadeContratacao


class URLBuilder:
    """Builds PNCP API endpoint URLs with proper parameters"""

    _config_cache: Optional[Dict] = None

    def __init__(self, base_url: Optional[str] = None):
        self.pncp_settings = PNCPAPISettings()
        self.base_url = base_url or self.pncp_settings.base_url
        if URLBuilder._config_cache is None:
            URLBuilder._config_cache = ENDPOINT_CONFIG

    def build_url(self, endpoint_name: str, **params) -> str:
        """Build complete URL for endpoint with parameters"""
        if URLBuilder._config_cache is None:
            URLBuilder._config_cache = ENDPOINT_CONFIG
        config = URLBuilder._config_cache.get(endpoint_name)
        if not config:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")

        # Validate required parameters
        missing_params = []
        for required_param in config.required_params:
            if required_param not in params:
                missing_params.append(required_param)

        if missing_params:
            raise ValueError(
                f"Missing required parameters for {endpoint_name}: {missing_params}"
            )

        # Filter only valid parameters
        valid_params = config.required_params + config.optional_params
        filtered_params = {k: v for k, v in params.items() if k in valid_params}

        # Build URL
        path = config.path
        url = f"{self.base_url}{path}"

        if filtered_params:
            url += f"?{urlencode(filtered_params)}"

        return url

    def get_endpoint_config(self, endpoint_name: str) -> Optional[Dict]:
        """Get configuration for specific endpoint"""
        if ENDPOINT_CONFIG is None:
            return None
        return ENDPOINT_CONFIG.get(endpoint_name)


class DateRangeHelper:
    """Helper for generating date ranges for API requests"""

    @staticmethod
    def format_date(date_obj: date) -> str:
        """Format date as YYYYMMDD for PNCP API"""
        return date_obj.strftime("%Y%m%d")

    @staticmethod
    def parse_date(date_str: str) -> date:
        """Parse YYYYMMDD string to date object"""
        return datetime.strptime(date_str, "%Y%m%d").date()

    @classmethod
    def get_last_n_days(cls, n_days: int) -> Tuple[str, str]:
        """Get date range for last N days"""
        end_date = date.today()
        start_date = end_date - timedelta(days=n_days)
        return cls.format_date(start_date), cls.format_date(end_date)

    @classmethod
    def get_current_month(cls) -> Tuple[str, str]:
        """Get date range for current month"""
        today = date.today()
        start_date = today.replace(day=1)
        return cls.format_date(start_date), cls.format_date(today)

    @classmethod
    def get_previous_month(cls) -> Tuple[str, str]:
        """Get date range for previous month"""
        today = date.today()
        last_month = today.replace(day=1) - timedelta(days=1)
        start_date = last_month.replace(day=1)
        end_date = last_month
        return cls.format_date(start_date), cls.format_date(end_date)

    @classmethod
    def get_month_range_from_string(cls, year_month: str) -> Tuple[str, str]:
        """Get date range for specific month (YYYY-MM format)

        Args:
            year_month: Month in YYYY-MM format (e.g., '2021-10')

        Returns:
            Tuple of (start_date, end_date) in YYYYMMDD format
        """
        import calendar

        try:
            year, month = map(int, year_month.split("-"))
            start_date = date(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = date(year, month, last_day)
            return cls.format_date(start_date), cls.format_date(end_date)
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid month format '{year_month}'. Use YYYY-MM format.") from e

    @classmethod
    def get_month_range(cls, year: int, month: int) -> Tuple[str, str]:
        """Get date range for a specific month"""
        import calendar

        _, num_days = calendar.monthrange(year, month)
        start_date = date(year, month, 1)
        end_date = date(year, month, num_days)
        return cls.format_date(start_date), cls.format_date(end_date)

    @classmethod
    def chunk_date_range(
        cls, start_date: str, end_date: str, chunk_days: int
    ) -> List[Tuple[str, str]]:
        """Split date range into smaller chunks"""
        start = cls.parse_date(start_date)
        end = cls.parse_date(end_date)

        chunks = []
        current = start

        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((cls.format_date(current), cls.format_date(chunk_end)))
            current = chunk_end + timedelta(days=1)

        return chunks

    @staticmethod
    def parse_date_string(date_str: str) -> date:
        """Parse YYYY-MM-DD string to date object"""
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> bool:
        """Validate date range parameters"""
        try:
            start = DateRangeHelper.parse_date_string(start_date)
            end = DateRangeHelper.parse_date_string(end_date)
            return start <= end
        except ValueError:
            return False


class BaseEndpointParams(BaseModel):
    data_inicial: date
    data_final: date
    pagina: int = 1

    @validator("data_final")
    def validate_date_range(cls, v, values):
        if "data_inicial" in values and v < values["data_inicial"]:
            raise ValueError("data_final must be after data_inicial")
        return v


class ContratacoesPublicacaoParams(BaseEndpointParams):
    codigoModalidadeContratacao: int


class ModalidadeHelper:
    """Helper for working with modalidades de contratação"""

    @staticmethod
    def get_modalidade_name(modalidade: ModalidadeContratacao) -> str:
        """Get modalidade name by code"""
        return modalidade.name.replace("_", " ").title()

    @staticmethod
    def get_all_modalidades() -> List[ModalidadeContratacao]:
        """Get all available modalidade codes"""
        return list(ModalidadeContratacao)


class PaginationHelper:
    """Helper for handling API pagination"""

    @staticmethod
    def get_page_size(endpoint_name: str, requested_size: Optional[int] = None) -> int:
        """Get appropriate page size for endpoint"""
        config = ENDPOINT_CONFIG.get(endpoint_name)
        if config is None:
            return 500  # Default fallback

        limits = config.page_size_limits
        default_size = config.default_page_size

        if requested_size is None:
            return default_size

        # Clamp to limits
        return max(limits.min, min(requested_size, limits.max))

    @staticmethod
    def calculate_total_pages(total_records: int, page_size: int) -> int:
        """Calculate total number of pages needed"""
        return (total_records + page_size - 1) // page_size

    @staticmethod
    def has_more_pages(current_page: int, total_pages: int) -> bool:
        """Check if there are more pages to fetch"""
        return current_page < total_pages

    @staticmethod
    def get_page_ranges(total_pages: int, batch_size: int) -> List[Tuple[int, int]]:
        """Get page ranges for parallel processing"""
        ranges = []
        for i in range(1, total_pages + 1, batch_size):
            start_page = i
            end_page = min(i + batch_size - 1, total_pages)
            ranges.append((start_page, end_page))
        return ranges

    @staticmethod
    def estimate_requests_needed(
        date_range_days: int, avg_records_per_day: int, records_per_page: int
    ) -> int:
        """Estimate requests needed for a given date range"""
        total_records = date_range_days * avg_records_per_day
        return (total_records + records_per_page - 1) // records_per_page


class EndpointValidator:
    """Validates endpoint requests and responses"""

    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> None:
        """Validate date range parameters"""
        try:
            start = DateRangeHelper.parse_date(start_date)
            end = DateRangeHelper.parse_date(end_date)
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYYMMDD: {e}")

        if start > end:
            raise ValueError("Start date must be before end date")

        # Check if range is too large
        max_days = settings.MAX_DATE_RANGE_DAYS
        if (end - start).days > max_days:
            raise ValueError(f"Date range too large. Maximum {max_days} days allowed")

    @staticmethod
    def validate_modalidade(modalidade: int) -> bool:
        """Validate modalidade code"""
        if modalidade is None:
            return False
        return modalidade in [m.value for m in ModalidadeContratacao]

    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """Validate date format"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_pagination_params(page: int, page_size: int) -> bool:
        """Validate pagination parameters"""
        return page >= 1 and 1 <= page_size <= 100

    @staticmethod
    def validate_pagination(page: int, page_size: int, endpoint_name: str) -> None:
        """Validate pagination parameters"""
        if page < 1:
            raise ValueError("Page number must be >= 1")

        config = ENDPOINT_CONFIG.get(endpoint_name)
        if config is None:
            limits_min, limits_max = 10, 500  # Default fallback
        else:
            limits_min, limits_max = config.page_size_limits.min, config.page_size_limits.max

        if page_size < limits_min or page_size > limits_max:
            raise ValueError(
                f"Page size must be between {limits_min} and {limits_max}"
            )

    ENDPOINT_PARAM_MODELS = {
        "contratacoes_publicacao": ContratacoesPublicacaoParams,
        # Add other endpoint models here...
    }

    def validate_endpoint_params(self, endpoint_name: str, params: dict) -> dict:
        """Validate endpoint parameters using Pydantic models."""
        model = self.ENDPOINT_PARAM_MODELS.get(endpoint_name)
        if not model:
            # If no model is defined, assume validation is not required
            return {"valid": True, "errors": []}

        try:
            model(**params)
            return {"valid": True, "errors": []}
        except Exception as e:
            return {
                "valid": False,
                "errors": e.errors() if hasattr(e, "errors") else str(e),
            }


# Convenience functions for common operations
def build_contratacao_url(
    data_inicial: str,
    data_final: str,
    modalidade: ModalidadeContratacao,
    pagina: int = 1,
    **kwargs,
) -> str:
    """Build URL for contratações/publicação endpoint"""
    builder = URLBuilder()
    # Use maximum page size for optimization
    page_size = PaginationHelper.get_page_size("contratacoes_publicacao")
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade.value,
        "pagina": pagina,
        "tamanhoPagina": page_size,
        **kwargs,
    }
    return builder.build_url("contratacoes_publicacao", **params)


def build_contratos_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for contratos endpoint"""
    builder = URLBuilder()
    # Use maximum page size for optimization
    page_size = PaginationHelper.get_page_size("contratos")
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        "tamanhoPagina": page_size,
        **kwargs,
    }
    return builder.build_url("contratos", **params)


def build_atas_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for atas endpoint"""
    builder = URLBuilder()
    # Use maximum page size for optimization
    page_size = PaginationHelper.get_page_size("atas")
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        "tamanhoPagina": page_size,
        **kwargs,
    }
    return builder.build_url("atas", **params)


# URL builders for missing endpoints


def build_contratacoes_atualizacao_url(
    data_inicial: str,
    data_final: str,
    modalidade: ModalidadeContratacao,
    pagina: int = 1,
    **kwargs,
) -> str:
    """Build URL for contratações/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade.value,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("contratacoes_atualizacao", **params)


def build_contratacoes_proposta_url(
    data_final: str,
    pagina: int = 1,
    modalidade: Optional[ModalidadeContratacao] = None,
    **kwargs,
) -> str:
    """Build URL for contratações/proposta endpoint"""
    builder = URLBuilder()
    params = {
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    if modalidade:
        params["codigoModalidadeContratacao"] = modalidade.value
    return builder.build_url("contratacoes_proposta", **params)


def build_contratos_atualizacao_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for contratos/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("contratos_atualizacao", **params)


def build_atas_atualizacao_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for atas/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("atas_atualizacao", **params)


def build_instrumentos_cobranca_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for instrumentoscobranca/inclusao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("instrumentoscobranca_inclusao", **params)


def build_pca_url(
    ano_pca: int, codigo_classificacao: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for pca endpoint"""
    builder = URLBuilder()
    params = {
        "anoPca": ano_pca,
        "codigoClassificacaoSuperior": codigo_classificacao,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("pca", **params)


def build_pca_usuario_url(
    ano_pca: int, id_usuario: int, pagina: int = 1, **kwargs
) -> str:
    """Build URL for pca/usuario endpoint"""
    builder = URLBuilder()
    params = {
        "anoPca": ano_pca,
        "idUsuario": id_usuario,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("pca_usuario", **params)


def build_pca_atualizacao_url(
    data_inicio: str, data_fim: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for pca/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicio": data_inicio,
        "dataFim": data_fim,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("pca_atualizacao", **params)


def get_phase_2a_endpoints() -> List[str]:
    """Get list of Phase 2A priority endpoints"""
    return settings.phase_2a_endpoints


def get_all_pncp_endpoints() -> List[str]:
    """Get list of ALL PNCP endpoints for 100% coverage"""
    return settings.ALL_PNCP_ENDPOINTS
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
from .enums import ModalidadeContratacao, get_enum_by_value
from .utils import (
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

        from .utils import build_contratacao_url

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
            from .utils import build_contratacao_url, build_contratos_url, build_atas_url
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

        from .utils import build_contratos_url

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

        from .utils import build_atas_url

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

        from .utils import build_contratacoes_atualizacao_url

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

        from .utils import build_contratacoes_proposta_url

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

        from .utils import build_contratos_atualizacao_url

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

        from .utils import build_atas_atualizacao_url

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

        from .utils import build_instrumentos_cobranca_url

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

        from .utils import build_pca_url

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

        from .utils import build_pca_usuario_url

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

        from .utils import build_pca_atualizacao_url

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
"""I/O utilities for Baliza."""

from pathlib import Path


def load_sql_file(filename: str) -> str:
    """Load SQL file from src/baliza/sql/ directory."""
    sql_path = Path(__file__).parent.parent / "sql" / filename
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
