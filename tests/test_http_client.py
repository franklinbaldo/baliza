"""
Tests for HTTP client with circuit breaker and rate limiting
"""

import pytest
import asyncio
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timedelta

from pydantic import ValidationError

from src.baliza.utils.http_client import (
    PNCPResponse,
    APIRequest,
    AdaptiveRateLimiter,
    PNCPClient,
    EndpointExtractor,
)
from src.baliza.utils.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState
from src.baliza.enums import ModalidadeContratacao


class TestPNCPResponse:
    """Test Pydantic response model"""

    def test_valid_response(self):
        """Test valid PNCP API response"""
        data = {"data": [{"id": 1}, {"id": 2}], "totalRegistros": 100, "totalPaginas": 10, "numeroPagina": 1, "paginasRestantes": 9}

        response = PNCPResponse(**data)
        assert len(response.data) == 2
        assert response.totalRegistros == 100
        assert response.totalPaginas == 10
        assert response.numeroPagina == 1
        assert response.paginasRestantes == 9

    def test_empty_response(self):
        """Test empty response handling"""
        data = {"data": [], "totalRegistros": 0, "totalPaginas": 0, "numeroPagina": 1, "paginasRestantes": 0, "empty": True}

        response = PNCPResponse(**data)
        assert response.data == []
        assert response.totalRegistros == 0
        assert response.empty is True

    def test_invalid_response(self):
        """Test invalid response raises validation error"""
        with pytest.raises(ValidationError):
            PNCPResponse(invalid_field="test")


class TestAPIRequest:
    """Test API request model"""

    def test_create_api_request(self):
        """Test creating API request"""
        request = APIRequest(
            request_id="test_id",
            ingestion_date=datetime.now().date(),
            endpoint="test_endpoint",
            http_status=200,
            etag="test_etag",
            payload_sha256="abc123",
            payload_size=1024,
            payload_compressed=b"test_data",
            collected_at=datetime.now(),
        )

        assert request.endpoint == "test_endpoint"
        assert request.http_status == 200
        assert request.payload_size == 1024
        assert isinstance(request.request_id, str)
        assert isinstance(request.collected_at, datetime)



class TestCircuitBreaker:
    """Test circuit breaker functionality"""

    def test_initial_state(self):
        """Test circuit breaker initial state"""
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30))
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_failure_tracking(self):
        """Test failure counting"""
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=30))

        # First failure
        cb._on_failure(Exception("Test failure"))
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 1

        # Second failure should open circuit
        cb._on_failure(Exception("Test failure"))
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 2

    def test_success_reset(self):
        """Test success resets failure count"""
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30))

        cb._on_failure(Exception("Test failure"))
        assert cb.failure_count == 1

        cb._on_success()
        assert cb.failure_count == 0
        assert cb.state == CircuitState.CLOSED


class TestAdaptiveRateLimiter:
    """Test adaptive rate limiting"""

    

    def test_rate_adjustment(self):
        """Test rate adjustment based on response"""
        limiter = AdaptiveRateLimiter()
        initial_adaptive_factor = limiter.adaptive_factor

        # Simulate 429 response - should decrease rate
        with patch("src.baliza.utils.circuit_breaker.datetime") as mock_dt:
            # Set last_adaptive_check to be old enough
            limiter.last_adaptive_check = datetime.now() - timedelta(seconds=60)
            mock_dt.now.return_value = datetime.now() + timedelta(seconds=31) # Ensure enough time passes
            limiter.adapt_rate(response_time=1.0, status_code=500)
            assert limiter.adaptive_factor < initial_adaptive_factor

        # Gradually increase rate if server is responding well
        with patch("src.baliza.utils.circuit_breaker.datetime") as mock_dt:
            # Set last_adaptive_check to be old enough again
            limiter.last_adaptive_check = datetime.now() - timedelta(seconds=60)
            mock_dt.now.return_value = datetime.now() + timedelta(seconds=62) # Ensure enough time passes
            limiter.adapt_rate(response_time=0.1, status_code=200)
            # Rate should not immediately return to original, but increase slightly
            assert limiter.adaptive_factor > initial_adaptive_factor * 0.8 # Should be greater than the reduced factor
            assert limiter.adaptive_factor <= 1.0 # Should not exceed 1.0



@pytest.mark.asyncio
class TestPNCPClient:
    """Test PNCP HTTP client"""

    async def test_successful_request(self):
        """Test successful API request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"etag": "test_etag"}
        mock_response.content = b'{"data": [], "totalRegistros": 0}'

        with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
            mock_get.return_value.json.return_value = {"data": [], "totalRegistros": 0, "totalPaginas": 0, "numeroPagina": 1, "paginasRestantes": 0, "empty": True}
            client = PNCPClient()

            result = await client.fetch_endpoint_data("test_endpoint", "https://test.com/api")

            assert result.http_status == 200
            assert result.etag == "test_etag"
            assert result.payload_size > 0

    async def test_circuit_breaker_open(self):
        """Test circuit breaker prevents requests when open"""
        client = PNCPClient()

        # Force circuit breaker to open
        for _ in range(client.circuit_breaker.config.failure_threshold):
            client.circuit_breaker._on_failure(Exception("Test failure"))

        with pytest.raises(Exception, match="Circuit breaker is OPEN"):
            await client.fetch_endpoint_data("test_endpoint", "https://test.com/api")

    async def test_rate_limiting(self):
        """Test rate limiting behavior"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.content = b'{"data": [], "totalRegistros": 0}'

        with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
            mock_get.return_value.json.return_value = {"data": [], "totalRegistros": 0, "totalPaginas": 0, "numeroPagina": 1, "paginasRestantes": 0, "empty": True}
            client = PNCPClient()

            start_time = asyncio.get_event_loop().time()

            # Make two requests
            await client.fetch_endpoint_data("test_endpoint", "https://test.com/api")
            await client.fetch_endpoint_data("test_endpoint", "https://test.com/api")

            end_time = asyncio.get_event_loop().time()

            # Should have some delay due to rate limiting
            assert end_time - start_time > 0


@pytest.mark.asyncio
class TestEndpointExtractor:
    """Test endpoint-specific extraction logic"""

    async def test_extract_contratacoes_publicacao(self):
        """Test contratacoes publicacao extraction"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.content = b'{"data": [{"id": 1}], "totalRegistros": 1}'

        with patch("httpx.AsyncClient.get", return_value=mock_response):
            extractor = EndpointExtractor()

            results = await extractor.extract_contratacoes_publicacao(
                data_inicial="2024-01-01",
                data_final="2024-01-31",
                modalidade=ModalidadeContratacao.PREGAO_ELETRONICO,
            )

            assert len(results) >= 1
            assert all(r.endpoint == "contratacoes_publicacao" for r in results)
            assert all(r.http_status == 200 for r in results)

    async def test_extract_contratos(self):
        """Test contratos extraction"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.content = b'{"data": [{"id": 1}], "totalRegistros": 1}'

        with patch("httpx.AsyncClient.get", return_value=mock_response):
            extractor = EndpointExtractor()

            results = await extractor.extract_contratos(
                data_inicial="2024-01-01", data_final="2024-01-31"
            )

            assert len(results) >= 1
            assert all(r.endpoint == "contratos" for r in results)
            assert all(r.http_status == 200 for r in results)

    async def test_pagination_handling(self):
        """Test pagination is handled correctly"""
        # Mock first page with totalRegistros > data length
        mock_response_1 = Mock()
        mock_response_1.status_code = 200
        mock_response_1.headers = {}
        mock_response_1.content = b'{"data": [{"id": 1}], "totalRegistros": 25}'

        # Mock second page
        mock_response_2 = Mock()
        mock_response_2.status_code = 200
        mock_response_2.headers = {}
        mock_response_2.content = b'{"data": [{"id": 2}], "totalRegistros": 25}'

        with patch(
            "httpx.AsyncClient.get", side_effect=[mock_response_1, mock_response_2]
        ):
            extractor = EndpointExtractor()

            results = await extractor.extract_contratos(
                data_inicial="2024-01-01", data_final="2024-01-31"
            )

            # Should make multiple requests for pagination
            assert len(results) >= 2
