"""Tests for retry with exponential backoff logic in extractor.

Tests the tenacity-based retry mechanism that handles:
- HTTP 429 (Too Many Requests)
- HTTP 5xx (Server errors)
- Connection errors
- Timeout exceptions
"""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from baliza.extractor import (
    DEFAULT_BASE_DELAY,
    DEFAULT_MAX_RETRIES,
    PNCPExtractor,
    _fetch_page,
    _is_retryable_error,
)


class TestIsRetryableError:
    """Tests for _is_retryable_error function."""

    def test_http_429_is_retryable(self) -> None:
        """HTTP 429 Too Many Requests should be retried."""
        response = MagicMock()
        response.status_code = 429
        exc = httpx.HTTPStatusError("Rate limited", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is True

    def test_http_500_is_retryable(self) -> None:
        """HTTP 500 Internal Server Error should be retried."""
        response = MagicMock()
        response.status_code = 500
        exc = httpx.HTTPStatusError("Server error", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is True

    def test_http_502_is_retryable(self) -> None:
        """HTTP 502 Bad Gateway should be retried."""
        response = MagicMock()
        response.status_code = 502
        exc = httpx.HTTPStatusError("Bad gateway", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is True

    def test_http_503_is_retryable(self) -> None:
        """HTTP 503 Service Unavailable should be retried."""
        response = MagicMock()
        response.status_code = 503
        exc = httpx.HTTPStatusError("Service unavailable", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is True

    def test_http_400_is_not_retryable(self) -> None:
        """HTTP 400 Bad Request should NOT be retried."""
        response = MagicMock()
        response.status_code = 400
        exc = httpx.HTTPStatusError("Bad request", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is False

    def test_http_401_is_not_retryable(self) -> None:
        """HTTP 401 Unauthorized should NOT be retried."""
        response = MagicMock()
        response.status_code = 401
        exc = httpx.HTTPStatusError("Unauthorized", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is False

    def test_http_403_is_not_retryable(self) -> None:
        """HTTP 403 Forbidden should NOT be retried."""
        response = MagicMock()
        response.status_code = 403
        exc = httpx.HTTPStatusError("Forbidden", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is False

    def test_http_404_is_not_retryable(self) -> None:
        """HTTP 404 Not Found should NOT be retried."""
        response = MagicMock()
        response.status_code = 404
        exc = httpx.HTTPStatusError("Not found", request=MagicMock(), response=response)
        assert _is_retryable_error(exc) is False

    def test_connect_error_is_retryable(self) -> None:
        """Connection errors should be retried."""
        exc = httpx.ConnectError("Connection refused")
        assert _is_retryable_error(exc) is True

    def test_timeout_error_is_retryable(self) -> None:
        """Timeout errors should be retried."""
        exc = httpx.TimeoutException("Request timed out")
        assert _is_retryable_error(exc) is True

    def test_value_error_is_not_retryable(self) -> None:
        """ValueError (e.g., response too large) should NOT be retried."""
        exc = ValueError("Response too large")
        assert _is_retryable_error(exc) is False


class TestFetchPageRetry:
    """Tests for _fetch_page retry behavior."""

    def test_success_on_first_attempt(self) -> None:
        """Should return immediately on successful first attempt."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_bytes = MagicMock(return_value=[b'{"data": []}'])
        mock_client.stream.return_value = mock_response

        result = _fetch_page(mock_client, "http://test.com", {})

        assert result == {"data": []}
        assert mock_client.stream.call_count == 1

    def test_retry_on_429_with_eventual_success(self) -> None:
        """Should retry on 429 and succeed when API recovers."""
        mock_client = MagicMock()

        # First call returns 429, second succeeds
        call_count = [0]

        def stream_side_effect(*args, **kwargs):
            call_count[0] += 1
            mock_response = MagicMock()

            if call_count[0] == 1:
                mock_response.status_code = 429
                mock_response.__enter__ = MagicMock(return_value=mock_response)
                mock_response.__exit__ = MagicMock(return_value=False)
                mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                    "Too Many Requests",
                    request=MagicMock(),
                    response=mock_response,
                )
            else:
                mock_response.status_code = 200
                mock_response.__enter__ = MagicMock(return_value=mock_response)
                mock_response.__exit__ = MagicMock(return_value=False)
                mock_response.raise_for_status = MagicMock()
                mock_response.iter_bytes = MagicMock(return_value=[b'{"data": ["success"]}'])

            return mock_response

        mock_client.stream.side_effect = stream_side_effect

        result = _fetch_page(
            mock_client, "http://test.com", {}, max_retries=3, base_delay=0.01
        )

        assert result == {"data": ["success"]}
        assert mock_client.stream.call_count == 2  # 1 failure + 1 success

    def test_non_retryable_error_not_retried(self) -> None:
        """Should not retry on non-retryable errors like 400 Bad Request."""
        mock_client = MagicMock()

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Bad Request",
            request=MagicMock(),
            response=mock_response,
        )
        mock_client.stream.return_value = mock_response

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _fetch_page(mock_client, "http://test.com", {})

        assert exc_info.value.response.status_code == 400
        assert mock_client.stream.call_count == 1  # No retry


class TestPNCPExtractorRetryConfig:
    """Test PNCPExtractor retry configuration."""

    def setup_method(self) -> None:
        """Create temporary database for isolated testing."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"

    def teardown_method(self) -> None:
        """Clean up temporary files."""
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_default_retry_config(self) -> None:
        """Should use default retry configuration."""
        extractor = PNCPExtractor(self.db_path)

        assert extractor.max_retries == DEFAULT_MAX_RETRIES
        assert extractor.base_delay == DEFAULT_BASE_DELAY
        extractor.close()

    def test_custom_retry_config(self) -> None:
        """Should accept custom retry configuration."""
        extractor = PNCPExtractor(
            self.db_path,
            max_retries=10,
            base_delay=2.0,
        )

        assert extractor.max_retries == 10
        assert extractor.base_delay == 2.0
        extractor.close()

    @patch("baliza.extractor._fetch_page")
    def test_extraction_uses_configured_retry(self, mock_fetch) -> None:
        """Should pass retry config to _fetch_page during extraction."""
        mock_fetch.return_value = {"data": [], "totalPaginas": 0}

        extractor = PNCPExtractor(
            self.db_path,
            max_retries=7,
            base_delay=0.5,
        )

        try:
            extractor.extract(
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
                resource="contratos",
                workers=1,
            )
        finally:
            extractor.close()

        # Verify the retry config was passed
        call_kwargs = mock_fetch.call_args.kwargs
        assert call_kwargs["max_retries"] == 7
        assert call_kwargs["base_delay"] == 0.5


class TestPNCPExtractor429Recovery:
    """Integration-style tests for 429 recovery in PNCPExtractor."""

    def setup_method(self) -> None:
        """Create temporary database for isolated testing."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"

    def teardown_method(self) -> None:
        """Clean up temporary files."""
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_extractor_recovers_from_transient_429(self) -> None:
        """
        Given: PNCP API returns 429 on first request
        When: PNCPExtractor retries the request
        Then: Extraction should succeed after API recovers
        """
        extractor = PNCPExtractor(
            self.db_path,
            max_retries=3,
            base_delay=0.01,  # Fast retries for testing
        )

        # Mock API to return 429 once, then succeed
        call_count = [0]

        def mock_stream(*args, **kwargs):
            call_count[0] += 1
            mock_response = MagicMock()

            if call_count[0] == 1:
                mock_response.status_code = 429
                mock_response.__enter__ = MagicMock(return_value=mock_response)
                mock_response.__exit__ = MagicMock(return_value=False)
                mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                    "Too Many Requests",
                    request=MagicMock(),
                    response=mock_response,
                )
            else:
                mock_response.status_code = 200
                mock_response.__enter__ = MagicMock(return_value=mock_response)
                mock_response.__exit__ = MagicMock(return_value=False)
                mock_response.raise_for_status = MagicMock()
                mock_response.iter_bytes = MagicMock(
                    return_value=[b'{"data": [], "totalPaginas": 1}']
                )

            return mock_response

        try:
            with patch.object(extractor.client, "stream", side_effect=mock_stream):
                result = extractor.extract(
                    datetime(2024, 1, 15),
                    datetime(2024, 1, 15),
                    resource="contratos",
                    workers=1,
                )

            # Extraction should succeed
            assert "rows_extracted" in result
            assert call_count[0] == 2  # 1 failure + 1 success
        finally:
            extractor.close()
