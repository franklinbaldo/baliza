"""Tests for retry with exponential backoff logic in extractor.

Tests the tenacity-based retry mechanism that handles:
- HTTP 429 (Too Many Requests)
- HTTP 5xx (Server errors)
- Connection errors
- Timeout exceptions
"""

from unittest.mock import MagicMock

import httpx

from baliza.extractor import _is_retryable_error


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
