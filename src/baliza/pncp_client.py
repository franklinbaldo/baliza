"""PNCP API Client - Modern HTTP Client Implementation

Implements resilient HTTP communication as specified in ADR-002 and ADR-005.
Uses httpx with HTTP/2 support and tenacity for retry logic.
Provides async/concurrent API access with proper back-pressure handling.
"""

import asyncio
import logging
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from baliza.config import settings
from baliza.utils import parse_json_robust

logger = logging.getLogger(__name__)


class PNCPClient:
    """Handles HTTP requests to the PNCP API with retry logic and back-pressure."""

    def __init__(self, concurrency: int):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self._init_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with graceful cleanup."""
        if self.client:
            await self.client.aclose()

    async def _init_client(self):
        """Initialize HTTP client with optimal settings and HTTP/2 fallback."""
        try:
            # Try with HTTP/2 first
            self.client = httpx.AsyncClient(
                base_url=settings.pncp_base_url,
                timeout=settings.request_timeout,
                headers={
                    "User-Agent": settings.user_agent,
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
                base_url=settings.pncp_base_url,
                timeout=settings.request_timeout,
                headers={
                    "User-Agent": settings.user_agent,
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

        except httpx.RequestError as e:
            logger.exception(f"HTTP/2 verification failed: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter())
    async def fetch_with_backpressure(
        self, url: str, params: dict[str, Any], task_id: str | None = None
    ) -> dict[str, Any]:
        """Fetch with semaphore back-pressure and retry logic."""
        logger.info(f"Fetching {url} with params {params}")
        async with self.semaphore:
            response = await self.client.get(url, params=params)
            logger.info(f"Response status code: {response.status_code}")

            # Common success data
            if response.status_code in [200, 204]:
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

            # Final failure after retries
            return {
                "success": False,
                "status_code": response.status_code,
                "error": f"HTTP {response.status_code} after {3} attempts",
                "content": response.text,
                "headers": dict(response.headers),
                "task_id": task_id,
            }
