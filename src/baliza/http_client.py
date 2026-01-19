"""HTTP client with security features for PNCP API access."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Protocol

try:  # pragma: no cover - optional dependency
    import httpx
except ModuleNotFoundError:  # pragma: no cover - fallback path
    httpx = None  # type: ignore[assignment]


class _HttpClient(Protocol):
    """Protocol for HTTP client interface."""

    def get(
        self, url: str, params: dict[str, Any] | None = None
    ) -> Any:  # pragma: no cover - protocol definition
        ...


HttpClientFactory = Callable[..., AbstractContextManager[_HttpClient]]


class _FallbackResponse:
    """Fallback response object when httpx is not available."""

    def __init__(self, *, status_code: int, text: str) -> None:
        self.status_code = status_code
        self._text = text

    def json(self) -> Any:
        if not self._text:
            return {}
        return json.loads(self._text)

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP request failed with status {self.status_code}")


class _FallbackClient(AbstractContextManager["_FallbackClient"]):
    """Fallback HTTP client using urllib when httpx is not available."""

    def __init__(self, *, headers: dict[str, str] | None = None, timeout: int = 30) -> None:
        self.headers = headers or {}
        self.timeout = timeout

    def __enter__(self) -> _FallbackClient:
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # pragma: no cover - no cleanup needed
        return None

    def get(self, url: str, params: dict[str, Any] | None = None) -> _FallbackResponse:
        from urllib import parse, request

        class NoRedirectHandler(request.HTTPRedirectHandler):
            def http_error_302(self, req, fp, code, msg, headers):
                return fp

            http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302

        if not url.startswith(("http://", "https://")):
            raise ValueError("URL scheme must be http or https")

        query = parse.urlencode(params or {}, doseq=True)
        full_url = f"{url}?{query}" if query else url
        req = request.Request(full_url, headers=self.headers)

        opener = request.build_opener(NoRedirectHandler())
        try:
            with opener.open(req, timeout=self.timeout) as response:
                status = int(response.getcode() or 0)

                # Security: Limit response size to prevent DoS via memory exhaustion
                content = bytearray()
                chunk_size = 8192  # 8KB chunks
                max_size = 10 * 1024 * 1024  # 10 MB limit

                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    content.extend(chunk)
                    if len(content) > max_size:
                        raise RuntimeError(f"Response too large (exceeded {max_size} bytes)")

                text = content.decode("utf-8")
        except Exception as exc:  # pragma: no cover - network not exercised in tests
            # Security: Redact query parameters in error logs to prevent secret leakage
            safe_url = parse.urlparse(full_url)._replace(query="").geturl()
            raise RuntimeError(f"HTTP request to {safe_url} failed: {exc}") from exc
        return _FallbackResponse(status_code=status, text=text)


if httpx is not None:

    class _SecureClient(httpx.Client):
        """Secure HTTP client with response size limits."""

        def send(
            self, request: httpx.Request, *, stream: bool = False, **kwargs: Any
        ) -> httpx.Response:
            if request.url.scheme not in ("http", "https"):
                raise ValueError("URL scheme must be http or https")

            try:
                # Force stream=True to inspect/limit content
                response = super().send(request, stream=True, **kwargs)
            except httpx.RequestError as exc:
                safe_url = str(request.url.copy_with(query=None))
                raise RuntimeError(f"HTTP request to {safe_url} failed: {exc}") from exc

            max_size = 10 * 1024 * 1024  # 10 MB

            def _check_size(chunk: bytes, total: int) -> int:
                total += len(chunk)
                if total > max_size:
                    raise RuntimeError(f"Response too large (exceeded {max_size} bytes)")
                return total

            if stream:
                from collections.abc import Iterable

                original_iter = response.iter_bytes()

                def limited_iter() -> Iterable[bytes]:
                    total = 0
                    for chunk in original_iter:
                        total = _check_size(chunk, total)
                        yield chunk

                response.stream = limited_iter()
            else:
                try:
                    content = bytearray()
                    total = 0
                    for chunk in response.iter_bytes():
                        total = _check_size(chunk, total)
                        content.extend(chunk)

                    # Manually populate response content
                    response._content = bytes(content)
                    # We must close the underlying stream since we've consumed it
                    # and won't be using the response as a stream.
                    response.close()
                except Exception as exc:
                    response.close()
                    raise
            return response


# Global HTTP client factory
_http_client_factory: HttpClientFactory = None  # type: ignore[assignment]


def _default_http_client_factory(
    *, headers: dict[str, str] | None = None, timeout: int = 30
) -> AbstractContextManager[_HttpClient]:
    """Create default HTTP client with security features."""
    if httpx is not None:
        return _SecureClient(headers=headers, timeout=timeout, follow_redirects=False)
    return _FallbackClient(headers=headers, timeout=timeout)


def set_http_client_factory(factory: HttpClientFactory) -> None:
    """Set custom HTTP client factory (for testing)."""
    global _http_client_factory
    _http_client_factory = factory


def reset_http_client_factory() -> None:
    """Reset to default HTTP client factory."""
    global _http_client_factory
    _http_client_factory = _default_http_client_factory


# Initialize with default factory
reset_http_client_factory()
