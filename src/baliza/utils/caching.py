from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from typing import Any

import requests

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]


@contextlib.contextmanager
def install_json_caching() -> Iterator[None]:
    """
    Context manager that monkey-patches requests (and httpx) Response objects
    to cache their .json() method results. This prevents re-decoding large JSON
    payloads when multiple components (e.g., dlt and our coverage tracker)
    access the same response data.
    """
    # Patch requests
    _orig_requests_json = requests.Response.json

    def cached_requests_json(self: requests.Response, **kwargs: Any) -> Any:
        # If arguments are passed, skip caching to ensure correctness with custom decoder options
        if kwargs:
            return _orig_requests_json(self, **kwargs)

        if not hasattr(self, "_json_cache"):
            self._json_cache = _orig_requests_json(self)
        return self._json_cache

    requests.Response.json = cached_requests_json  # type: ignore[method-assign]

    # Patch httpx if available
    _orig_httpx_json: Callable[..., Any] | None = None
    if httpx is not None:
        _orig_httpx_json = httpx.Response.json

        def cached_httpx_json(self: httpx.Response, **kwargs: Any) -> Any:
            # If arguments are passed, skip caching
            if kwargs:
                return _orig_httpx_json(self, **kwargs)  # type: ignore[misc]

            if not hasattr(self, "_json_cache"):
                self._json_cache = _orig_httpx_json(self)  # type: ignore[misc]
            return self._json_cache

        httpx.Response.json = cached_httpx_json  # type: ignore[method-assign]

    try:
        yield
    finally:
        # Restore requests
        requests.Response.json = _orig_requests_json  # type: ignore[method-assign]

        # Restore httpx
        if httpx is not None and _orig_httpx_json is not None:
            httpx.Response.json = _orig_httpx_json  # type: ignore[method-assign]
