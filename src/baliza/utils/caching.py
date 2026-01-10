"""Response caching utilities to optimize repeated JSON parsing."""

import contextlib
from collections.abc import Iterator


@contextlib.contextmanager
def install_json_caching() -> Iterator[None]:
    """
    Context manager that patches Response.json() methods in requests and httpx
    to cache their results. This optimizes scenarios where json() is called
    multiple times on the same response object (e.g. by dlt and then by us).
    """
    patches = []

    # Patch requests if available
    try:
        import requests

        patches.append((requests.Response, "json", requests.Response.json))
    except ImportError:
        pass

    # Patch httpx if available
    try:
        import httpx

        patches.append((httpx.Response, "json", httpx.Response.json))
    except ImportError:
        pass

    def make_cached_json(original_method):
        def cached_json(self, *args, **kwargs):
            # Only cache if no arguments are passed to ensure correctness.
            # If arguments are passed (e.g. custom decoder), we skip caching
            # to avoid returning a result decoded with different options.
            if not args and not kwargs:
                # Use a specific attribute name to avoid conflicts
                if not hasattr(self, "_baliza_cached_json"):
                    self._baliza_cached_json = original_method(self)
                return self._baliza_cached_json
            return original_method(self, *args, **kwargs)

        return cached_json

    # Apply patches
    for cls, name, orig in patches:
        setattr(cls, name, make_cached_json(orig))

    try:
        yield
    finally:
        # Restore patches
        for cls, name, orig in patches:
            setattr(cls, name, orig)
