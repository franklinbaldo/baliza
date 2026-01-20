from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import urlencode

import pytest


# =============================================================================
# VCR Configuration for Integration Tests
# =============================================================================

@pytest.fixture(scope="module")
def vcr_config():
    """
    VCR configuration for recording/replaying HTTP interactions.

    Configuration:
    - record_mode='once': Record cassettes once, then always replay
    - match_on=['uri', 'method']: Match requests by URL and HTTP method
    - filter_headers: Remove sensitive headers from cassettes
    - cassette_library_dir: Store cassettes in tests/cassettes/
    - decode_compressed_response: Handle gzip/deflate responses
    """
    return {
        "record_mode": "new_episodes",  # Allow recording new interactions
        "match_on": ["uri", "method"],
        "filter_headers": [
            ("authorization", "REDACTED"),
            ("cookie", "REDACTED"),
        ],
        "cassette_library_dir": str(Path(__file__).parent / "cassettes"),
        "path_transformer": lambda path: path + ".yaml",
        "decode_compressed_response": True,
        # This option was removed in a recent version of vcr.py
        # "allow_playback_repeats": True,
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    """Return cassette directory path for VCR."""
    return Path(__file__).parent / "cassettes"


# =============================================================================
# HTTP Mock for Unit Tests (existing)
# =============================================================================


@dataclass
class _MockResponse:
    status_code: int
    _json_data: Any

    def json(self) -> Any:
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


Matcher = Callable[[str], bool | re.Match[str] | None]


class _MockClient:
    def __init__(self, responses: Iterable[tuple[Matcher, _MockResponse]]):
        self._responses = list(responses)

    def __enter__(self) -> "_MockClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> _MockResponse:
        if params:
            query = urlencode(params, doseq=True)
            if query:
                url = f"{url}?{query}"
        for matcher, response in self._responses:
            if matcher(url):
                return response
        raise AssertionError(f"No mock response available for {url}")


class HttpxMock:
    def __init__(self) -> None:
        self._entries: List[tuple[Matcher, _MockResponse]] = []

    def add_response(
        self,
        *,
        url: Optional[str | re.Pattern[str]] = None,
        json: Optional[Any] = None,
        status_code: int = 200,
    ) -> None:
        payload = json if json is not None else {}
        if url is None:
            def matcher(target: str) -> bool:
                return True
        elif isinstance(url, re.Pattern):
            matcher = url.match
        else:
            def matcher(target: str, *, expected: str = url) -> bool:
                return target == expected
        self._entries.append((matcher, _MockResponse(status_code=status_code, _json_data=payload)))

    def create_client(self) -> _MockClient:
        return _MockClient(self._entries)


# Old httpx_mock fixture for dlt CLI - no longer needed
# New simple tests mock httpx.Client.get directly with unittest.mock
#
# @pytest.fixture()
# def httpx_mock() -> HttpxMock:
#     ...
