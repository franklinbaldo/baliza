from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pytest

# =============================================================================
# Test Tier Configuration
# =============================================================================
# Tiers are defined in pyproject.toml:
#   tier0: Critical Path - Without these, tool is useless
#   tier1: Core Features - Essential for production use
#   tier2: Operator Experience - Quality-of-life improvements
#   tier3: Future Enhancements - Aspirational features


def pytest_collection_modifyitems(config, items):
    """Auto-apply tier markers based on feature file tags or location."""
    for item in items:
        # Check if test has @tier0, @tier1, etc. in the scenario tags
        if hasattr(item, "callspec") and "scenario" in item.callspec.params:
            scenario = item.callspec.params["scenario"]
            if hasattr(scenario, "tags"):
                for tag in scenario.tags:
                    if tag.startswith("tier"):
                        item.add_marker(getattr(pytest.mark, tag))

        # Auto-mark based on file path
        test_path = str(item.fspath)
        if "/tier0/" in test_path:
            item.add_marker(pytest.mark.tier0)
        elif "/tier1/" in test_path:
            item.add_marker(pytest.mark.tier1)
        elif "/tier2/" in test_path:
            item.add_marker(pytest.mark.tier2)
        elif "/tier3/" in test_path:
            item.add_marker(pytest.mark.tier3)

        # Default: tests without tier marker get tier1
        tier_markers = [m for m in item.iter_markers() if m.name.startswith("tier")]
        if not tier_markers:
            item.add_marker(pytest.mark.tier1)


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

    def __enter__(self) -> _MockClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, url: str, params: dict[str, Any] | None = None) -> _MockResponse:
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
        self._entries: list[tuple[Matcher, _MockResponse]] = []

    def add_response(
        self,
        *,
        url: str | re.Pattern[str] | None = None,
        json: Any | None = None,
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
