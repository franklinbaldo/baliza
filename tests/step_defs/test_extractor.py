"""BDD step definitions for extractor.feature."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock

import httpx
import pytest
from pytest_bdd import given, scenario, then, when

from baliza.engine import BalizaEngine
from baliza.extractor import PNCPExtractor


def _stream_ctx(status_code: int, body: dict | None):
    resp = Mock()
    resp.status_code = status_code
    if status_code >= 400:
        request = httpx.Request("GET", "https://pncp.gov.br/")
        response = httpx.Response(status_code, request=request)

        def _raise():
            raise httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)

        resp.raise_for_status = _raise
    else:
        resp.raise_for_status = Mock()

    payload = json.dumps(body or {}).encode("utf-8")

    def _iter_bytes():
        yield payload

    resp.iter_bytes = _iter_bytes

    cm = MagicMock()
    cm.__enter__ = Mock(return_value=resp)
    cm.__exit__ = Mock(return_value=None)
    return cm


@scenario(
    "../features/extractor.feature",
    "Resumes from a cached page instead of refetching",
)
def test_extractor_cache_resume():
    pass


@scenario(
    "../features/extractor.feature",
    "Retries a transient 5xx before succeeding",
)
def test_extractor_retry():
    pass


@pytest.fixture
def extractor(tmp_path: Path, monkeypatch) -> PNCPExtractor:
    monkeypatch.chdir(tmp_path)
    engine = BalizaEngine(db_path=tmp_path / "ext.duckdb")
    return PNCPExtractor(engine, use_curl=False)


# ── Given ────────────────────────────────────────────────────────────────────


@given(
    "a cached PNCP page for 2023-01 page 1",
    target_fixture="cached_payload",
)
def seed_cache(tmp_path: Path, monkeypatch) -> dict:
    monkeypatch.chdir(tmp_path)
    cache_dir = tmp_path / "data" / "raw" / "2023-01"
    cache_dir.mkdir(parents=True, exist_ok=True)
    payload = {"data": [{"numeroControlePNCP": "FROM-CACHE"}], "totalPaginas": 1}
    (cache_dir / "contratos_p1.json").write_text(json.dumps(payload))
    return payload


@given("PNCP returns 500 twice then 200", target_fixture="retry_sequence")
def stream_sequence(monkeypatch) -> dict:
    payload_200 = {"data": [{"numeroControlePNCP": "AFTER-RETRY"}], "totalPaginas": 1}
    call_count = {"n": 0}

    def fake_stream(self, method, url, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 3:
            return _stream_ctx(500, None)
        return _stream_ctx(200, payload_200)

    monkeypatch.setattr(httpx.Client, "stream", fake_stream)
    return {"payload_200": payload_200, "call_count": call_count}


# ── When ─────────────────────────────────────────────────────────────────────


@when(
    "I fetch the same page with the network set to fail",
    target_fixture="fetch_result",
)
def fetch_with_failing_network(extractor, cached_payload, monkeypatch):
    call_count = {"n": 0}

    def exploding_stream(self, method, url, **kwargs):
        call_count["n"] += 1
        return _stream_ctx(500, None)

    monkeypatch.setattr(httpx.Client, "stream", exploding_stream)
    start = datetime(2023, 1, 1)
    end = datetime(2023, 1, 31)
    data = extractor.fetch_page("contratos", start, end, page=1)
    return {"data": data, "call_count": call_count, "expected": cached_payload}


@when(
    "I fetch contratos for 2023-01 page 1",
    target_fixture="fetch_result",
)
def fetch_with_retries(extractor, retry_sequence):
    start = datetime(2023, 1, 1)
    end = datetime(2023, 1, 31)
    data = extractor.fetch_page("contratos", start, end, page=1)
    return {
        "data": data,
        "call_count": retry_sequence["call_count"],
        "expected": retry_sequence["payload_200"],
    }


# ── Then ─────────────────────────────────────────────────────────────────────


@then("the extractor should return the cached payload")
def check_cached_payload(fetch_result):
    assert fetch_result["data"] == fetch_result["expected"]


@then("the extractor should return the 200 payload")
def check_200_payload(fetch_result):
    assert fetch_result["data"] == fetch_result["expected"]


@then("the PNCP API should not have been called")
def check_no_network(fetch_result):
    assert fetch_result["call_count"]["n"] == 0, (
        f"network called {fetch_result['call_count']['n']} times"
    )


@then("the PNCP API should have been called 3 times")
def check_three_calls(fetch_result):
    assert fetch_result["call_count"]["n"] == 3, (
        f"expected 3 calls, got {fetch_result['call_count']['n']}"
    )
