"""Tests for JSON response caching utilities."""

import json

import httpx
import requests

from baliza.utils.caching import install_json_caching


def test_install_json_caching_requests():
    """Verify that requests.Response.json() is cached."""

    # Create a mock response with content
    content = json.dumps({"key": "value"}).encode("utf-8")
    response = requests.Response()
    response._content = content
    response.status_code = 200

    # Without caching, json() creates a new object each time
    obj1 = response.json()
    obj2 = response.json()
    assert obj1 is not obj2, "Default requests behavior should not cache"

    # With caching
    with install_json_caching():
        response_cached = requests.Response()
        response_cached._content = content
        response_cached.status_code = 200

        cached1 = response_cached.json()
        cached2 = response_cached.json()

        assert cached1 is cached2, "Result should be cached object"
        assert cached1 == {"key": "value"}

        # Verify kwargs bypass cache
        bypass = response_cached.json(parse_int=float)
        assert bypass is not cached1

    # Verify cleanup
    response_after = requests.Response()
    response_after._content = content
    response_after.status_code = 200

    after1 = response_after.json()
    after2 = response_after.json()
    assert after1 is not after2, "Caching should be removed after context exit"


def test_install_json_caching_httpx():
    """Verify that httpx.Response.json() is cached."""

    content = json.dumps({"key": "value"}).encode("utf-8")
    response = httpx.Response(200, content=content)

    # Without caching
    obj1 = response.json()
    obj2 = response.json()
    assert obj1 is not obj2

    # With caching
    with install_json_caching():
        response_cached = httpx.Response(200, content=content)

        cached1 = response_cached.json()
        cached2 = response_cached.json()

        assert cached1 is cached2
        assert cached1 == {"key": "value"}

        # Verify kwargs bypass cache
        # httpx.Response.json() accepts kwargs passed to json.loads
        bypass = response_cached.json(parse_int=float)
        assert bypass is not cached1

    # Verify cleanup
    response_after = httpx.Response(200, content=content)
    after1 = response_after.json()
    after2 = response_after.json()
    assert after1 is not after2


def test_caching_reentrancy():
    """Verify caching works correctly when nested or reused."""
    content = json.dumps({"a": 1}).encode("utf-8")

    with install_json_caching():
        resp = httpx.Response(200, content=content)
        d1 = resp.json()
        d2 = resp.json()
        assert d1 is d2

        # Ensure we can mutate the result if we want (it's just a dict)
        # But callers should be careful. In our case, we just read.
        d1["a"] = 2
        assert d2["a"] == 2

        # Ensure a new response gets its own cache
        resp2 = httpx.Response(200, content=content)
        d3 = resp2.json()
        assert d3 is not d1
        assert d3["a"] == 1
