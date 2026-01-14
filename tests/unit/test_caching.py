import httpx
import requests

from baliza.utils.caching import install_json_caching


def test_requests_json_caching():
    content = b'{"key": "value"}'
    response = requests.Response()
    response._content = content
    response.status_code = 200

    # Without caching, json() returns a new dict every time
    assert response.json() is not response.json()

    with install_json_caching():
        # First call caches
        data1 = response.json()
        assert data1 == {"key": "value"}

        # Second call returns same object
        data2 = response.json()
        assert data2 is data1

        # Call with kwargs should bypass cache (and return new object)
        # We assume empty kwargs for cache hit.
        # Note: requests.json() passes kwargs to json.loads.
        # Passing a dummy kwarg to trigger bypass.
        # parse_int is a valid argument for json.loads
        data3 = response.json(parse_int=str)
        assert data3 == {"key": "value"}
        assert data3 is not data1

    # After context manager, caching should be disabled
    # The method is restored, so it ignores any _json_cache attribute on the object
    assert response.json() is not response.json()


def test_httpx_json_caching():
    content = b'{"key": "value"}'
    response = httpx.Response(200, content=content)

    # Without caching
    assert response.json() is not response.json()

    with install_json_caching():
        data1 = response.json()
        assert data1 == {"key": "value"}

        data2 = response.json()
        assert data2 is data1

        # Call with kwargs
        data3 = response.json(parse_int=str)
        assert data3 == {"key": "value"}
        assert data3 is not data1

    # After context
    assert response.json() is not response.json()


def test_exception_safety():
    """Ensure original methods are restored even if exception occurs."""
    original_method = requests.Response.json
    try:
        with install_json_caching():
            raise RuntimeError("Boom")
    except RuntimeError:
        pass

    assert requests.Response.json is original_method
