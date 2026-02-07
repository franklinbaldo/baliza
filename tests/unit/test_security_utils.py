"""Unit tests for security utilities."""

from baliza.utils import scrub_url_params


def test_scrub_url_params_simple():
    """Test scrubbing a simple URL with query parameters."""
    text = "Error accessing https://example.com/api?token=secret123"
    expected = "Error accessing https://example.com/api?***"
    assert scrub_url_params(text) == expected


def test_scrub_url_params_multiple_params():
    """Test scrubbing multiple query parameters."""
    text = "GET https://site.org/search?q=test&api_key=abc&sort=desc"
    expected = "GET https://site.org/search?***"
    assert scrub_url_params(text) == expected


def test_scrub_url_params_no_params():
    """Test that URLs without parameters are unchanged."""
    text = "Connected to https://pncp.gov.br/api/v1/contratos"
    expected = "Connected to https://pncp.gov.br/api/v1/contratos"
    assert scrub_url_params(text) == expected


def test_scrub_url_params_multiple_urls():
    """Test scrubbing multiple URLs in one string."""
    text = (
        "Failed: https://a.com?secret=1 and "
        "https://b.com/resource?id=2&auth=x"
    )
    expected = (
        "Failed: https://a.com?*** and "
        "https://b.com/resource?***"
    )
    assert scrub_url_params(text) == expected


def test_scrub_url_params_in_quotes():
    """Test scrubbing URLs inside quotes (common in exceptions)."""
    text = "Client error '404' for url 'https://api.com/v1?token=xyz'"
    expected = "Client error '404' for url 'https://api.com/v1?***'"
    assert scrub_url_params(text) == expected


def test_scrub_url_params_http():
    """Test scrubbing HTTP URLs."""
    text = "http://insecure.com?pass=123"
    expected = "http://insecure.com?***"
    assert scrub_url_params(text) == expected


def test_scrub_url_params_empty_query():
    """Test scrubbing empty query string."""
    text = "https://example.com/?"
    expected = "https://example.com/?***"
    assert scrub_url_params(text) == expected
