"""Unit tests for security utilities."""

import pytest

from baliza.utils import scrub_url_params, validate_identifier, validate_resource_path


def test_validate_identifier_valid():
    """Test valid identifiers."""
    assert validate_identifier("valid_name") == "valid_name"
    assert validate_identifier("name_123") == "name_123"
    assert validate_identifier("A_B_C") == "A_B_C"


def test_validate_identifier_invalid_chars():
    """Test invalid characters in identifiers."""
    with pytest.raises(ValueError, match="Invalid identifier"):
        validate_identifier("name-with-dash")
    with pytest.raises(ValueError, match="Invalid identifier"):
        validate_identifier("name with space")
    with pytest.raises(ValueError, match="Invalid identifier"):
        validate_identifier("name;drop table")


def test_validate_identifier_length_limit():
    """Test identifier length limit."""
    # Default limit is 64
    long_name = "a" * 65
    with pytest.raises(ValueError, match="exceeds limit"):
        validate_identifier(long_name)

    # Custom limit
    with pytest.raises(ValueError, match="exceeds limit"):
        validate_identifier("abc", max_length=2)

    # Within limit
    assert validate_identifier("a" * 64) == "a" * 64


def test_validate_resource_path_valid():
    """Test valid resource paths."""
    assert validate_resource_path("path/to/resource") == "path/to/resource"
    assert validate_resource_path("resource-name") == "resource-name"
    assert validate_resource_path("resource_123") == "resource_123"


def test_validate_resource_path_invalid_chars():
    """Test invalid characters in resource paths."""
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("path with spaces")
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("path;ls")


def test_validate_resource_path_traversal():
    """Test path traversal attempts."""
    # Fails due to invalid characters ('.') which effectively prevents traversal too
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("../secret")
    with pytest.raises(ValueError, match="Invalid resource path"):
        validate_resource_path("path/../other")


def test_validate_resource_path_length_limit():
    """Test resource path length limit."""
    # Default limit is 255
    long_path = "a" * 256
    with pytest.raises(ValueError, match="exceeds limit"):
        validate_resource_path(long_path)

    # Custom limit
    with pytest.raises(ValueError, match="exceeds limit"):
        validate_resource_path("abc", max_length=2)

    # Within limit
    assert validate_resource_path("a" * 255) == "a" * 255


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
    text = "Failed: https://a.com?secret=1 and https://b.com/resource?id=2&auth=x"
    expected = "Failed: https://a.com?*** and https://b.com/resource?***"
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
