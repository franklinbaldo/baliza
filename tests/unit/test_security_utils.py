"""Unit tests for security utilities."""

import pytest
from baliza.utils import validate_identifier, validate_resource_path


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
