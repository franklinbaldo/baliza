"""Security tests for resource path validation."""

from datetime import datetime

import pytest

from baliza.extractor import PNCPExtractor
from baliza.utils import validate_resource_path


def test_validate_resource_path_valid():
    """Test that valid resource paths are accepted."""
    assert validate_resource_path("contratos") == "contratos"
    assert validate_resource_path("atas_registro") == "atas_registro"
    assert validate_resource_path("atas-registro") == "atas-registro"
    assert validate_resource_path("v1/contratos") == "v1/contratos"
    assert validate_resource_path("some/deep/resource-123") == "some/deep/resource-123"

def test_validate_resource_path_invalid():
    """Test that invalid resource paths are rejected."""
    invalid_inputs = [
        "../admin",
        "/absolute/path",
        "http://evil.com",
        "https://evil.com",
        "contratos?sql=1",
        "contratos; drop table",
        "contratos.json", # Dots currently not allowed by regex
        "win\\path", # Backslashes not allowed
    ]

    for input_str in invalid_inputs:
        with pytest.raises(ValueError) as excinfo:
            validate_resource_path(input_str)
        # Verify message content for some cases
        if ".." in input_str:
            assert "Path traversal" in str(excinfo.value)
        elif input_str.startswith("/"):
            assert "relative" in str(excinfo.value)
        else:
            assert "Invalid resource path" in str(excinfo.value)

def test_extractor_enforces_validation(tmp_path):
    """Test that PNCPExtractor.extract enforces validation."""
    db_path = tmp_path / "test.duckdb"
    extractor = PNCPExtractor(db_path)

    start = datetime(2023, 1, 1)
    end = datetime(2023, 1, 2)

    # Valid should proceed (we expect connection error or mock it, but here we just check it doesn't raise ValueError immediately)
    # Actually, if we don't mock, it will try to connect.
    # But we want to verify it FAILS for invalid.

    with pytest.raises(ValueError, match="Path traversal"):
        extractor.extract(start, end, resource="../secret")

    with pytest.raises(ValueError, match="relative"):
        extractor.extract(start, end, resource="/etc/passwd")

    with pytest.raises(ValueError, match="Invalid resource path"):
        extractor.extract(start, end, resource="http://evil.com")

    extractor.close()
