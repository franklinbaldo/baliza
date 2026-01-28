from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from src.baliza.cli_simple import app
from src.baliza.extractor import PNCPExtractor

runner = CliRunner()

def test_extractor_prevents_path_traversal():
    """Test that PNCPExtractor raises ValueError for resources with path traversal."""
    extractor = PNCPExtractor(Path(":memory:"))

    # Invalid resource should raise ValueError
    # We mock _fetch_page so we don't actually hit the network if validation fails (or is missing)
    # But we expect validation to happen BEFORE network call.
    with patch("src.baliza.extractor._fetch_page", return_value={"data": []}):
        with pytest.raises(ValueError, match="Invalid resource path"):
            extractor.extract(datetime.now(), datetime.now(), resource="../secret")

def test_cli_verify_prevents_command_injection():
    """Test that CLI verify command validates resource input."""
    result = runner.invoke(app, [
        "verify",
        "--resource", "contratos; rm -rf /",
        "--start", "2024-01-01",
        "--end", "2024-01-02",
        "--duckdb", ":memory:"
    ])

    assert result.exit_code != 0
    assert "Invalid resource path" in result.stdout

def test_cli_extract_prevents_path_traversal():
    """Test that CLI extract command validates resource input."""
    result = runner.invoke(app, [
        "extract",
        "--resource", "../etc/passwd",
        "--start", "2024-01-01",
        "--end", "2024-01-02",
        "--duckdb", ":memory:"
    ])

    assert result.exit_code != 0
    assert "Invalid resource path" in result.stdout
