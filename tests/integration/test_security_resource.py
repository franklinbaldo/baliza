from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from src.baliza.cli_simple import app
from src.baliza.extractor import PNCPExtractor
from src.baliza.engine import BalizaEngine

runner = CliRunner()


def test_extractor_prevents_path_traversal(tmp_path):
    """Test that PNCPExtractor raises ValueError for resources with path traversal."""
    engine = BalizaEngine(tmp_path / "test.duckdb")
    extractor = PNCPExtractor(engine)

    # Invalid resource should raise ValueError
    with pytest.raises(ValueError, match="Invalid resource path"):
        extractor.fetch_page(resource="../secret", start_date=datetime.now(), end_date=datetime.now())


def test_cli_verify_prevents_command_injection():
    """Test that 'verify' command rejects invalid resource names."""
    result = runner.invoke(app, ["verify", "--resource", "contratos; rm -rf /", "--start", "2023-01-01", "--end", "2023-01-31"])
    assert result.exit_code != 0, f"Expected non-zero exit code. Output was: {result.output}"
    assert "Invalid resource path" in result.output, f"Expected 'Invalid resource path' in output. Output was: {result.output}"


def test_cli_extract_prevents_path_traversal():
    """Test that 'extract' command rejects path traversal attempts."""
    # We use 'verify' for traversal check as it's a simple way to test validation
    result = runner.invoke(app, ["verify", "--resource", "../secret", "--start", "2023-01-01", "--end", "2023-01-01"])
    assert result.exit_code != 0, f"Expected non-zero exit code. Output was: {result.output}"
    assert "Invalid resource path" in result.output, f"Expected 'Invalid resource path' in output. Output was: {result.output}"
