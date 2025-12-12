from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from typer.testing import CliRunner

from baliza import cli

runner = CliRunner()

def test_export_ia_identifier_path_traversal(tmp_path: Path):
    """Test that path traversal in ia-identifier is blocked."""

    # Mock export_parquet to avoid actual DB operations
    with patch("baliza.cli.export_parquet") as mock_export:
        mock_export.return_value = MagicMock(
            asdict=lambda: {},
            output_dir=str(tmp_path / "out"),
            table="test_table",
            start_date="2024-01-01",
            end_date="2024-01-31",
            rows_exported=10,
            partition_count=1
        )

        # We also need to mock os.environ or provide keys via CLI to reach the validation logic
        # But validation should happen before keys are checked?
        # Actually in the code:
        # metadata = export_parquet(...)
        # typer.echo(...)
        # if ia_identifier and (not ia_access_key ...): checks keys

        # So we need to provide fake keys to reach the upload logic where path is constructed

        result = runner.invoke(
            cli.app,
            [
                "export",
                "--table", "t",
                "--ia-identifier", "../vulnerable",
                "--ia-access-key", "fake",
                "--ia-secret-key", "fake"
            ],
        )

        # We expect a failure due to our validation (which we haven't implemented yet, so it might fail or pass depending on current behavior)
        # Currently it would proceed to try to upload.
        # But we want to Assert that it fails with our new validation.

        # If the code is not fixed yet, this test should fail to catch the error (or pass if we assert success, confirming vuln).
        # We want TDD: write test that expects secure behavior.

        assert result.exit_code != 0
        assert "Invalid value for --ia-identifier" in result.stderr
