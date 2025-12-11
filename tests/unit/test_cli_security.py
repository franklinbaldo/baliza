from __future__ import annotations

from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from baliza import cli

runner = CliRunner()

@patch("baliza.cli.export_parquet")
def test_export_rejects_path_traversal_in_ia_identifier(mock_export, tmp_path):
    # Mock export_parquet to return a dummy object with attributes needed
    mock_metadata = MagicMock()
    mock_metadata.output_dir = str(tmp_path / "output")
    mock_metadata.table = "test_table"
    mock_metadata.start_date = "2024-01-01"
    mock_metadata.end_date = "2024-01-01"
    mock_metadata.rows_exported = 10
    mock_metadata.partition_count = 1
    mock_metadata.asdict.return_value = {}
    mock_export.return_value = mock_metadata

    # Create dummy output dir so shutil.copytree doesn't fail if it reached there (it shouldn't)
    (tmp_path / "output").mkdir()

    # Inputs
    malicious_identifier = "hack/../victim"

    result = runner.invoke(cli.app, [
        "export",
        "--table", "foo",
        "--ia-identifier", malicious_identifier,
        "--ia-access-key", "dummy",
        "--ia-secret-key", "dummy"
    ])

    assert result.exit_code == 1
    assert "Error: ia_identifier cannot contain path separators." in result.stderr

    # Try with another separator if on Windows (or just check generally)
    # The code checks os.path.sep

    malicious_identifier_2 = "hack\\..\\victim"
    # This might not trigger on Linux if sep is /, but safe to check if it does nothing bad.
    # If os.path.sep is /, then \ is just a char.

    # Let's test valid identifier
    result_valid = runner.invoke(cli.app, [
        "export",
        "--table", "foo",
        "--ia-identifier", "valid-identifier",
        "--ia-access-key", "dummy",
        "--ia-secret-key", "dummy"
    ])

    # This should fail later because internetarchive lib might be missing or mocked credentials fail
    # But it should NOT fail with the path separator error.
    assert "Error: ia_identifier cannot contain path separators." not in result_valid.stderr
