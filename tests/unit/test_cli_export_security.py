from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()


def test_export_arbitrary_directory_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Verification:
    The export command should NO LONGER create a directory named f"ia_archive_{ia_identifier}"
    in the current working directory. It should use a temp dir.
    Thus, an existing directory with that name should be preserved.
    """

    # Use monkeypatch to safely change CWD for this test only
    monkeypatch.chdir(tmp_path)

    # Setup: Create a directory that mimics a user's existing directory
    ia_identifier = "test-dataset"
    target_dir_name = f"ia_archive_{ia_identifier}"
    target_dir = tmp_path / target_dir_name
    target_dir.mkdir()

    # Put a "precious" file in it
    precious_file = target_dir / "precious.txt"
    precious_file.write_text("Do not delete me!")

    assert target_dir.exists()
    assert precious_file.exists()

    # Mocks
    # Mock export_parquet to avoid DB ops
    mock_metadata = MagicMock()
    mock_metadata.asdict.return_value = {}
    mock_metadata.output_dir = str(tmp_path / "output_data")
    mock_metadata.start_date = "2024-01-01"
    mock_metadata.end_date = "2024-01-31"
    mock_metadata.table = "test_table"
    mock_metadata.rows_exported = 100
    mock_metadata.partition_count = 1

    # Create dummy output dir to copy from
    Path(mock_metadata.output_dir).mkdir(exist_ok=True)

    with (
        patch("baliza.cli.export_parquet", return_value=mock_metadata),
        patch("baliza.cli.get_session") as mock_get_session,
    ):
        mock_item = MagicMock()
        mock_session = MagicMock()
        mock_session.get_item.return_value = mock_item
        mock_get_session.return_value = mock_session

        result = runner.invoke(
            app,
            [
                "export",
                "--table",
                "t",
                "--ia-identifier",
                ia_identifier,
                "--ia-access-key",
                "dummy",
                "--ia-secret-key",
                "dummy",
                "--duckdb",
                "dummy.duckdb",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0

    # The directory MUST exist now
    if not target_dir.exists():
        pytest.fail("Directory WAS deleted! Fix failed.")

    assert target_dir.exists()
    assert precious_file.exists()
