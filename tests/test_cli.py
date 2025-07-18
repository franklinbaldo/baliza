from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from baliza.cli import app

runner = CliRunner()


@patch("subprocess.Popen")
def test_transform(mock_popen):
    """
    Tests the transform command.
    """
    mock_process = MagicMock()
    mock_process.stdout.readline.return_value = b""
    mock_process.returncode = 0
    mock_popen.return_value = mock_process

    result = runner.invoke(app, ["transform"])
    assert result.exit_code == 0
    mock_popen.assert_called_with(
        ["dbt", "run", "--project-dir", "dbt_baliza"],
        stdout=-1,
        stderr=-2,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )


@patch("baliza.loader.export_to_parquet")
@patch("baliza.loader.upload_to_internet_archive")
def test_load(mock_upload, mock_export):
    """
    Tests the load command.
    """
    result = runner.invoke(app, ["load"])
    assert result.exit_code == 0
    mock_export.assert_called_once()
    mock_upload.assert_called_once()
