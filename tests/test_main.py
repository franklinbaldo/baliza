# This file can be used to test the overall CLI behavior and integration of modules.
# Since main.py is now a thin wrapper for cli.py,
# most of these tests might be more relevant in a new test_cli.py.

import pytest
from typer.testing import CliRunner
import json
from unittest.mock import patch, MagicMock
import os # Ensure os is imported for os.path.join
import csv # Ensure csv is imported for state file checks

# Assuming the main Typer app is now in baliza.cli
from baliza.cli import app
from baliza import state, storage, pipeline

runner = CliRunner()

# Fixture to manage the state file for CLI tests
@pytest.fixture(autouse=True)
def manage_cli_state_file():
    if not os.path.exists(state.STATE_DIR):
        os.makedirs(state.STATE_DIR)
    if os.path.exists(state.PROCESSED_CSV_PATH):
        os.remove(state.PROCESSED_CSV_PATH)
    state.ensure_csv_file_and_header()
    yield
    if os.path.exists(state.PROCESSED_CSV_PATH):
        os.remove(state.PROCESSED_CSV_PATH)

# Fixture to manage the data output dir for CLI tests
@pytest.fixture(autouse=True)
def manage_cli_data_output_dir():
    if not os.path.exists(storage.DATA_OUTPUT_DIR):
        os.makedirs(storage.DATA_OUTPUT_DIR)
    yield


def test_run_baliza_help():
    """Test the --help flag for the main run-baliza command."""
    result = runner.invoke(app, ["run-baliza", "--help"])
    assert result.exit_code == 0
    assert "Usage: baliza run-baliza" in result.stdout
    assert "--date-str TEXT" in result.stdout
    assert "BALIZA_DATE" in result.stdout

def test_run_baliza_invalid_date_format():
    result = runner.invoke(app, ["run-baliza", "--date-str", "2023/01/01"])
    assert result.exit_code == 2
    assert "Invalid value for '--date-str'" in result.stderr
    assert "'2023/01/01' is not a valid YYYY-MM-DD date." in result.stderr

def test_run_baliza_no_date_provided():
    result = runner.invoke(app, ["run-baliza"])
    assert result.exit_code == 2
    assert "Missing option '--date-str'" in result.stderr

@patch("baliza.pipeline.harvest_endpoint_data")
@patch("baliza.storage.create_jsonl_file")
@patch("baliza.storage.compress_file_zstd")
@patch("baliza.storage.calculate_sha256_checksum")
@patch("baliza.storage.upload_to_internet_archive")
@patch("baliza.storage.cleanup_local_file")
@patch("baliza.state.check_if_processed", return_value=False)
def test_run_baliza_happy_path_single_endpoint(
    mock_check_processed, mock_cleanup, mock_upload_ia, mock_calc_checksum,
    mock_compress, mock_create_jsonl, mock_harvest,
):
    test_date = "2023-03-15"
    endpoint_key = "contratacoes"
    mock_harvest.return_value = [{"id": 1, "data": "harvested_data"}]
    jsonl_path = os.path.join(storage.DATA_OUTPUT_DIR, f"pncp-ctrt-{test_date}.jsonl")
    compressed_path = f"{jsonl_path}.zst"
    mock_create_jsonl.return_value = jsonl_path
    mock_compress.return_value = compressed_path
    mock_calc_checksum.return_value = "dummy_sha256_checksum_happy"
    mock_upload_ia.return_value = {
        "upload_status": "success",
        "ia_identifier": f"pncp-ctrt-{test_date}",
        "ia_item_url": f"https://archive.org/details/pncp-ctrt-{test_date}"
    }

    result = runner.invoke(app, ["run-baliza", "--date-str", test_date], env={"BALIZA_DATE": ""})
    assert result.exit_code == 0, f"CLI run failed: {result.stdout} {result.stderr}"

    mock_harvest.assert_called_once()
    args_harvest, _ = mock_harvest.call_args
    assert args_harvest[0] == test_date
    assert args_harvest[1] == endpoint_key
    assert args_harvest[2]["api_path"] == "/v1/contratacoes/publicacao"

    summary_start_marker = "--- BALIZA RUN SUMMARY ---"
    summary_end_marker = "--- END BALIZA RUN SUMMARY ---"
    stdout_str = result.stdout
    summary_json = None
    try:
        summary_block_start = stdout_str.rindex(summary_start_marker) + len(summary_start_marker)
        summary_block_end = stdout_str.rindex(summary_end_marker, summary_block_start)
        json_text_block = stdout_str[summary_block_start:summary_block_end].strip()
        summary_json = json.loads(json_text_block)
    except ValueError:
        pytest.fail(f"Could not parse JSON summary from output: {stdout_str}")

    assert summary_json is not None, "Failed to find or parse JSON run summary"
    assert summary_json["overall_status"] == "success"
    assert endpoint_key in summary_json["endpoints_processed"]

    with open(state.PROCESSED_CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["data_date"] == test_date
        assert rows[0]["upload_status"] == "success"

@patch("baliza.pipeline.harvest_endpoint_data", return_value=None)
@patch("baliza.state.check_if_processed", return_value=False)
def test_run_baliza_harvest_fails(mock_check_processed, mock_harvest_fails):
    test_date = "2023-03-16"
    result = runner.invoke(app, ["run-baliza", "--date-str", test_date], env={"BALIZA_DATE": ""})
    assert result.exit_code == 1
    assert "Harvesting failed" in result.stdout
    assert "\"overall_status\": \"failed\"" in result.stdout

@patch("baliza.pipeline.harvest_endpoint_data")
@patch("baliza.storage.create_jsonl_file", return_value="/path/to/file.jsonl")
@patch("baliza.storage.compress_file_zstd", return_value="/path/to/file.jsonl.zst")
@patch("baliza.storage.calculate_sha256_checksum", return_value="a_checksum")
@patch("baliza.storage.upload_to_internet_archive")
@patch("baliza.state.check_if_processed", return_value=False)
def test_run_baliza_upload_fails(
    mock_check_proc, mock_upload, mock_checksum, mock_compress, mock_jsonl, mock_harvest
):
    test_date = "2023-03-17"
    mock_harvest.return_value = [{"id": 1}]
    mock_upload.return_value = {"upload_status": "failed_upload_error", "ia_identifier": "id", "ia_item_url": None}
    result = runner.invoke(app, ["run-baliza", "--date-str", test_date], env={"BALIZA_DATE": ""})
    assert result.exit_code == 1
    assert "Upload failed for 'contratacoes'" in result.stdout
    assert "\"overall_status\": \"failed\"" in result.stdout

@patch("baliza.pipeline.harvest_endpoint_data")
@patch("baliza.state.check_if_processed", return_value=False)
def test_run_baliza_no_records_found(mock_check_proc, mock_harvest_empty):
    test_date = "2023-03-18"
    mock_harvest_empty.return_value = []
    result = runner.invoke(app, ["run-baliza", "--date-str", test_date], env={"BALIZA_DATE": ""})
    assert result.exit_code == 0
    assert "No records found for 'contratacoes'" in result.stdout
    assert "\"overall_status\": \"success\"" in result.stdout

@patch("baliza.state.check_if_processed", return_value=True)
def test_run_baliza_skips_if_already_processed(mock_check_processed_true):
    test_date = "2023-03-19"
    result = runner.invoke(app, ["run-baliza", "--date-str", test_date], env={"BALIZA_DATE": ""})
    assert result.exit_code == 0
    assert "All configured endpoints for date 2023-03-19 appear to have been successfully processed already. Skipping run." in result.stdout
    assert "\"overall_status\": \"skipped_all_done\"" in result.stdout

def test_list_processed_help():
    result = runner.invoke(app, ["list-processed", "--help"])
    assert result.exit_code == 0
    assert "Lists processed items from the state file/database." in result.stdout

def test_list_processed_empty_state():
    result = runner.invoke(app, ["list-processed"])
    assert result.exit_code == 0
    assert "No records found matching your criteria." in result.stdout

def test_list_processed_with_data():
    state.record_processed_item(
        data_date="2023-01-01", endpoint_key="contratacoes", records_fetched=10,
        jsonl_file_path="j1.jsonl", compressed_file_path="c1.zst", sha256_checksum="s1",
        ia_identifier="id1", ia_item_url="url1", upload_status="success", notes="n1"
    )
    state.record_processed_item(
        data_date="2023-01-02", endpoint_key="contratos", records_fetched=5,
        jsonl_file_path="j2.jsonl", compressed_file_path="c2.zst", sha256_checksum="s2",
        ia_identifier="id2", ia_item_url="url2", upload_status="failed_upload_error", notes="n2"
    )
    result = runner.invoke(app, ["list-processed"])
    assert result.exit_code == 0
    assert "Date: 2023-01-01, Endpoint: contratacoes, Status: success, IA ID: id1" in result.stdout
    assert "Date: 2023-01-02, Endpoint: contratos, Status: failed_upload_error, IA ID: id2" in result.stdout

def test_reprocess_help():
    result = runner.invoke(app, ["reprocess", "--help"])
    assert result.exit_code == 0
    assert "Marks an item for reprocessing." in result.stdout

def test_reprocess_command_output():
    result = runner.invoke(app, ["reprocess", "2023-01-01/contratacoes"])
    assert result.exit_code == 0
    assert "Reprocessing command called for: 2023-01-01/contratacoes" in result.stdout

def test_main_py_wrapper_can_be_imported_and_app_is_cli_app():
    from baliza import main as main_module
    from baliza import cli as cli_module
    assert main_module.app is cli_module.app
