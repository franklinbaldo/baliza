import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock
import datetime
import os
import json

# Assuming cli.py is in src/baliza/
from baliza.cli import app # The Typer application
from baliza import state # To manage state file for tests

# Fixture to manage the state CSV file for CLI tests
@pytest.fixture(autouse=True)
def manage_cli_state_file():
    """Ensures state dir exists, and cleans up/recreates the CSV file for each CLI test."""
    state_dir = state.STATE_DIR
    processed_csv_path = state.PROCESSED_CSV_PATH

    if not os.path.exists(state_dir):
        os.makedirs(state_dir)

    if os.path.exists(processed_csv_path):
        os.remove(processed_csv_path)

    state.ensure_csv_file_and_header() # Create a fresh CSV with header

    yield

    if os.path.exists(processed_csv_path):
        os.remove(processed_csv_path)

runner = CliRunner()

# --- Basic CLI Tests ---

def test_run_baliza_missing_date_fails():
    result = runner.invoke(app, ["run-baliza"])
    assert result.exit_code == 2 # Typer's exit code for missing options is 2
    assert "Missing option" in result.stderr # Error messages go to stderr
    assert "--date-str" in result.stderr or "--date" in result.stderr

def test_run_baliza_invalid_date_format_fails():
    result = runner.invoke(app, ["run-baliza", "--date-str", "2023/01/01"])
    assert result.exit_code == 1 # Typer's exit code for validation errors is typically 1
    assert "Date must be in YYYY-MM-DD format." in result.stderr # Error messages go to stderr

# --- More Comprehensive run_baliza Tests with Mocking ---

@patch("baliza.cli.pipeline.harvest_endpoint_data")
@patch("baliza.cli.storage.create_jsonl_file")
@patch("baliza.cli.storage.compress_file_zstd")
@patch("baliza.cli.storage.calculate_sha256_checksum")
@patch("baliza.cli.storage.upload_to_internet_archive")
@patch("baliza.cli.state.record_processed_item")
@patch("baliza.cli.state.check_if_processed", return_value=False) # Assume not processed by date/endpoint
@patch("baliza.cli.state.check_if_checksum_processed", return_value=None) # Assume checksum not found initially
def test_run_baliza_successful_flow(
    mock_check_checksum_processed, mock_check_if_processed, mock_record_state, mock_upload,
    mock_calc_checksum, mock_compress, mock_create_jsonl, mock_harvest,
    tmp_path # Pytest fixture for temporary directory
):
    # Mock return values for a successful run
    mock_harvest.return_value = [{"id": 1, "data": "record1"}]

    # Use tmp_path for file operations to avoid cluttering project directory
    # These paths will be returned by the mocked storage functions
    jsonl_file = tmp_path / "pncp-ctrt-2023-01-01.jsonl"
    compressed_file = tmp_path / "pncp-ctrt-2023-01-01.jsonl.zst"

    mock_create_jsonl.return_value = str(jsonl_file)
    mock_compress.return_value = str(compressed_file)
    mock_calc_checksum.return_value = "new_checksum_123"
    mock_upload.return_value = {
        "upload_status": "success",
        "ia_identifier": "pncp-ctrt-2023-01-01",
        "ia_item_url": "https://archive.org/details/pncp-ctrt-2023-01-01"
    }

    # Create dummy files that storage functions would expect,
    # so os.path.exists checks pass if any.
    # For this test, mocks are high-level, so file content doesn't matter.
    # However, if mocks were more transparent, we'd need to write to them.
    # Here, we just need the paths to be valid strings.

    target_date = "2023-01-01"
    result = runner.invoke(app, ["run-baliza", "--date-str", target_date])

    assert result.exit_code == 0
    assert f"BALIZA process starting for target data date: {target_date}" in result.stdout
    assert f"Successfully processed and uploaded data for 'contratacoes'" in result.stdout
    assert "Overall status: success" in result.stdout

    mock_harvest.assert_called_once()
    mock_create_jsonl.assert_called_once()
    mock_compress.assert_called_once_with(str(jsonl_file))
    mock_calc_checksum.assert_called_once_with(str(compressed_file))
    mock_check_checksum_processed.assert_called_once_with("new_checksum_123")
    mock_upload.assert_called_once()

    # Check that state was recorded with "success"
    args, kwargs = mock_record_state.call_args
    assert kwargs["upload_status"] == "success"
    assert kwargs["sha256_checksum"] == "new_checksum_123"


@patch("baliza.cli.pipeline.harvest_endpoint_data")
@patch("baliza.cli.storage.create_jsonl_file")
@patch("baliza.cli.storage.compress_file_zstd")
@patch("baliza.cli.storage.calculate_sha256_checksum")
@patch("baliza.cli.storage.upload_to_internet_archive") # Should NOT be called
@patch("baliza.cli.state.record_processed_item")
@patch("baliza.cli.state.check_if_processed", return_value=False) # Assume not processed by date/endpoint
@patch("baliza.cli.state.check_if_checksum_processed") # Will be configured in the test
def test_run_baliza_skips_upload_if_checksum_exists(
    mock_check_checksum_processed, mock_check_if_processed_date, mock_record_state, mock_upload,
    mock_calc_checksum, mock_compress, mock_create_jsonl, mock_harvest,
    tmp_path
):
    target_date = "2023-02-01"
    existing_checksum = "existing_checksum_789"
    original_ia_id = "pncp-ctrt-2022-12-31" # From a previous, different date
    original_ia_url = f"https://archive.org/details/{original_ia_id}"

    # --- Mock Setup ---
    mock_harvest.return_value = [{"id": 10, "data": "record10"}]

    jsonl_file = tmp_path / f"pncp-ctrt-{target_date}.jsonl"
    compressed_file = tmp_path / f"pncp-ctrt-{target_date}.jsonl.zst"

    mock_create_jsonl.return_value = str(jsonl_file)
    mock_compress.return_value = str(compressed_file)
    mock_calc_checksum.return_value = existing_checksum # This file generates an existing checksum

    # IMPORTANT: Mock check_if_checksum_processed to return details of the existing item
    mock_check_checksum_processed.return_value = {
        "ia_identifier": original_ia_id,
        "ia_item_url": original_ia_url,
        "data_date": "2022-12-31",
        "endpoint_key": "contratacoes", # Can be same or different
        "original_checksum": existing_checksum
    }

    # --- Run CLI ---
    result = runner.invoke(app, ["run-baliza", "--date-str", target_date])

    # --- Assertions ---
    assert result.exit_code == 0 # Skipping is a success condition for the run
    assert f"BALIZA process starting for target data date: {target_date}" in result.stdout
    assert f"Calculating SHA256 checksum for 'contratacoes'" in result.stdout
    assert f"Checksum {existing_checksum} matches a previously successful upload: IA ID '{original_ia_id}'" in result.stdout
    assert "Overall status: success" in result.stdout # Or partial_success if defined, but success is fine

    mock_harvest.assert_called_once()
    mock_create_jsonl.assert_called_once()
    mock_compress.assert_called_once()
    mock_calc_checksum.assert_called_once()
    mock_check_checksum_processed.assert_called_once_with(existing_checksum)

    mock_upload.assert_not_called() # CRITICAL: Upload should be skipped

    # Check that state was recorded with "skipped_duplicate_checksum"
    args, kwargs = mock_record_state.call_args
    assert kwargs["upload_status"] == "skipped_duplicate_checksum"
    assert kwargs["sha256_checksum"] == existing_checksum
    assert kwargs["ia_identifier"] == original_ia_id # Should log the original IA ID
    assert kwargs["ia_item_url"] == original_ia_url   # Should log the original IA URL
    assert f"Duplicate content (checksum: {existing_checksum})" in kwargs["notes"]
    assert f"Original upload: IA ID '{original_ia_id}'" in kwargs["notes"]


@patch("baliza.cli.state.check_if_processed", return_value=True) # All endpoints already processed by date
@patch("baliza.cli.pipeline.harvest_endpoint_data") # Should not be called
def test_run_baliza_skips_if_all_endpoints_previously_successful_by_date(
    mock_harvest, mock_check_if_processed_date
):
    target_date = "2023-03-01"
    result = runner.invoke(app, ["run-baliza", "--date-str", target_date])

    assert result.exit_code == 0
    assert f"INFO: All configured endpoints for date {target_date} appear to have been successfully processed already. Skipping run." in result.stdout

    # Extract and check JSON summary for overall_status
    summary_json = None
    summary_start_marker = "--- BALIZA RUN SUMMARY ---"
    summary_end_marker = "--- END BALIZA RUN SUMMARY ---"
    stdout_str = result.stdout
    try:
        summary_block_start = stdout_str.rindex(summary_start_marker) + len(summary_start_marker)
        summary_block_end = stdout_str.rindex(summary_end_marker, summary_block_start)
        json_text_block = stdout_str[summary_block_start:summary_block_end].strip()
        summary_json = json.loads(json_text_block)
    except ValueError:
        pytest.fail(f"Could not parse JSON summary from output: {stdout_str}")
    assert summary_json is not None, "Failed to find or parse JSON run summary"
    assert summary_json.get("overall_status") == "skipped_all_done"

    mock_harvest.assert_not_called()


@patch("baliza.cli.pipeline.harvest_endpoint_data", return_value=[]) # No records found
@patch("baliza.cli.state.check_if_processed", return_value=False)
@patch("baliza.cli.state.record_processed_item")
def test_run_baliza_handles_no_data_found(
    mock_record_state, mock_check_if_processed_date, mock_harvest
):
    target_date = "2023-04-01"
    result = runner.invoke(app, ["run-baliza", "--date-str", target_date])

    assert result.exit_code == 0 # Not an error
    assert f"No records found for 'contratacoes' on {target_date}. Nothing to process or upload." in result.stdout
    assert "Overall status: success" in result.stdout # If only one endpoint and it's no_data, overall is success

    args, kwargs = mock_record_state.call_args
    assert kwargs["upload_status"] == "no_data_found"
    assert kwargs["records_fetched"] == 0


@patch("baliza.cli.pipeline.harvest_endpoint_data", return_value=None) # Simulate harvest failure
@patch("baliza.cli.state.check_if_processed", return_value=False)
@patch("baliza.cli.state.record_processed_item")
def test_run_baliza_handles_harvest_failure(
    mock_record_state, mock_check_if_processed_date, mock_harvest
):
    target_date = "2023-05-01"
    result = runner.invoke(app, ["run-baliza", "--date-str", target_date])

    assert result.exit_code == 1 # Overall failure
    assert "ERROR: Harvesting failed for 'contratacoes'." in result.stderr

    # Extract and check JSON summary for overall_status
    summary_json = None
    summary_start_marker = "--- BALIZA RUN SUMMARY ---"
    summary_end_marker = "--- END BALIZA RUN SUMMARY ---"
    # Summary still goes to stdout
    stdout_str = result.stdout
    try:
        summary_block_start = stdout_str.rindex(summary_start_marker) + len(summary_start_marker)
        summary_block_end = stdout_str.rindex(summary_end_marker, summary_block_start)
        json_text_block = stdout_str[summary_block_start:summary_block_end].strip()
        summary_json = json.loads(json_text_block)
    except ValueError:
        pytest.fail(f"Could not parse JSON summary from output: {stdout_str}")
    assert summary_json is not None, "Failed to find or parse JSON run summary"
    assert summary_json.get("overall_status") == "failed"

    args, kwargs = mock_record_state.call_args
    assert kwargs["upload_status"] == "harvest_failed"


# Example of testing IA credential handling (from storage, but orchestrated by CLI)
@patch("baliza.cli.pipeline.harvest_endpoint_data")
@patch("baliza.cli.storage.create_jsonl_file")
@patch("baliza.cli.storage.compress_file_zstd")
@patch("baliza.cli.storage.calculate_sha256_checksum")
@patch("baliza.cli.storage.upload_to_internet_archive") # We'll check its args or return
@patch("baliza.cli.state.record_processed_item")
@patch("baliza.cli.state.check_if_processed", return_value=False)
@patch("baliza.cli.state.check_if_checksum_processed", return_value=None)
@patch.dict(os.environ, {"IA_KEY": "", "IA_SECRET": ""}) # Simulate missing env vars
def test_run_baliza_handles_missing_ia_credentials(
    mock_check_checksum_proc, mock_check_if_proc, mock_record_state, mock_upload,
    mock_calc_checksum, mock_compress, mock_create_jsonl, mock_harvest, tmp_path
):
    target_date = "2023-06-01"
    mock_harvest.return_value = [{"id": 1, "data": "record1"}]
    jsonl_file = tmp_path / f"pncp-ctrt-{target_date}.jsonl"
    compressed_file = tmp_path / f"pncp-ctrt-{target_date}.jsonl.zst"
    mock_create_jsonl.return_value = str(jsonl_file)
    mock_compress.return_value = str(compressed_file)
    mock_calc_checksum.return_value = "checksum_for_no_creds_test"

    # Configure mock_upload to simulate the behavior of the actual storage.upload_to_internet_archive
    # when credentials are missing. The actual function logs a warning and returns a specific dict.
    mock_upload.return_value = {
        "upload_status": "skipped_no_credentials",
        "ia_identifier": f"pncp-ctrt-{target_date}",
        "ia_item_url": f"https://archive.org/details/pncp-ctrt-{target_date}" # Potential URL
    }

    result = runner.invoke(app, ["run-baliza", "--date-str", target_date])

    assert result.exit_code == 0 # Partial success is exit code 0 for now
    # The actual warning about missing creds comes from storage.py,
    # so we check the outcome in the summary and state recording.
    assert "Upload skipped for 'contratacoes': skipped_no_credentials" in result.stdout
    assert "Overall status: partial_success" in result.stdout

    mock_upload.assert_called_once() # upload_to_internet_archive IS called, but it handles missing creds internally

    args, kwargs = mock_record_state.call_args
    assert kwargs["upload_status"] == "skipped_no_credentials"


# TODO: Add tests for list-processed and reprocess commands when their CSV/DuckDB logic is firmed up.
# For now, list-processed has very basic CSV reading.

@patch("builtins.open", new_callable=MagicMock) # Mock open to control CSV content
@patch("baliza.cli.csv.DictReader")
def test_list_processed_basic(mock_csv_dict_reader, mock_builtin_open):
    # Simulate CSV content for DictReader
    mock_csv_dict_reader.return_value = [
        {"data_date": "2023-01-01", "endpoint_key": "contratacoes", "upload_status": "success", "ia_identifier": "id1"},
        {"data_date": "2023-01-02", "endpoint_key": "contratos", "upload_status": "failed", "ia_identifier": "id2"},
    ]

    result = runner.invoke(app, ["list-processed"])
    assert result.exit_code == 0
    assert "Date: 2023-01-01, Endpoint: contratacoes, Status: success, IA ID: id1" in result.stdout
    assert "Date: 2023-01-02, Endpoint: contratos, Status: failed, IA ID: id2" in result.stdout

@patch("baliza.cli.state.PROCESSED_CSV_PATH", "non_existent_file.csv")
def test_list_processed_no_state_file():
    # This ensures that if PROCESSED_CSV_PATH doesn't exist, it handles it.
    # The os.path.exists check in list_processed should catch this.
    # The autouse fixture manage_cli_state_file might interfere if it always creates one.
    # For this test, we are patching the path to something that manage_cli_state_file won't create.

    # Temporarily disable manage_cli_state_file for this one test if needed, or ensure it uses the patched path.
    # Simpler: patch os.path.exists as used by the command.
    with patch("os.path.exists", return_value=False):
        result = runner.invoke(app, ["list-processed"])
        assert result.exit_code == 0
        assert f"State file non_existent_file.csv not found." in result.stdout


def test_reprocess_command_placeholder_message():
    result = runner.invoke(app, ["reprocess", "some-identifier"])
    assert result.exit_code == 0
    assert "Reprocessing command called for: some-identifier" in result.stdout
    assert "Reprocessing logic (modifying CSV state) is complex and not fully implemented" in result.stdout

if __name__ == "__main__":
    pytest.main()
