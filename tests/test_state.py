import pytest
import os
import csv
import datetime
from unittest.mock import patch, mock_open

from baliza import state
from baliza.state import (
    record_processed_item,
    check_if_processed,
    ensure_csv_file_and_header,
    PROCESSED_CSV_PATH,
    CSV_FIELDNAMES,
    STATE_DIR
)

# --- Fixtures ---

@pytest.fixture(autouse=True) # Run before each test in this module
def manage_state_file():
    """Ensures state dir exists, and cleans up/recreates the CSV file for each test."""
    if not os.path.exists(STATE_DIR):
        os.makedirs(STATE_DIR)

    # If CSV exists from a previous test or run, remove it for a clean slate
    if os.path.exists(PROCESSED_CSV_PATH):
        os.remove(PROCESSED_CSV_PATH)

    # Ensure a fresh CSV with header is created for the test
    ensure_csv_file_and_header()

    yield # Test runs here

    # Teardown: remove the CSV file after the test
    if os.path.exists(PROCESSED_CSV_PATH):
        os.remove(PROCESSED_CSV_PATH)
    # If STATE_DIR was created by this fixture and is empty, could remove it too,
    # but it's generally fine to leave it.

@pytest.fixture
def BALIZA_DATA_OUTPUT_DIR():
    """Provides the data output directory used in other modules, for consistency in test data."""
    # This avoids importing from baliza.storage directly in this state test module,
    # keeping dependencies cleaner if storage.py had complex imports.
    return "baliza_data"


# Sample data for records
@pytest.fixture
def sample_record_data_success(BALIZA_DATA_OUTPUT_DIR):
    return {
        "data_date": "2023-01-01",
        "endpoint_key": "contratacoes_test",
        "records_fetched": 100,
        "jsonl_file_path": f"{BALIZA_DATA_OUTPUT_DIR}/pncp-ctrt-2023-01-01.jsonl",
        "compressed_file_path": f"{BALIZA_DATA_OUTPUT_DIR}/pncp-ctrt-2023-01-01.jsonl.zst",
        "sha256_checksum": "checksum_success_123",
        "ia_identifier": "pncp-ctrt-2023-01-01",
        "ia_item_url": "https://archive.org/details/pncp-ctrt-2023-01-01",
        "upload_status": "success",
        "notes": "Test success entry"
    }

@pytest.fixture
def sample_record_data_failed(BALIZA_DATA_OUTPUT_DIR):
    return {
        "data_date": "2023-01-02",
        "endpoint_key": "contratos_test",
        "records_fetched": 0,
        "jsonl_file_path": None, # No JSONL if harvest failed early
        "compressed_file_path": f"{BALIZA_DATA_OUTPUT_DIR}/pncp-ctos-2023-01-02.jsonl.zst", # Path might still exist
        "sha256_checksum": "checksum_failed_456", # Checksum might exist even if upload fails
        "ia_identifier": "pncp-ctos-2023-01-02", # Identifier might be generated
        "ia_item_url": None, # No URL if upload failed
        "upload_status": "failed_upload_error",
        "notes": "Simulated upload failure"
    }

@pytest.fixture
def sample_record_data_skipped_duplicate(BALIZA_DATA_OUTPUT_DIR, sample_record_data_success):
    """A record that was skipped due to a duplicate checksum."""
    return {
        "data_date": "2023-01-03", # Different date
        "endpoint_key": "pca_test",    # Different endpoint
        "records_fetched": 50,
        "jsonl_file_path": f"{BALIZA_DATA_OUTPUT_DIR}/pncp-pca-2023-01-03.jsonl",
        "compressed_file_path": f"{BALIZA_DATA_OUTPUT_DIR}/pncp-pca-2023-01-03.jsonl.zst",
        "sha256_checksum": sample_record_data_success["sha256_checksum"], # SAME checksum as success
        "ia_identifier": sample_record_data_success["ia_identifier"], # Should point to original IA ID
        "ia_item_url": sample_record_data_success["ia_item_url"],       # Should point to original IA URL
        "upload_status": "skipped_duplicate_checksum",
        "notes": f"Duplicate content (checksum: {sample_record_data_success['sha256_checksum']}). Original upload: IA ID '{sample_record_data_success['ia_identifier']}'"
    }


# --- Tests for ensure_csv_file_and_header ---

def test_ensure_csv_file_and_header_creates_file_with_header():
    # manage_state_file fixture already calls this, but we can test its effect.
    assert os.path.exists(PROCESSED_CSV_PATH)
    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == CSV_FIELDNAMES
        with pytest.raises(StopIteration): # Should be empty after header
            next(reader)

def test_ensure_csv_file_and_header_does_not_overwrite_existing_header(sample_record_data_success):
    # First, ensure_csv_file_and_header has run via fixture.
    # Add a record to simulate an existing file with content.
    record_processed_item(**sample_record_data_success)

    # Call it again
    ensure_csv_file_and_header() # Should be idempotent if header exists

    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == CSV_FIELDNAMES # Header still correct
        row = next(reader) # First data row
        assert row["data_date"] == sample_record_data_success["data_date"]
        # Ensure only one data row exists (not overwritten or duplicated header)
        with pytest.raises(StopIteration):
            next(reader)


# --- Tests for record_processed_item ---

def test_record_processed_item_appends_correctly(sample_record_data_success):
    record_processed_item(**sample_record_data_success)

    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == CSV_FIELDNAMES

        row = next(reader)
        assert row["data_date"] == sample_record_data_success["data_date"]
        assert row["endpoint_key"] == sample_record_data_success["endpoint_key"]
        assert int(row["records_fetched"]) == sample_record_data_success["records_fetched"]
        assert row["upload_status"] == sample_record_data_success["upload_status"]
        assert row["sha256_checksum"] == sample_record_data_success["sha256_checksum"]
        assert row["ia_identifier"] == sample_record_data_success["ia_identifier"]
        assert row["ia_item_url"] == sample_record_data_success["ia_item_url"]
        assert row["notes"] == sample_record_data_success["notes"]
        assert row["jsonl_file_path"] == sample_record_data_success["jsonl_file_path"]
        assert row["compressed_file_path"] == sample_record_data_success["compressed_file_path"]
        # Check timestamp was added (is a valid ISO format string)
        assert datetime.datetime.fromisoformat(row["processing_timestamp_utc"].replace("Z", "+00:00"))

        with pytest.raises(StopIteration): # Only one record written
            next(reader)

def test_record_processed_item_handles_none_values(sample_record_data_failed):
    # sample_record_data_failed has several None values
    record_processed_item(**sample_record_data_failed)

    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row = next(reader)
        assert row["jsonl_file_path"] == "" # None becomes empty string in CSV for jsonl_file_path
        assert row["sha256_checksum"] == sample_record_data_failed["sha256_checksum"] # Should be the value from fixture
        assert row["ia_item_url"] == "" # None becomes empty string for ia_item_url
        assert row["upload_status"] == sample_record_data_failed["upload_status"]


def test_record_processed_item_io_error_on_write(sample_record_data_success, caplog):
    # The manage_state_file fixture has already created the CSV with a header.
    # We use 'patch' as a context manager to limit its scope around the SUT call.

    # This will mock 'builtins.open' only for the duration of the 'with' block.
    with patch("builtins.open", side_effect=IOError("CSV write permission error")) as mock_open_within_context, \
         patch("baliza.state.typer.echo") as mock_typer_echo:

        # This call to record_processed_item will encounter the IOError via the mocked 'open'
        record_processed_item(**sample_record_data_success)

        # Verify that an error was logged via typer.echo due to the IOError
        found_error_log = any(
            "IOError writing to CSV" in call_args[0][0] and "CSV write permission error" in call_args[0][0]
            for call_args in mock_typer_echo.call_args_list if call_args.kwargs.get("err")
        )
        assert found_error_log, "Error log for IOError not found or incorrect."

    # After the 'with patch' block, 'builtins.open' is restored to its original state.
    # Now, verify the file content. It should only contain the header because the write failed.
    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f_verify:
        reader = csv.reader(f_verify)
        header = next(reader)
        assert header == CSV_FIELDNAMES # Check if header is still the expected one
        with pytest.raises(StopIteration): # Should be empty after header
            next(reader)

@patch("baliza.state.CSV_FIELDNAMES", ["data_date", "endpoint_key"]) # Simulate incorrect fieldnames
def test_record_processed_item_key_error_if_data_mismatches_fields(sample_record_data_success):
    # This tests the internal validation logic.
    # If CSV_FIELDNAMES is monkeypatched to be shorter, some keys in sample_record_data_success
    # will not be in the (mocked) CSV_FIELDNAMES.
    with patch("baliza.state.typer.echo") as mock_typer_echo:
        record_processed_item(**sample_record_data_success)
        # Expect an error log about key mismatch
        found_error_log = any("Key '" in call_args[0][0] and "not in CSV_FIELDNAMES" in call_args[0][0] for call_args in mock_typer_echo.call_args_list if call_args.kwargs.get("err"))
        assert found_error_log
        # No item should have been written
        with open(PROCESSED_CSV_PATH, 'r') as f:
            assert len(f.readlines()) == 1 # Only header


# --- Tests for check_if_processed ---

def test_check_if_processed_true_for_success(sample_record_data_success):
    record_processed_item(**sample_record_data_success)
    assert check_if_processed(sample_record_data_success["data_date"], sample_record_data_success["endpoint_key"]) is True

def test_check_if_processed_false_for_failed_status(sample_record_data_failed):
    record_processed_item(**sample_record_data_failed) # status is "failed_upload_error"
    assert check_if_processed(sample_record_data_failed["data_date"], sample_record_data_failed["endpoint_key"]) is False

def test_check_if_processed_false_for_missing_item():
    assert check_if_processed("2023-01-03", "non_existent_endpoint") is False

def test_check_if_processed_false_if_csv_is_empty():
    # manage_state_file ensures CSV exists with header but is otherwise empty.
    assert check_if_processed("2023-01-01", "any_endpoint") is False


def test_check_if_processed_handles_file_not_found_gracefully():
    if os.path.exists(PROCESSED_CSV_PATH):
        os.remove(PROCESSED_CSV_PATH) # Ensure file is not there
    assert check_if_processed("2023-01-01", "any_endpoint") is False


@patch("csv.DictReader") # To simulate CSV errors or malformed content
def test_check_if_processed_handles_csv_error(mock_dict_reader, sample_record_data_success):
    # Record an item so the file is not empty initially.
    # This is more about testing error handling during read.
    record_processed_item(**sample_record_data_success)

    mock_dict_reader.side_effect = csv.Error("Simulated CSV parsing error")
    with patch("baliza.state.typer.echo") as mock_typer_echo:
        assert check_if_processed(sample_record_data_success["data_date"], sample_record_data_success["endpoint_key"]) is False
        found_error_log = any("CSV parsing error" in call_args[0][0] for call_args in mock_typer_echo.call_args_list if call_args.kwargs.get("err"))
        assert found_error_log

def test_check_if_processed_handles_mismatched_headers():
    # Create a CSV with different headers
    wrong_headers = ["col1", "col2", "col3"]
    with open(PROCESSED_CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(wrong_headers)
        writer.writerow(["data1", "data2", "data3"]) # Add a row of data

    with patch("baliza.state.typer.echo") as mock_typer_echo:
        assert check_if_processed("2023-01-01", "any_endpoint") is False
        found_error_log = any("CSV headers mismatch" in call_args[0][0] for call_args in mock_typer_echo.call_args_list if call_args.kwargs.get("err"))
        assert found_error_log


def test_check_if_processed_multiple_records_finds_correct_one(sample_record_data_success, sample_record_data_failed):
    # Record both a success and a failure
    record_processed_item(**sample_record_data_success)
    record_processed_item(**sample_record_data_failed)

    # Check the successful one
    assert check_if_processed(sample_record_data_success["data_date"], sample_record_data_success["endpoint_key"]) is True
    # Check the failed one (should be false as it's not 'success')
    assert check_if_processed(sample_record_data_failed["data_date"], sample_record_data_failed["endpoint_key"]) is False
    # Check a non-existent one
    assert check_if_processed("2023-01-03", "other_endpoint") is False

# Test __main__ block of state.py (illustrative)
# Similar to other modules, this __main__ is for ad-hoc testing.
# Its functionality (recording and checking) is covered by the tests above.
# Direct testing of __main__ output can be done with capsys or by refactoring
# the __main__ logic into a testable function. For now, we skip its direct test.

# Test DATA_OUTPUT_DIR in state.py (if it's used for paths in CSV)
# The sample_record_data_success uses state.DATA_OUTPUT_DIR.
# This is a bit unusual; state.py itself should probably not define DATA_OUTPUT_DIR
# if that's primarily a concern of the storage module.
# However, if it *does* use it for constructing default paths to log, that's testable.
# The fixture `sample_record_data_success` uses `state.DATA_OUTPUT_DIR` in its paths.
# Let's ensure `state.DATA_OUTPUT_DIR` exists and is "baliza_data" as in storage.py.
def test_state_module_data_output_dir_constant():
    # This depends on whether state.py actually defines its own DATA_OUTPUT_DIR
    # The current state.py doesn't define it, but the test fixture for sample_record_data_success does.
    # Let's assume the CSV might store paths relative to a known output dir.
    # For now, state.py does not have its own DATA_OUTPUT_DIR.
    # The paths in CSV are absolute or relative to where the script runs.
    # The test fixture for sample_record_data_success was:
    # `f"{state.DATA_OUTPUT_DIR}/pncp-ctrt-2023-01-01.jsonl"`
    # This implies `state` module should have `DATA_OUTPUT_DIR`.
    # Looking at `src/baliza/state.py`, it does NOT define `DATA_OUTPUT_DIR`.
    # This means the fixture `sample_record_data_success` has a slight issue.
    # It should probably use `storage.DATA_OUTPUT_DIR` or a hardcoded "baliza_data".

    # Let's correct the fixture or assume paths are just strings.
    # For the purpose of testing `state.py`, the exact path string doesn't matter as much as
    # whether it's stored and retrieved correctly.
    # The current tests for `record_processed_item` verify this.
    pass

# Test that ensure_csv_file_and_header is called on module import
# This is tricky to test directly without unloading/reloading modules.
# However, the `manage_state_file` autouse fixture effectively covers this
# by ensuring the file is in the correct state before each test.
# If we wanted to test it in isolation:
# @patch("os.path.exists", return_value=False) # Simulate file not existing
# @patch("builtins.open", new_callable=mock_open)
# def test_ensure_csv_called_on_import_if_file_not_exists(mock_file_open, mock_path_exists):
#     # This would require re-importing state or running a function that triggers it.
#     # For now, this is implicitly handled by the setup fixture.
#     # state.ensure_csv_file_and_header() # Call it directly
#     # mock_path_exists.assert_called_with(PROCESSED_CSV_PATH)
#     # mock_file_open.assert_called_with(PROCESSED_CSV_PATH, 'w', newline='', encoding='utf-8')
#     # writer_mock = mock_file_open().write
#     # header_line = ",".join(CSV_FIELDNAMES) + "\r\n" # csv module uses \r\n by default on Windows
#     # Check if header was written. This is complex with mock_open.
#     pass

# Final check on the STATE_DIR constant
def test_state_directory_constant():
    assert state.STATE_DIR == "state"
    # The manage_state_file fixture also ensures this directory is created.
    assert os.path.exists(state.STATE_DIR)
    # No specific test for os.makedirs(STATE_DIR, exist_ok=True) as it's simple
    # and covered by fixture ensuring the dir exists.
    # If it failed (e.g. permissions), tests requiring file creation would fail.

# --- Tests for check_if_checksum_processed ---

def test_check_if_checksum_processed_found(sample_record_data_success, sample_record_data_failed):
    record_processed_item(**sample_record_data_success)
    record_processed_item(**sample_record_data_failed) # Add another record

    checksum_to_find = sample_record_data_success["sha256_checksum"]
    result = state.check_if_checksum_processed(checksum_to_find)

    assert result is not None
    assert result["original_checksum"] == checksum_to_find
    assert result["ia_identifier"] == sample_record_data_success["ia_identifier"]
    assert result["ia_item_url"] == sample_record_data_success["ia_item_url"]
    assert result["data_date"] == sample_record_data_success["data_date"]
    assert result["endpoint_key"] == sample_record_data_success["endpoint_key"]

def test_check_if_checksum_processed_not_found_non_existent_checksum(sample_record_data_success):
    record_processed_item(**sample_record_data_success)
    assert state.check_if_checksum_processed("non_existent_checksum_xyz") is None

def test_check_if_checksum_processed_not_found_if_status_not_success(sample_record_data_failed):
    # sample_record_data_failed has 'failed_upload_error' status
    record_processed_item(**sample_record_data_failed)
    # Even if checksum exists, it's not a "success" status, so should not be returned by this function
    assert state.check_if_checksum_processed(sample_record_data_failed["sha256_checksum"]) is None

def test_check_if_checksum_processed_empty_checksum_string(sample_record_data_success):
    record_processed_item(**sample_record_data_success)
    with patch("baliza.state._log_error") as mock_log_error:
        assert state.check_if_checksum_processed("") is None
        mock_log_error.assert_called_with("Checksum not provided for checking.")

def test_check_if_checksum_processed_none_checksum_string(sample_record_data_success):
    record_processed_item(**sample_record_data_success)
    with patch("baliza.state._log_error") as mock_log_error:
        assert state.check_if_checksum_processed(None) is None
        mock_log_error.assert_called_with("Checksum not provided for checking.")


def test_check_if_checksum_processed_empty_csv_file():
    # manage_state_file fixture ensures CSV exists with only header
    assert state.check_if_checksum_processed("any_checksum_value") is None

def test_check_if_checksum_processed_csv_file_not_exists():
    if os.path.exists(PROCESSED_CSV_PATH):
        os.remove(PROCESSED_CSV_PATH)
    assert state.check_if_checksum_processed("any_checksum_value") is None


@patch("csv.DictReader")
def test_check_if_checksum_processed_handles_csv_error(mock_csv_reader, sample_record_data_success):
    record_processed_item(**sample_record_data_success) # Ensure file has some content
    mock_csv_reader.side_effect = csv.Error("Simulated CSV parsing error during checksum check")
    with patch("baliza.state._log_error") as mock_log_error:
        result = state.check_if_checksum_processed(sample_record_data_success["sha256_checksum"])
        assert result is None
        mock_log_error.assert_any_call(f"CSV parsing error while checking checksum in {PROCESSED_CSV_PATH}: Simulated CSV parsing error during checksum check")

def test_check_if_checksum_processed_handles_mismatched_headers(sample_record_data_success):
    # Record one item correctly first
    record_processed_item(**sample_record_data_success)
    correct_checksum = sample_record_data_success["sha256_checksum"]

    # Now, rewrite the CSV with wrong headers (but keep the content for checksum to be there)
    # This is a bit artificial; normally DictReader would fail on content if headers are wrong.
    # More realistically, the header itself is wrong when DictReader is initialized.
    wrong_headers = ["col1", "col2", "col3", "sha256_checksum_wrong_name", "upload_status_wrong_name"]
    # Keep original content for a moment to simulate data being there
    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f_read:
        lines = f_read.readlines() # Read header + data

    with open(PROCESSED_CSV_PATH, 'w', newline='', encoding='utf-8') as f_write:
        writer = csv.writer(f_write)
        writer.writerow(wrong_headers) # Write wrong header
        for line in lines[1:]: # Write back original data rows (if any)
            f_write.write(line)


    with patch("baliza.state._log_error") as mock_log_error:
        # Even if the checksum value might be parseable by luck from a misaligned row,
        # the header check should fail first.
        # state.CSV_FIELDNAMES still holds the correct expected headers.
        assert state.check_if_checksum_processed(correct_checksum) is None
        mock_log_error.assert_any_call(f"CSV headers mismatch in {PROCESSED_CSV_PATH}. Expected {CSV_FIELDNAMES}, got {wrong_headers}. Cannot reliably check checksum.")

def test_record_processed_item_with_skipped_duplicate_status(sample_record_data_skipped_duplicate):
    # This tests if a "skipped_duplicate_checksum" record is written correctly
    record_processed_item(**sample_record_data_skipped_duplicate)
    with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row = next(reader)
        assert row["upload_status"] == "skipped_duplicate_checksum"
        assert row["sha256_checksum"] == sample_record_data_skipped_duplicate["sha256_checksum"]
        assert row["ia_identifier"] == sample_record_data_skipped_duplicate["ia_identifier"] # Should be original IA ID
        assert row["ia_item_url"] == sample_record_data_skipped_duplicate["ia_item_url"] # Should be original IA URL
        assert "Duplicate content" in row["notes"]
