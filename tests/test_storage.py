import pytest
import os
import json
import zstandard
import hashlib
from unittest.mock import patch, MagicMock, mock_open
import builtins

from baliza import storage
from baliza.storage import (
    create_jsonl_file,
    compress_file_zstd,
    calculate_sha256_checksum,
    upload_to_internet_archive,
    cleanup_local_file,
    DATA_OUTPUT_DIR
)

@pytest.fixture(autouse=True)
def ensure_data_output_dir():
    if not os.path.exists(DATA_OUTPUT_DIR):
        os.makedirs(DATA_OUTPUT_DIR)
    yield

@pytest.fixture
def sample_records():
    return [{"id": 1, "name": "Test Record 1"}, {"id": 2, "name": "Test Record 2"}]

@pytest.fixture
def temp_jsonl_file_factory(sample_records):
    def _create_file(filename="test_jsonl_temp.jsonl", records_to_write=None, make_empty=False):
        if records_to_write is None:
            records_to_write = sample_records
        filepath = os.path.join(DATA_OUTPUT_DIR, filename)
        if os.path.exists(filepath): os.remove(filepath)
        with open(filepath, "wb") as f_jsonl:
            if not make_empty:
                for record in records_to_write:
                    f_jsonl.write(json.dumps(record, ensure_ascii=False).encode('utf-8') + b"\n")
        return filepath
    return _create_file

@pytest.fixture
def temp_jsonl_file(temp_jsonl_file_factory):
    filepath = temp_jsonl_file_factory()
    yield filepath
    if os.path.exists(filepath): os.remove(filepath)

@pytest.fixture
def temp_empty_jsonl_file(temp_jsonl_file_factory):
    filepath = temp_jsonl_file_factory(filename="empty_test.jsonl", make_empty=True)
    yield filepath
    if os.path.exists(filepath): os.remove(filepath)

@pytest.fixture
def temp_compressed_file(temp_jsonl_file):
    if not os.path.exists(temp_jsonl_file) or os.path.getsize(temp_jsonl_file) == 0:
        pytest.skip("Skipping temp_compressed_file: source JSONL is empty/missing.")
    compressed_filepath = compress_file_zstd(temp_jsonl_file)
    if compressed_filepath is None:
         pytest.fail("Fixture temp_compressed_file setup: Compression failed.")
    yield compressed_filepath
    if compressed_filepath and os.path.exists(compressed_filepath): os.remove(compressed_filepath)

def test_create_jsonl_file_success(sample_records):
    base_filename = "test_create_success"
    expected_filepath = os.path.join(DATA_OUTPUT_DIR, f"{base_filename}.jsonl")
    if os.path.exists(expected_filepath): os.remove(expected_filepath)
    filepath = create_jsonl_file(sample_records, base_filename)
    assert filepath == expected_filepath
    assert os.path.exists(filepath)
    with open(filepath, "rb") as f:
        lines = f.readlines()
    assert len(lines) == len(sample_records)
    for i, record in enumerate(sample_records):
        assert json.loads(lines[i].decode('utf-8').strip()) == record
    os.remove(filepath)

def test_create_jsonl_file_empty_records():
    base_filename = "test_create_empty"
    expected_filepath = os.path.join(DATA_OUTPUT_DIR, f"{base_filename}.jsonl")
    if os.path.exists(expected_filepath): os.remove(expected_filepath)
    filepath = create_jsonl_file([], base_filename)
    assert filepath == expected_filepath
    assert os.path.exists(filepath)
    with open(filepath, "rb") as f:
        assert f.read() == b""
    os.remove(filepath)

@patch("builtins.open", side_effect=IOError("Disk full simulation"))
def test_create_jsonl_file_io_error(mock_open_err, sample_records):
    with patch("baliza.storage.typer.echo") as mock_typer_echo:
        filepath = create_jsonl_file(sample_records, "test_io_error")
        assert filepath is None
        assert any("Error writing JSONL file" in c[0][0] for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))

def test_compress_file_zstd_success(temp_jsonl_file):
    compressed_filepath = compress_file_zstd(temp_jsonl_file)
    assert compressed_filepath is not None
    assert compressed_filepath == f"{temp_jsonl_file}.zst"
    assert os.path.exists(compressed_filepath)
    original_size = os.path.getsize(temp_jsonl_file)
    if original_size > 0 : # Only expect smaller size if input was not empty
      assert os.path.getsize(compressed_filepath) > 0
    # else: compressed empty file will still have zstd header, so size > 0

    # Decompress for verification
    with open(temp_jsonl_file, 'rb') as f_original:
        original_data = f_original.read()

    with open(compressed_filepath, 'rb') as f_compressed_stream:
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(f_compressed_stream) as reader:
            decompressed_data = reader.read()
        assert decompressed_data == original_data

    if os.path.exists(compressed_filepath): os.remove(compressed_filepath)

def test_compress_empty_jsonl_file(temp_empty_jsonl_file):
    compressed_filepath = compress_file_zstd(temp_empty_jsonl_file)
    assert compressed_filepath is not None
    assert os.path.exists(compressed_filepath)
    assert os.path.getsize(compressed_filepath) > 0
    dctx = zstandard.ZstdDecompressor()
    with open(compressed_filepath, 'rb') as f_compressed:
        decompressed_data = dctx.decompress(f_compressed.read())
        assert decompressed_data == b""
    if os.path.exists(compressed_filepath): os.remove(compressed_filepath)

def test_compress_file_zstd_input_not_found():
    filepath = compress_file_zstd("non_existent_file.jsonl")
    assert filepath is None

@patch("zstandard.ZstdCompressor")
def test_compress_file_zstd_compression_error(MockZstdCompressor, temp_jsonl_file):
    mock_compressor_instance = MockZstdCompressor.return_value
    mock_compressor_instance.copy_stream.side_effect = Exception("Zstd copy_stream error")
    with patch("baliza.storage.typer.echo") as mock_typer_echo:
        filepath = compress_file_zstd(temp_jsonl_file)
        assert filepath is None
        assert any("Error during Zstandard compression" in c[0][0] and "Zstd copy_stream error" in c[0][0]
                   for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))

def test_calculate_sha256_checksum_success(temp_compressed_file):
    if not temp_compressed_file or not os.path.exists(temp_compressed_file):
        pytest.skip("Skipping checksum test: compressed file fixture failed or is None.")
    checksum = calculate_sha256_checksum(temp_compressed_file)
    assert checksum is not None
    assert len(checksum) == 64
    file_hash = hashlib.sha256()
    with open(temp_compressed_file, "rb") as f:
        while chunk := f.read(8192): file_hash.update(chunk)
    expected_checksum = file_hash.hexdigest()
    assert checksum == expected_checksum

def test_calculate_sha256_checksum_input_not_found():
    checksum = calculate_sha256_checksum("non_existent_file.zst")
    assert checksum is None

def test_calculate_sha256_checksum_io_error():
    dummy_file = os.path.join(DATA_OUTPUT_DIR, "checksum_io_error.zst")
    _original_open = builtins.open
    with _original_open(dummy_file, "wb") as f: f.write(b"dummy")

    def faulty_open(file, mode='rb', *args, **kwargs):
        if file == dummy_file and mode == 'rb':
            raise IOError("Permission denied (simulated for checksum)")
        return _original_open(file, mode, *args, **kwargs)

    with patch('builtins.open', side_effect=faulty_open), \
         patch("baliza.storage.typer.echo") as mock_typer_echo:
        checksum = calculate_sha256_checksum(dummy_file)
        assert checksum is None
        assert any("Error calculating checksum" in c[0][0] and "Permission denied (simulated for checksum)" in c[0][0]
                   for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))
    if os.path.exists(dummy_file): os.remove(dummy_file)

@patch("baliza.storage.upload")
@patch.dict(os.environ, {"IA_KEY": "test_key", "IA_SECRET": "test_secret"})
def test_upload_to_ia_success(mock_ia_upload, temp_compressed_file):
    if not temp_compressed_file or not os.path.exists(temp_compressed_file):
         pytest.skip("Skipping IA upload test.")
    ia_identifier = "test-item-success"
    mock_response = MagicMock(); mock_response.status_code = 200
    mock_ia_upload.return_value = [mock_response]
    result = upload_to_internet_archive(
        filepath=temp_compressed_file, ia_identifier=ia_identifier, ia_title="Test Title",
        day_iso="2023-01-01", sha256_checksum="cs", endpoint_key="ek", endpoint_cfg={}
    )
    assert result["upload_status"] == "success"

@patch.dict(os.environ, {}, clear=True)
def test_upload_to_ia_no_credentials(temp_compressed_file):
    if not temp_compressed_file or not os.path.exists(temp_compressed_file):
         pytest.skip("Skipping IA no creds test.")
    result = upload_to_internet_archive(
        filepath=temp_compressed_file, ia_identifier="test-no-creds", ia_title="Test No Creds",
        day_iso="2023-01-02", sha256_checksum="dcs", endpoint_key="ek", endpoint_cfg={}
    )
    assert result["upload_status"] == "skipped_no_credentials"

def test_upload_to_ia_file_not_found():
    result = upload_to_internet_archive(filepath="non_existent.zst", ia_identifier="tfnf", ia_title="TFNF", day_iso="d", sha256_checksum="cs", endpoint_key="ek", endpoint_cfg={})
    assert result["upload_status"] == "failed_file_not_found"

@patch("baliza.storage.upload", side_effect=Exception("IA upload bombed"))
@patch.dict(os.environ, {"IA_KEY": "test_key", "IA_SECRET": "test_secret"})
def test_upload_to_ia_upload_exception(mock_ia_upload_exc, temp_compressed_file):
    if not temp_compressed_file or not os.path.exists(temp_compressed_file):
         pytest.skip("Skipping IA upload exception test.")
    with patch("baliza.storage.typer.echo") as mock_typer_echo:
        result = upload_to_internet_archive(
            filepath=temp_compressed_file, ia_identifier="test-item-exception", ia_title="Test Item Exception",
            day_iso="2023-01-04", sha256_checksum="dcs", endpoint_key="ek", endpoint_cfg={}
        )
        assert result["upload_status"] == "failed_upload_error"
        assert any("Failed to upload" in c[0][0] for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))
        exception_name = type(Exception("IA upload bombed")).__name__
        assert any(f"Exception type: {exception_name}" in c[0][0] for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))

def test_cleanup_local_file_success():
    temp_file_path = os.path.join(DATA_OUTPUT_DIR, "temp_cleanup_test.txt")
    with open(temp_file_path, "w") as f: f.write("delete me")
    cleanup_local_file(temp_file_path)
    assert not os.path.exists(temp_file_path)

def test_cleanup_local_file_not_found():
    with patch("baliza.storage._log_warning") as mock_log_warning:
         cleanup_local_file("non_existent_for_cleanup.txt")
         mock_log_warning.assert_called_once_with("Attempted to clean up non-existent file: non_existent_for_cleanup.txt")

def test_cleanup_local_file_os_error_targeted_patch():
    temp_file_path = os.path.join(DATA_OUTPUT_DIR, "temp_cleanup_os_error.txt")
    # Create the file
    with open(temp_file_path, "w") as f:
        f.write("cant delete me")

    assert os.path.exists(temp_file_path)

    # Patch os.remove only for the SUT call
    with patch("baliza.storage.os.remove", side_effect=OSError("Permission denied for delete")) as mock_os_remove_sut, \
         patch("baliza.storage.typer.echo") as mock_typer_echo:

        cleanup_local_file(temp_file_path) # Call the function under test

        # Assertions: file still exists, mock was called, error logged
        assert os.path.exists(temp_file_path)
        mock_os_remove_sut.assert_called_once_with(temp_file_path)
        found_error_log = any(
            "Error removing temporary file" in call_args[0][0] and "Permission denied for delete" in call_args[0][0]
            for call_args in mock_typer_echo.call_args_list if call_args.kwargs.get("err")
        )
        assert found_error_log, "Specific error log for OSError not found"

    # Cleanup: os.remove is now back to original, so this should work
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
    assert not os.path.exists(temp_file_path)


@patch("baliza.storage.typer.echo")
def test_storage_internal_log_functions(mock_typer_echo):
    storage._log_info("Storage info test")
    mock_typer_echo.assert_any_call("Storage info test")
    storage._log_error("Storage error test")
    mock_typer_echo.assert_any_call("Storage error test", err=True)
    storage._log_warning("Storage warning test")

    warning_found = False
    for call_args_tuple in mock_typer_echo.call_args_list:
        args, kwargs = call_args_tuple
        if args and args[0] == "Storage warning test":
            # Ensure storage.typer and storage.typer.colors are valid before access
            if hasattr(storage, 'typer') and hasattr(storage.typer, 'colors') and hasattr(storage.typer.colors, 'YELLOW'):
                 assert kwargs.get("color") == storage.typer.colors.YELLOW
            warning_found = True
            break
    assert warning_found

def test_data_output_dir_created_on_import():
    assert os.path.exists(DATA_OUTPUT_DIR)

def test_checksum_of_compressed_empty_file(temp_empty_jsonl_file):
    compressed_empty = compress_file_zstd(temp_empty_jsonl_file)
    assert compressed_empty is not None
    checksum = calculate_sha256_checksum(compressed_empty)
    assert checksum is not None
    assert len(checksum) == 64
    if os.path.exists(compressed_empty): os.remove(compressed_empty)

@patch.dict(os.environ, {"IA_KEY": "", "IA_SECRET": ""}, clear=True) # Ensure no real keys are present
def test_upload_to_ia_no_credentials_logs_specific_warning(temp_compressed_file, caplog):
    """
    Tests that the specific enhanced warning message is logged when IA credentials are missing.
    Uses caplog to capture logging output from the 'baliza.storage' module.
    """
    if not temp_compressed_file or not os.path.exists(temp_compressed_file):
         pytest.skip("Skipping IA no creds log test: compressed file fixture failed or is None.")

    # For caplog to capture typer.echo, we need to ensure typer.echo calls are routed to logging
    # or mock typer.echo to call a logger.
    # The _log_warning in storage.py uses typer.echo.
    # A simple way for this test: patch _log_warning to use a real logger that caplog can capture.

    import logging
    # Get a logger that caplog can capture.
    # Note: The logger name should ideally match how it's configured if structured logging was fully in place.
    # For this test, using a generic logger that caplog can pick up.
    # Or, directly patch `typer.echo` within the `_log_warning` context if that's simpler.

    # Let's patch `baliza.storage._log_warning` to use a standard logger
    # that caplog can capture.
    # Note: This assumes `_log_warning` is accessible for patching or is the direct call.
    # `_log_warning` is `typer.echo(message, color=typer.colors.YELLOW)`
    # We can patch `typer.echo` specifically for the call inside `upload_to_internet_archive`
    # or rely on the fact that the CLI test `test_run_baliza_handles_missing_ia_credentials`
    # already verifies the *outcome* (status 'skipped_no_credentials').
    # This test is specifically for the *log message content*.

    # Re-evaluating: `_log_warning` is a local function in storage.py.
    # Patching `typer.echo` within the `storage` module.
    with patch("baliza.storage.typer.echo") as mock_typer_echo:
        result = upload_to_internet_archive(
            filepath=temp_compressed_file,
            ia_identifier="test-no-creds-log",
            ia_title="Test No Creds Log",
            day_iso="2023-01-05",
            sha256_checksum="checksum_log_test",
            endpoint_key="contratacoes",
            endpoint_cfg={"ia_title_prefix": "PNCP Contratações"}
        )
        assert result["upload_status"] == "skipped_no_credentials"

        # Check that typer.echo was called with the expected warning message
        found_log = False
        expected_message_part1 = "Internet Archive credentials (IA_KEY, IA_SECRET) not found in environment variables."
        expected_message_part2 = "Upload to Internet Archive will be skipped."
        expected_message_part3 = "Please set these variables if you intend to upload."

        for call_args_tuple in mock_typer_echo.call_args_list:
            args, kwargs = call_args_tuple
            if args: # Ensure there's at least one positional argument (the message)
                message_logged = args[0]
                if (expected_message_part1 in message_logged and
                    expected_message_part2 in message_logged and
                    expected_message_part3 in message_logged and
                    kwargs.get("color") == storage.typer.colors.YELLOW): # Check it was a warning
                    found_log = True
                    break
        assert found_log, f"Expected IA credential warning message not found in typer.echo calls. Calls: {mock_typer_echo.call_args_list}"
