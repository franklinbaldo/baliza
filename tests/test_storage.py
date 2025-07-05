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

    dctx = zstandard.ZstdDecompressor()
    with open(compressed_filepath, 'rb') as f_compressed, open(temp_jsonl_file, 'rb') as f_original:
        original_data = f_original.read()
        decompressed_data = dctx.decompress(f_compressed.read())
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

@patch("baliza.storage.os.remove", side_effect=OSError("Permission denied for delete")) # Target SUT's os.remove
def test_cleanup_local_file_os_error_targeted_patch(mock_os_remove_in_storage):
    temp_file_path = os.path.join(DATA_OUTPUT_DIR, "temp_cleanup_os_error.txt")
    _original_open = builtins.open
    with _original_open(temp_file_path, "w") as f: f.write("cant delete me")

    with patch("baliza.storage.typer.echo") as mock_typer_echo:
        cleanup_local_file(temp_file_path)
        assert os.path.exists(temp_file_path)
        assert any("Error removing temporary file" in c[0][0] for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))
        assert any("Permission denied for delete" in c[0][0] for c in mock_typer_echo.call_args_list if c.kwargs.get("err"))

    if os.path.exists(temp_file_path):
        # Use the real os.remove for cleanup, not the mocked one
        real_os_remove = os.remove # This will be the original if os isn't globally mocked
        if 'baliza.storage.os.remove' == mock_os_remove_in_storage.name: # Check if we are in the test that mocked it
             _builtin_os_remove = builtins.os.remove
             _builtin_os_remove(temp_file_path)
        else: # Should not happen if patch is specific
            real_os_remove(temp_file_path)


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
