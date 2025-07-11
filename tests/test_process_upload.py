import pytest
from unittest.mock import patch, mock_open, MagicMock
import os
from baliza.main import process_and_upload_data, PROCESSED_CSV_PATH


@pytest.fixture
def mock_records():
    return [{'id': 1, 'data': 'test1'}, {'id': 2, 'data': 'test2'}]

@pytest.fixture
def mock_endpoint_cfg():
    return {
        "api_path": "/test",
        "file_prefix": "test-prefix",
        "ia_title_prefix": "Test Title"
    }

@pytest.fixture
def mock_run_summary_data():
    return {
        "test_endpoint": {
            "status": "pending",
            "records_fetched": 0,
            "files_generated": []
        }
    }


def test_process_and_upload_data_no_records(mock_endpoint_cfg, mock_run_summary_data):
    """Tests that the function handles no records gracefully."""
    process_and_upload_data("2024-01-01", "test_endpoint", mock_endpoint_cfg, [], mock_run_summary_data)
    assert mock_run_summary_data["test_endpoint"]["status"] == "no_data"


@patch('os.makedirs')
@patch('os.remove')
@patch('os.path.exists', return_value=False) # For _write_to_processed_csv
@patch('builtins.open', new_callable=mock_open)
@patch('zstandard.ZstdCompressor')
@patch('baliza.main.upload')
@patch('os.getenv', side_effect=lambda x: "test_key" if x in ["IA_KEY", "IA_SECRET"] else None)
@patch('baliza.main._write_to_processed_csv')
def test_process_and_upload_data_success(
    mock_write_csv,
    mock_getenv,
    mock_ia_upload,
    mock_zstd_compressor,
    mock_open_file,
    mock_path_exists,
    mock_remove_file,
    mock_makedirs,
    mock_records,
    mock_endpoint_cfg,
    mock_run_summary_data,
    mocker
):
    """Tests successful processing and upload of data."""
    # Mock the compressor stream copy
    mock_zstd_compressor.return_value.copy_stream.return_value = None

    # Mock hashlib.sha256
    mocker_hash = MagicMock()
    mocker_hash.hexdigest.return_value = "mock_sha256_checksum"
    mocker.patch('hashlib.sha256', return_value=mocker_hash)

    # Ensure os.path.exists returns True for the jsonl file before os.remove is called
    mock_path_exists.side_effect = lambda path: path.endswith(".jsonl")

    process_and_upload_data("2024-01-01", "test_endpoint", mock_endpoint_cfg, mock_records, mock_run_summary_data)

    # Assertions for file writing and compression
    mock_makedirs.assert_called_once_with("baliza_data", exist_ok=True)
    assert mock_open_file.call_count >= 2 # For .jsonl and .jsonl.zst
    mock_zstd_compressor.assert_called_once_with(level=3)
    mock_zstd_compressor.return_value.copy_stream.assert_called_once()

    # Assertions for checksum calculation
    mocker_hash.hexdigest.assert_called_once()

    # Assertions for Internet Archive upload
    mock_ia_upload.assert_called_once()
    args, kwargs = mock_ia_upload.call_args
    assert kwargs['identifier'] == "test-prefix-2024-01-01"
    assert kwargs['access_key'] == "test_key"
    assert kwargs['secret_key'] == "test_key"
    assert kwargs['metadata']['sha256'] == "mock_sha256_checksum"

    # Assertions for summary data and CSV writing
    assert mock_run_summary_data["test_endpoint"]["status"] == "success"
    assert mock_run_summary_data["test_endpoint"]["files_generated"][0]["upload_status"] == "success"
    mock_write_csv.assert_called_once()

    # Assertions for cleanup
    mock_remove_file.assert_called_once()


@patch('os.makedirs')
@patch('os.remove')
@patch('os.path.exists', return_value=False) # For _write_to_processed_csv
@patch('builtins.open', new_callable=mock_open)
@patch('zstandard.ZstdCompressor')
@patch('internetarchive.upload')
@patch('os.getenv', side_effect=lambda x: None) # No credentials
@patch('baliza.main._write_to_processed_csv')
def test_process_and_upload_data_no_credentials(
    mock_write_csv,
    mock_getenv,
    mock_ia_upload,
    mock_zstd_compressor,
    mock_open_file,
    mock_path_exists,
    mock_remove_file,
    mock_makedirs,
    mock_records,
    mock_endpoint_cfg,
    mock_run_summary_data,
    mocker
):
    """Tests that upload is skipped when no IA credentials are provided."""
    mock_zstd_compressor.return_value.copy_stream.return_value = None

    mocker_hash = MagicMock()
    mocker_hash.hexdigest.return_value = "mock_sha256_checksum"
    mocker.patch('hashlib.sha256', return_value=mocker_hash)

    # Ensure os.path.exists returns True for the jsonl file before os.remove is called
    # and False for PROCESSED_CSV_PATH for consistent test behavior
    day_iso = "2024-01-01"
    output_dir = "baliza_data"
    base_filename = f"{mock_endpoint_cfg['file_prefix']}-{day_iso}"
    expected_jsonl_path = os.path.join(output_dir, f"{base_filename}.jsonl")

    def path_exists_side_effect(path_arg):
        if path_arg == PROCESSED_CSV_PATH:
            return False # Assume CSV doesn't exist for header writing by _write_to_processed_csv
        if path_arg == expected_jsonl_path:
            return True # JSONL file exists for cleanup
        return False # Default for any other paths
    mock_path_exists.side_effect = path_exists_side_effect

    process_and_upload_data(day_iso, "test_endpoint", mock_endpoint_cfg, mock_records, mock_run_summary_data)

    mock_ia_upload.assert_not_called()
    assert mock_run_summary_data["test_endpoint"]["status"] == "upload_skipped"
    assert mock_run_summary_data["test_endpoint"]["files_generated"][0]["upload_status"] == "skipped_no_credentials"
    mock_write_csv.assert_called_once()
    mock_remove_file.assert_called_once()


@patch('os.makedirs')
@patch('os.remove')
@patch('os.path.exists', return_value=False) # For _write_to_processed_csv
@patch('builtins.open', new_callable=mock_open)
@patch('zstandard.ZstdCompressor')
@patch('baliza.main.upload', side_effect=Exception("Upload Error"))
@patch('os.getenv', side_effect=lambda x: "test_key" if x in ["IA_KEY", "IA_SECRET"] else None)
@patch('baliza.main._write_to_processed_csv')
def test_process_and_upload_data_upload_failure(
    mock_write_csv,
    mock_getenv,
    mock_ia_upload,
    mock_zstd_compressor,
    mock_open_file,
    mock_path_exists,
    mock_remove_file,
    mock_makedirs,
    mock_records,
    mock_endpoint_cfg,
    mock_run_summary_data,
    mocker
):
    """Tests that upload failure is handled correctly."""
    mock_zstd_compressor.return_value.copy_stream.return_value = None

    mocker_hash = MagicMock()
    mocker_hash.hexdigest.return_value = "mock_sha256_checksum"
    mocker.patch('hashlib.sha256', return_value=mocker_hash)

    # Ensure os.path.exists returns True for the jsonl file before os.remove is called
    mock_path_exists.side_effect = lambda path: path.endswith(".jsonl")

    process_and_upload_data("2024-01-01", "test_endpoint", mock_endpoint_cfg, mock_records, mock_run_summary_data)

    mock_ia_upload.assert_called_once()
    assert mock_run_summary_data["test_endpoint"]["status"] == "upload_failed"
    assert mock_run_summary_data["test_endpoint"]["files_generated"][0]["upload_status"] == "failed_upload_error"
    mock_write_csv.assert_called_once()
    mock_remove_file.assert_called_once()
