# This module will handle state management.
# Initially, it will use CSV as per the original design.
# It will be replaced by DuckDB in Week 2.

import os
import csv
import datetime
import threading # For thread-safe CSV writing, basic protection

import typer # For logging, will be replaced by logging module later

# Placeholder for actual logging
def _log_info(message):
    typer.echo(message)

def _log_error(message):
    typer.echo(message, err=True)

STATE_DIR = "state"
PROCESSED_CSV_FILENAME = "processed_records.csv" # Renaming for clarity vs. old "processed.csv"
PROCESSED_CSV_PATH = os.path.join(STATE_DIR, PROCESSED_CSV_FILENAME)

# Ensure the state directory exists
os.makedirs(STATE_DIR, exist_ok=True)

# A simple lock for CSV file operations to prevent concurrent write issues
# This is a basic measure; DuckDB will offer more robust transaction control.
_csv_lock = threading.Lock()

# Define the expected headers for the CSV file.
# This helps in maintaining consistency.
CSV_FIELDNAMES = [
    "processing_timestamp_utc", # When this record was written
    "data_date",                # The date the data pertains to (e.g., YYYY-MM-DD from PNCP)
    "endpoint_key",             # Identifier for the PNCP endpoint (e.g., "contratacoes")
    "records_fetched",          # Number of records fetched from PNCP for this entry
    "jsonl_file_path",          # Path to the local raw JSONL file (might be temporary)
    "compressed_file_path",     # Path to the local compressed Zstd file
    "sha256_checksum",          # SHA256 checksum of the compressed file
    "ia_identifier",            # Internet Archive item identifier
    "ia_item_url",              # Full URL to the Internet Archive item
    "upload_status",            # Status of the upload (e.g., "success", "failed_upload_error", "skipped_no_credentials")
    "notes"                     # Any additional notes or error messages
]

def ensure_csv_file_and_header():
    """Ensures the CSV file exists and has the correct header row if it's new."""
    if not os.path.exists(PROCESSED_CSV_PATH):
        _log_info(f"Processed records CSV file not found at {PROCESSED_CSV_PATH}. Creating with header.")
        with open(PROCESSED_CSV_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            _log_info(f"Created {PROCESSED_CSV_PATH} with header.")

# Initialize CSV file and header when module is loaded
ensure_csv_file_and_header()

def record_processed_item(
    data_date: str,
    endpoint_key: str,
    records_fetched: int,
    jsonl_file_path: str | None,
    compressed_file_path: str,
    sha256_checksum: str | None,
    ia_identifier: str | None,
    ia_item_url: str | None,
    upload_status: str,
    notes: str = ""
):
    """
    Appends a record to the processed items CSV file.

    Args:
        data_date: The date for which data was fetched (YYYY-MM-DD).
        endpoint_key: The specific PNCP endpoint key.
        records_fetched: Number of records obtained from PNCP.
        jsonl_file_path: Path to the (temporary) JSONL file.
        compressed_file_path: Path to the compressed .zst file.
        sha256_checksum: SHA256 checksum of the compressed file.
        ia_identifier: Internet Archive identifier for the uploaded item.
        ia_item_url: URL to the item on Internet Archive.
        upload_status: A string indicating the outcome of the upload.
        notes: Optional notes or error details.
    """
    with _csv_lock:
        # Ensure header exists, especially if file was deleted manually after module load
        if not os.path.exists(PROCESSED_CSV_PATH) or os.path.getsize(PROCESSED_CSV_PATH) == 0:
            ensure_csv_file_and_header()

        timestamp_utc = datetime.datetime.now(datetime.UTC).isoformat()
        row_data = {
            "processing_timestamp_utc": timestamp_utc,
            "data_date": data_date,
            "endpoint_key": endpoint_key,
            "records_fetched": records_fetched,
            "jsonl_file_path": jsonl_file_path or "", # Handle None
            "compressed_file_path": compressed_file_path,
            "sha256_checksum": sha256_checksum or "", # Handle None
            "ia_identifier": ia_identifier or "",     # Handle None
            "ia_item_url": ia_item_url or "",         # Handle None
            "upload_status": upload_status,
            "notes": notes
        }

        # Validate that all keys in row_data are in CSV_FIELDNAMES to catch errors early
        for key in row_data.keys():
            if key not in CSV_FIELDNAMES:
                _log_error(f"Error: Key '{key}' in data to be written is not in CSV_FIELDNAMES. Data: {row_data}")
                # Decide on error handling: raise error, skip write, or write partial?
                # For now, log and skip, as this is a programming error.
                return

        try:
            with open(PROCESSED_CSV_PATH, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                # If the file was just created by another thread/process after check,
                # header might be missing. This is a limitation of this simple CSV approach.
                # A more robust check would be to read the first line and verify header.
                # However, ensure_csv_file_and_header() at module load and before write helps.

                writer.writerow(row_data)
            _log_info(f"Successfully recorded item for {data_date} - {endpoint_key} to {PROCESSED_CSV_PATH}")
        except IOError as e:
            _log_error(f"IOError writing to CSV {PROCESSED_CSV_PATH}: {e}")
        except Exception as e:
            _log_error(f"Unexpected error writing to CSV {PROCESSED_CSV_PATH}: {e}")


def check_if_processed(data_date: str, endpoint_key: str) -> bool:
    """
    Checks if a specific data_date and endpoint_key combination has already been
    successfully processed and uploaded.
    This is a basic check and might be slow for large CSV files.
    DuckDB will make this much more efficient.

    Args:
        data_date: The data date (YYYY-MM-DD).
        endpoint_key: The endpoint key.

    Returns:
        True if a "success" record exists, False otherwise.
    """
    with _csv_lock:
        if not os.path.exists(PROCESSED_CSV_PATH):
            return False # File doesn't exist, so nothing processed.

        try:
            with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames != CSV_FIELDNAMES:
                    _log_error(f"CSV headers mismatch in {PROCESSED_CSV_PATH}. Expected {CSV_FIELDNAMES}, got {reader.fieldnames}. Cannot reliably check status.")
                    # This indicates a corrupted or manually altered CSV.
                    # Depending on policy, could return True to prevent reprocessing or False to allow.
                    # Returning False is safer to ensure data isn't missed if file is just empty/new.
                    if not reader.fieldnames: # Empty file
                        return False
                    # If headers are there but different, it's a problem.
                    # For now, let's assume it means we can't confirm, so treat as not processed.
                    return False

                for row in reader:
                    # Check for successful upload. Other statuses might warrant reprocessing.
                    if row.get("data_date") == data_date and \
                       row.get("endpoint_key") == endpoint_key and \
                       row.get("upload_status") == "success":
                        _log_info(f"Found successful record for {data_date} - {endpoint_key} in CSV.")
                        return True
            return False # No matching successful record found
        except FileNotFoundError:
            return False # Should be caught by os.path.exists, but good practice
        except csv.Error as e: # Handles various CSV parsing errors
            _log_error(f"CSV parsing error while checking if processed {PROCESSED_CSV_PATH}: {e}")
            return False # Cannot determine, assume not processed to be safe
        except Exception as e:
            _log_error(f"Unexpected error reading CSV {PROCESSED_CSV_PATH} while checking if processed: {e}")
            return False # Cannot determine, assume not processed


def check_if_checksum_processed(sha256_checksum: str) -> dict | None:
    """
    Checks if a specific SHA256 checksum has already been successfully processed and uploaded.

    Args:
        sha256_checksum: The SHA256 checksum to look for.

    Returns:
        A dictionary containing details of the first found successfully processed item
        (ia_identifier, ia_item_url, data_date, endpoint_key), or None if not found or error.
    """
    if not sha256_checksum:
        _log_error("Checksum not provided for checking.")
        return None

    with _csv_lock:
        if not os.path.exists(PROCESSED_CSV_PATH):
            return None # File doesn't exist, so nothing processed.

        try:
            with open(PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames != CSV_FIELDNAMES:
                    _log_error(f"CSV headers mismatch in {PROCESSED_CSV_PATH}. Expected {CSV_FIELDNAMES}, got {reader.fieldnames}. Cannot reliably check checksum.")
                    return None # Header mismatch means we can't trust the columns

                for row in reader:
                    if row.get("sha256_checksum") == sha256_checksum and \
                       row.get("upload_status") == "success":
                        _log_info(f"Found successful record with checksum {sha256_checksum} in CSV: IA ID {row.get('ia_identifier')}")
                        return {
                            "ia_identifier": row.get("ia_identifier"),
                            "ia_item_url": row.get("ia_item_url"),
                            "data_date": row.get("data_date"),
                            "endpoint_key": row.get("endpoint_key"),
                            "original_checksum": sha256_checksum
                        }
            return None # No matching successful record found
        except FileNotFoundError:
            return None
        except csv.Error as e:
            _log_error(f"CSV parsing error while checking checksum in {PROCESSED_CSV_PATH}: {e}")
            return None
        except Exception as e:
            _log_error(f"Unexpected error reading CSV {PROCESSED_CSV_PATH} while checking checksum: {e}")
            return None


if __name__ == '__main__':
    # Example usage for testing this module directly
    typer.echo("Testing state module (CSV implementation)...")

    # Ensure CSV is created and clear it for a clean test run
    if os.path.exists(PROCESSED_CSV_PATH):
        os.remove(PROCESSED_CSV_PATH)
        typer.echo(f"Removed existing {PROCESSED_CSV_PATH} for a clean test run.")
    ensure_csv_file_and_header()
    typer.echo(f"State CSV file is at: {PROCESSED_CSV_PATH}")

    # Test data
    test_date1 = "2023-01-01"
    test_endpoint1 = "contratacoes_test_state"
    checksum1 = "checksum_test_123"
    test_date2 = "2023-01-02"
    test_endpoint2 = "contratos_test_state"
    checksum2 = "checksum_test_456_failed" # For a failed upload
    test_date3 = "2023-01-03"
    test_endpoint3 = "pca_test_state"
    checksum3 = "checksum_test_123" # Same as checksum1, different date/endpoint


    # 1. Record a successful item
    typer.echo(f"\nRecording a new successful item for {test_date1} - {test_endpoint1} with checksum {checksum1}...")
    record_processed_item(
        data_date=test_date1,
        endpoint_key=test_endpoint1,
        records_fetched=150,
        jsonl_file_path=f"{DATA_OUTPUT_DIR}/pncp-ctrt-{test_date1}.jsonl",
        compressed_file_path=f"{DATA_OUTPUT_DIR}/pncp-ctrt-{test_date1}.jsonl.zst",
        sha256_checksum=checksum1,
        ia_identifier=f"pncp-ctrt-{test_date1}",
        ia_item_url=f"https://archive.org/details/pncp-ctrt-{test_date1}",
        upload_status="success",
        notes="Test entry for successful processing."
    )

    # 2. Record a failed item (different checksum)
    typer.echo(f"\nRecording a new failed item for {test_date2} - {test_endpoint2} with checksum {checksum2}...")
    record_processed_item(
        data_date=test_date2,
        endpoint_key=test_endpoint2,
        records_fetched=0,
        jsonl_file_path=None,
        compressed_file_path=f"{DATA_OUTPUT_DIR}/pncp-ctos-{test_date2}.jsonl.zst",
        sha256_checksum=checksum2,
        ia_identifier=f"pncp-ctos-{test_date2}",
        ia_item_url=None,
        upload_status="failed_upload_error",
        notes="Simulated upload failure."
    )

    # 3. Record another successful item with the SAME checksum as the first, but different date/endpoint
    # This scenario should ideally not happen if the first one was uploaded and this is a true duplicate.
    # However, for testing check_if_checksum_processed, we add it.
    typer.echo(f"\nRecording another successful item for {test_date3} - {test_endpoint3} with checksum {checksum3} (same as first)...")
    record_processed_item(
        data_date=test_date3,
        endpoint_key=test_endpoint3,
        records_fetched=200,
        jsonl_file_path=f"{DATA_OUTPUT_DIR}/pncp-pca-{test_date3}.jsonl",
        compressed_file_path=f"{DATA_OUTPUT_DIR}/pncp-pca-{test_date3}.jsonl.zst",
        sha256_checksum=checksum3, # Same as checksum1
        ia_identifier=f"pncp-pca-{test_date3}",
        ia_item_url=f"https://archive.org/details/pncp-pca-{test_date3}",
        upload_status="success",
        notes="Second test entry with a duplicate checksum but different identifier."
    )


    # 4. Check if items are processed by date/endpoint (original check)
    typer.echo(f"\nChecking date/endpoint status for {test_date1} - {test_endpoint1} (should be True):")
    is_processed1 = check_if_processed(test_date1, test_endpoint1)
    typer.echo(f"Result: {is_processed1}")
    assert is_processed1 is True

    typer.echo(f"\nChecking date/endpoint status for {test_date2} - {test_endpoint2} (should be False as it wasn't 'success'):")
    is_processed2 = check_if_processed(test_date2, test_endpoint2)
    typer.echo(f"Result: {is_processed2}")
    assert is_processed2 is False

    # 5. Check for checksums
    typer.echo(f"\nChecking for checksum {checksum1} (should be found and return details):")
    checksum_info1 = check_if_checksum_processed(checksum1)
    typer.echo(f"Result: {checksum_info1}")
    assert checksum_info1 is not None
    assert checksum_info1["original_checksum"] == checksum1
    assert checksum_info1["ia_identifier"] == f"pncp-ctrt-{test_date1}" # It should find the first one

    typer.echo(f"\nChecking for checksum {checksum2} (associated with a FAILED upload, should NOT be found by this function):")
    checksum_info2 = check_if_checksum_processed(checksum2)
    typer.echo(f"Result: {checksum_info2}")
    assert checksum_info2 is None

    typer.echo(f"\nChecking for a non-existent checksum 'nonexistent123' (should NOT be found):")
    checksum_info_nonexistent = check_if_checksum_processed("nonexistent123")
    typer.echo(f"Result: {checksum_info_nonexistent}")
    assert checksum_info_nonexistent is None

    typer.echo(f"\nChecking for checksum {checksum3} (which is same as checksum1, should return details of first encountered):")
    checksum_info3 = check_if_checksum_processed(checksum3)
    typer.echo(f"Result: {checksum_info3}")
    assert checksum_info3 is not None
    assert checksum_info3["original_checksum"] == checksum3
    # Depending on file write order, this could be the first or third item's IA ID if they share checksums.
    # The test writes them sequentially, so it should find the first one.
    assert checksum_info3["ia_identifier"] == f"pncp-ctrt-{test_date1}"


    typer.echo("\nState module (CSV) test finished.")
    typer.echo(f"Please inspect the content of {PROCESSED_CSV_PATH}")

    # Clean up the test file
    # if os.path.exists(PROCESSED_CSV_PATH):
    #     os.remove(PROCESSED_CSV_PATH)
    #     typer.echo(f"\n{PROCESSED_CSV_PATH} removed for cleanup.")
    # ensure_csv_file_and_header() # Recreate empty for subsequent tests
    # typer.echo("Recreated empty CSV file with header.")
