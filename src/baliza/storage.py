# This module will be responsible for interacting with the Internet Archive
# and local file operations (JSONL, compression, checksum).

import os
import json
import zstandard
import hashlib
import datetime
from internetarchive import upload, get_item # To check if item exists or for other IA interactions
import typer # For logging, will be replaced by logging module later

# Placeholder for actual logging
# Will be replaced by Python's logging module configured for JSON output
def _log_info(message):
    typer.echo(message)

def _log_error(message):
    typer.echo(message, err=True)

def _log_warning(message):
    typer.echo(message, color=typer.colors.YELLOW)


# IA Credentials will be fetched from environment variables within the upload function
# IA_ACCESS_KEY = os.getenv("IA_KEY")
# IA_SECRET_KEY = os.getenv("IA_SECRET")

# Directory for storing generated data files
DATA_OUTPUT_DIR = "baliza_data"
os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)


def create_jsonl_file(records: list, base_filename: str) -> str | None:
    """
    Writes records to a JSONL file.
    Each line in the file is a JSON representation of a record.

    Args:
        records: A list of dictionary records.
        base_filename: The base name for the file (e.g., "pncp-ctrt-YYYY-MM-DD").

    Returns:
        The path to the created JSONL file, or None if an error occurs.
    """
    jsonl_filepath = os.path.join(DATA_OUTPUT_DIR, f"{base_filename}.jsonl")
    _log_info(f"Writing {len(records)} records to {jsonl_filepath}...")
    try:
        with open(jsonl_filepath, "wb") as f_jsonl: # Open in binary mode for utf-8 encoding
            for record in records:
                f_jsonl.write(json.dumps(record, ensure_ascii=False).encode('utf-8') + b"\n")
        _log_info(f"Successfully created JSONL file: {jsonl_filepath}")
        return jsonl_filepath
    except IOError as e:
        _log_error(f"Error writing JSONL file {jsonl_filepath}: {e}")
        return None
    except Exception as e:
        _log_error(f"An unexpected error occurred while creating JSONL file {jsonl_filepath}: {e}")
        return None


def compress_file_zstd(input_filepath: str) -> str | None:
    """
    Compresses a file using Zstandard.
    The compressed file will have a '.zst' extension.

    Args:
        input_filepath: Path to the file to be compressed.

    Returns:
        Path to the compressed file, or None if an error occurs.
    """
    if not os.path.exists(input_filepath):
        _log_error(f"Input file for compression does not exist: {input_filepath}")
        return None

    compressed_filepath = f"{input_filepath}.zst"
    _log_info(f"Compressing {input_filepath} to {compressed_filepath}...")

    cctx = zstandard.ZstdCompressor(level=3) # Level 3 is a good balance of speed and compression
    try:
        with open(input_filepath, "rb") as f_in, open(compressed_filepath, "wb") as f_out:
            cctx.copy_stream(f_in, f_out)
        _log_info(f"Successfully created compressed file: {compressed_filepath}")
        return compressed_filepath
    except Exception as e:
        _log_error(f"Error during Zstandard compression for {input_filepath}: {e}")
        return None


def calculate_sha256_checksum(filepath: str) -> str | None:
    """
    Calculates the SHA256 checksum of a file.

    Args:
        filepath: Path to the file.

    Returns:
        The hex digest of the SHA256 checksum, or None if an error occurs.
    """
    if not os.path.exists(filepath):
        _log_error(f"File for checksum calculation does not exist: {filepath}")
        return None

    _log_info(f"Calculating SHA256 checksum for {filepath}...")
    file_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            while chunk := f.read(8192): # Read in 8KB chunks
                file_hash.update(chunk)
        sha256_hex = file_hash.hexdigest()
        _log_info(f"SHA256 checksum for {filepath}: {sha256_hex}")
        return sha256_hex
    except IOError as e:
        _log_error(f"Error calculating checksum for {filepath}: {e}")
        return None
    except Exception as e:
        _log_error(f"An unexpected error occurred during checksum calculation for {filepath}: {e}")
        return None

def upload_to_internet_archive(
    filepath: str,
    ia_identifier: str,
    ia_title: str,
    day_iso: str, # For metadata
    sha256_checksum: str, # For metadata
    endpoint_key: str, # For metadata, e.g. "contratacoes"
    endpoint_cfg: dict # For metadata, e.g. original source description
) -> dict | None:
    """
    Uploads a file to the Internet Archive.

    Args:
        filepath: Path to the local file to upload.
        ia_identifier: The unique identifier for the item in Internet Archive.
        ia_title: The title for the Internet Archive item.
        day_iso: The date the data pertains to (YYYY-MM-DD).
        sha256_checksum: SHA256 checksum of the file.
        endpoint_key: The key of the PNCP endpoint (e.g., 'contratacoes').
        endpoint_cfg: Configuration for the endpoint.


    Returns:
        A dictionary with upload details (ia_item_url, status) or None if upload fails.
    """
    if not os.path.exists(filepath):
        _log_error(f"File for upload does not exist: {filepath}")
        return {"upload_status": "failed_file_not_found", "ia_identifier": ia_identifier, "ia_item_url": None}

    ia_access_key = os.getenv("IA_KEY")
    ia_secret_key = os.getenv("IA_SECRET")

    if not ia_access_key or not ia_secret_key:
        _log_warning(
            "Internet Archive credentials (IA_KEY, IA_SECRET) not found in environment variables. "
            "Upload to Internet Archive will be skipped. "
            "Please set these variables if you intend to upload."
        )
        return {"upload_status": "skipped_no_credentials", "ia_identifier": ia_identifier, "ia_item_url": f"https://archive.org/details/{ia_identifier}"} # URL is potential

    # Check if item already exists (optional, good for preventing re-uploads if not desired)
    # item = get_item(ia_identifier)
    # if item.exists:
    #     _log_warning(f"Item {ia_identifier} already exists on Internet Archive. Skipping upload.")
    #     # Consider checking if the existing file matches the current checksum if this check is enabled.
    #     return {"upload_status": "skipped_already_exists", "ia_identifier": ia_identifier, "ia_item_url": item.urls.details}

    _log_info(f"Uploading {filepath} to Internet Archive with identifier '{ia_identifier}'...")

    # Recommended metadata structure
    # Using endpoint_cfg.get("ia_title_prefix", endpoint_key) for flexibility
    metadata = {
        "title": ia_title,
        "collection": "opensource_data", # Or a more specific collection if created
        "subject": f"public procurement Brazil; PNCP; {endpoint_key}",
        "creator": "Baliza PNCP Mirror Bot", # As per README
        "language": "pt", # Portuguese
        "date": day_iso, # ISO 8601 date of the data content
        "mediatype": "data", # Or "text" if preferred for JSONL
        "description": f"Daily mirror of {endpoint_cfg.get('ia_title_prefix', endpoint_key)} from Brazil's Portal Nacional de Contratações Públicas (PNCP) for {day_iso}. Raw data in JSON Lines format, compressed with Zstandard (.jsonl.zst). SHA256: {sha256_checksum}.",
        "original_source_api_path": endpoint_cfg.get("api_path", "N/A"),
        "original_data_date": day_iso,
        "sha256": sha256_checksum, # Storing checksum in metadata is good practice
        # "scanner": "Baliza Agent v0.1.0" # Example, if you want to version your bot
    }

    try:
        # The internetarchive library handles retries internally by default (retries=15)
        # We can also specify retries: upload(..., retries=3)
        # The library uploads the file specified by its path.
        # For streaming, one would typically use IA's S3-like interface directly with boto3,
        # but the `internetarchive` library is simpler for direct file uploads.
        # It does not directly expose streaming the same handle used for checksum.
        # So, the flow is: compress -> checksum compressed file -> upload compressed file.
        upload_response = upload(
            identifier=ia_identifier,
            files={os.path.basename(filepath): filepath}, # Dict: {name_on_ia: local_path}
            metadata=metadata,
            access_key=ia_access_key,
            secret_key=ia_secret_key,
            # verbose=True, # For more detailed output from IA library
            retries=5 # Sensible number of retries for IA uploads
        )
        # upload() returns a list of requests.Response objects, one for each file uploaded or metadata update.
        # A successful upload generally means no exceptions were thrown and responses are 2xx.
        # We'll assume success if no exception. A more robust check could inspect response codes.
        # For simplicity, if it doesn't throw, we consider it successful at this stage.
        # A common pattern is to check response status codes if available from the lib.
        # However, internetarchive lib is a bit opaque here. Success is usually no Exception.

        ia_item_url = f"https://archive.org/details/{ia_identifier}"
        _log_info(f"Successfully uploaded {filepath} to Internet Archive. Item URL: {ia_item_url}")
        return {"upload_status": "success", "ia_identifier": ia_identifier, "ia_item_url": ia_item_url}

    except Exception as e:
        # This could be various errors: network, IA server issues, auth, etc.
        _log_error(f"Failed to upload {filepath} to Internet Archive (identifier: {ia_identifier}): {e}")
        # Log the type of exception for better debugging
        _log_error(f"Exception type: {type(e).__name__}")
        # For certain errors (like auth), retrying might not help.
        # The IA library's internal retries should handle transient network issues.
        return {"upload_status": "failed_upload_error", "ia_identifier": ia_identifier, "ia_item_url": f"https://archive.org/details/{ia_identifier}"} # URL is potential


def cleanup_local_file(filepath: str):
    """
    Removes a local file. Typically used for temporary .jsonl files after compression.
    """
    if filepath and os.path.exists(filepath):
        try:
            os.remove(filepath)
            _log_info(f"Cleaned up temporary file: {filepath}")
        except OSError as e:
            _log_error(f"Error removing temporary file {filepath}: {e}")
    elif filepath:
        _log_warning(f"Attempted to clean up non-existent file: {filepath}")


if __name__ == '__main__':
    # Example usage for testing this module directly
    # This will create dummy files and attempt an upload if IA_KEY and IA_SECRET are set.

    typer.echo("Testing storage module functions...")
    test_records = [{"id": 1, "data": "test_data_1"}, {"id": 2, "data": "another_test_value"}]
    test_day = datetime.date.today().isoformat()
    test_base_filename = f"pncp-ctrt-{test_day}-storage_test" # New naming convention + suffix for test
    test_endpoint_key = "contratacoes_storage_test"
    test_endpoint_cfg = {
        "api_path": "/v1/contratacoes/publicacao/test",
        "file_prefix": "pncp-ctrt", # New naming
        "ia_title_prefix": "PNCP Contratações Test (Storage)",
    }
    ia_test_identifier = f"{test_endpoint_cfg['file_prefix']}-{test_day}-storage-test" # Unique for testing
    ia_test_title = f"{test_endpoint_cfg['ia_title_prefix']} {test_day} (Storage Test)"

    # 1. Create JSONL
    jsonl_file = create_jsonl_file(test_records, test_base_filename)
    assert jsonl_file is not None, "JSONL file creation failed"
    typer.echo(f"JSONL file created: {jsonl_file}")

    # 2. Compress JSONL
    compressed_file = compress_file_zstd(jsonl_file)
    assert compressed_file is not None, "File compression failed"
    typer.echo(f"Compressed file created: {compressed_file}")

    # 3. Calculate Checksum
    checksum = calculate_sha256_checksum(compressed_file)
    assert checksum is not None, "Checksum calculation failed"
    typer.echo(f"SHA256 Checksum: {checksum}")

    # 4. Upload to Internet Archive (if credentials are set)
    if os.getenv("IA_KEY") and os.getenv("IA_SECRET"):
        typer.echo("\nAttempting upload to Internet Archive (using test identifier)...")
        upload_result = upload_to_internet_archive(
            filepath=compressed_file,
            ia_identifier=ia_test_identifier,
            ia_title=ia_test_title,
            day_iso=test_day,
            sha256_checksum=checksum,
            endpoint_key=test_endpoint_key,
            endpoint_cfg=test_endpoint_cfg
        )
        typer.echo(f"Upload result: {upload_result}")
        if upload_result and upload_result["upload_status"] == "success":
            typer.echo(f"Test item URL: {upload_result['ia_item_url']}")
            typer.echo(f"IMPORTANT: Remember to manually delete the test item '{ia_test_identifier}' from Internet Archive if it was created.", color=typer.colors.RED)
        elif upload_result and upload_result["upload_status"] == "skipped_no_credentials":
             typer.echo("Upload skipped due to missing credentials, as expected if not set.", color=typer.colors.YELLOW)
        else:
            typer.echo("Upload did not complete successfully or was skipped for other reasons.", err=True)

    else:
        typer.echo("\nIA_KEY and IA_SECRET not set in environment. Skipping Internet Archive upload test.", color=typer.colors.YELLOW)

    # 5. Cleanup
    cleanup_local_file(jsonl_file) # Clean up the uncompressed .jsonl
    # cleanup_local_file(compressed_file) # Usually, we keep the compressed file locally

    typer.echo(f"\nStorage module test finished. Compressed file kept at: {compressed_file}")
    typer.echo("If upload test ran, please check Internet Archive for the test item and delete it.")
