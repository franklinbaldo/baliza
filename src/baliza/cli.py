# This module will be the new entry point for the Typer application.
# It will import and use functions from the other new modules (client, pipeline, storage, state).

import datetime
import json
import os
import typer
from typing_extensions import Annotated
import csv # Added for list_processed command

# Assuming new modules are in the same package (src/baliza/)
from . import pipeline
from . import storage
from . import state # For recording final status (CSV initially, then DuckDB)
# client.py is used by pipeline.py, so direct import here might not be needed for run_baliza

# --- Configuration ---
# This would typically be loaded from a config file or env vars in a larger app
# For now, mirroring the structure from the old main.py
ENDPOINTS_CONFIG = {
    "contratacoes": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-ctrt", # Updated naming
        "ia_title_prefix": "PNCP Contratações", # For IA item title
        "page_size": 500 # Default, can be overridden per endpoint
    }
    # Example for another endpoint (when it's added):
    # "contratos": {
    #     "api_path": "/v1/contratos/publicacao",
    #     "file_prefix": "pncp-ctos", # Updated naming
    #     "ia_title_prefix": "PNCP Contratos",
    # }
}

# Create a Typer app instance
# help message from original main.py
app = typer.Typer(
    help="BALIZA: Backup Aberto de Licitações Zelando pelo Acesso. Downloads data from PNCP and uploads to Internet Archive.",
    name="baliza" # Explicitly setting the command name for clarity
)

# Placeholder for structured logging - will be replaced in Week 3
# For now, using typer.echo for simplicity and visibility
def _log_info(message: str):
    typer.echo(f"INFO: {message}")

def _log_error(message: str):
    typer.echo(f"ERROR: {message}", err=True)

def _log_summary(summary_data: dict):
    """Outputs the run summary as JSON."""
    typer.echo("\n--- BALIZA RUN SUMMARY ---")
    typer.echo(json.dumps(summary_data, indent=2, ensure_ascii=False)) # Pretty print for readability
    typer.echo("--- END BALIZA RUN SUMMARY ---\n")


@app.command()
def run_baliza(
    date_str: Annotated[str, typer.Option(
        ..., # Ellipsis means this argument is required
        help="The date to fetch data for, in YYYY-MM-DD format.",
        envvar="BALIZA_DATE" # Can also be set via environment variable
    )]
):
    """
    Main command to run the Baliza fetching, processing, and archiving pipeline for a specific date.
    """
    try:
        target_day_iso = datetime.datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
    except ValueError:
        _log_error("Date must be in YYYY-MM-DD format.")
        raise typer.Exit(code=1)

    _log_info(f"BALIZA process starting for target data date: {target_day_iso}")

    run_summary = {
        "run_start_time_utc": datetime.datetime.utcnow().isoformat(),
        "target_data_date": target_day_iso,
        "overall_status": "pending", # Will be updated to "success", "partial_success", or "failed"
        "endpoints_processed": {}
    }

    # Check if data for this date and all configured endpoints have already been successfully processed
    # This is a simplified check. A more robust check might look at individual endpoints.
    # For now, if ALL are done, we can offer a skip.
    # This check will be more refined with DuckDB state.
    # For now, we'll process regardless and let state.record_processed_item handle individual logs.
    # Later, we might add a --force option to override this.

    all_endpoints_globally_successful_previously = True
    if not ENDPOINTS_CONFIG:
        _log_error("No endpoints configured. Exiting.")
        run_summary["overall_status"] = "config_error"
        _log_summary(run_summary)
        raise typer.Exit(code=1)

    for endpoint_key in ENDPOINTS_CONFIG.keys():
        if not state.check_if_processed(target_day_iso, endpoint_key):
            all_endpoints_globally_successful_previously = False
            break

    if all_endpoints_globally_successful_previously:
        _log_info(f"All configured endpoints for date {target_day_iso} appear to have been successfully processed already. Skipping run.")
        # To re-run, user would need to modify state or use a future --force option.
        run_summary["overall_status"] = "skipped_all_done"
        run_summary["run_end_time_utc"] = datetime.datetime.utcnow().isoformat()
        _log_summary(run_summary)
        # typer.Exit(code=0) # Exiting cleanly as this is not an error.
        return # Exit the command cleanly


    # --- Main Processing Loop ---
    final_overall_status = "success" # Assume success, downgrade on any failure

    for endpoint_key, config in ENDPOINTS_CONFIG.items():
        _log_info(f"\nProcessing endpoint: '{endpoint_key}' for date {target_day_iso}")

        endpoint_summary = {
            "status": "pending",
            "records_fetched": 0,
            "jsonl_file": None,
            "compressed_file": None,
            "sha256_checksum": None,
            "ia_identifier": None,
            "ia_item_url": None,
            "upload_status": "not_attempted",
            "start_time_utc": datetime.datetime.utcnow().isoformat(),
            "end_time_utc": None,
            "error_message": None
        }
        run_summary["endpoints_processed"][endpoint_key] = endpoint_summary

        # 1. Check if this specific endpoint for this date was already successfully processed
        if state.check_if_processed(target_day_iso, endpoint_key):
            _log_info(f"Endpoint '{endpoint_key}' for date {target_day_iso} already successfully processed. Skipping.")
            endpoint_summary["status"] = "skipped_previously_successful"
            endpoint_summary["upload_status"] = "skipped_previously_successful" # Match more closely
            endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
            # Not changing final_overall_status here, as skipping is not a failure.
            state.record_processed_item( # Log that we skipped it again, or update existing. For now, just log.
                data_date=target_day_iso,
                endpoint_key=endpoint_key,
                records_fetched=0, # No new records
                jsonl_file_path=None,
                compressed_file_path="N/A (skipped)",
                sha256_checksum="N/A (skipped)",
                ia_identifier="N/A (skipped)",
                ia_item_url="N/A (skipped)",
                upload_status="skipped_previously_successful",
                notes=f"Skipped in current run as previous successful record found at {endpoint_summary['start_time_utc']}"
            )
            continue # Move to the next endpoint

        # 2. Harvest data using the pipeline module
        _log_info(f"Harvesting data for '{endpoint_key}'...")
        records = pipeline.harvest_endpoint_data(target_day_iso, endpoint_key, config)

        if records is None:
            _log_error(f"Harvesting failed for '{endpoint_key}'.")
            endpoint_summary["status"] = "harvest_failed"
            endpoint_summary["error_message"] = f"pipeline.harvest_endpoint_data returned None for {endpoint_key}"
            endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
            state.record_processed_item(
                data_date=target_day_iso, endpoint_key=endpoint_key, records_fetched=0,
                jsonl_file_path=None, compressed_file_path=None, sha256_checksum=None,
                ia_identifier=None, ia_item_url=None, upload_status="harvest_failed",
                notes=endpoint_summary["error_message"]
            )
            final_overall_status = "failed" # A part of the process failed critically
            continue # Move to next endpoint

        endpoint_summary["records_fetched"] = len(records)
        if not records:
            _log_info(f"No records found for '{endpoint_key}' on {target_day_iso}. Nothing to process or upload.")
            endpoint_summary["status"] = "no_data_found"
            endpoint_summary["upload_status"] = "skipped_no_data"
            endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
            state.record_processed_item(
                data_date=target_day_iso, endpoint_key=endpoint_key, records_fetched=0,
                jsonl_file_path=None, compressed_file_path=None, sha256_checksum=None,
                ia_identifier=None, ia_item_url=None, upload_status="no_data_found",
                notes="No records returned from PNCP for this date/endpoint combination."
            )
            # Not a failure, so final_overall_status is not changed to "failed" for this specific case.
            continue

        # 3. Process and store data using the storage module
        base_filename = f"{config['file_prefix']}-{target_day_iso}" # e.g., pncp-ctrt-2023-01-01

        # 3a. Create JSONL file
        _log_info(f"Creating JSONL file for '{endpoint_key}'...")
        jsonl_filepath = storage.create_jsonl_file(records, base_filename)
        if not jsonl_filepath:
            _log_error(f"Failed to create JSONL file for '{endpoint_key}'.")
            endpoint_summary["status"] = "jsonl_creation_failed"
            endpoint_summary["error_message"] = f"storage.create_jsonl_file returned None for {base_filename}"
            endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
            state.record_processed_item(
                data_date=target_day_iso, endpoint_key=endpoint_key, records_fetched=len(records),
                jsonl_file_path=None, compressed_file_path=None, sha256_checksum=None,
                ia_identifier=None, ia_item_url=None, upload_status="jsonl_creation_failed",
                notes=endpoint_summary["error_message"]
            )
            final_overall_status = "failed"
            continue
        endpoint_summary["jsonl_file"] = jsonl_filepath

        # 3b. Compress file
        _log_info(f"Compressing JSONL file for '{endpoint_key}'...")
        compressed_filepath = storage.compress_file_zstd(jsonl_filepath)
        if not compressed_filepath:
            _log_error(f"Failed to compress file for '{endpoint_key}'.")
            endpoint_summary["status"] = "compression_failed"
            endpoint_summary["error_message"] = f"storage.compress_file_zstd returned None for {jsonl_filepath}"
            endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
            state.record_processed_item(
                data_date=target_day_iso, endpoint_key=endpoint_key, records_fetched=len(records),
                jsonl_file_path=jsonl_filepath, compressed_file_path=None, sha256_checksum=None,
                ia_identifier=None, ia_item_url=None, upload_status="compression_failed",
                notes=endpoint_summary["error_message"]
            )
            final_overall_status = "failed"
            storage.cleanup_local_file(jsonl_filepath) # Clean up raw jsonl
            continue
        endpoint_summary["compressed_file"] = compressed_filepath

        # 3c. Calculate checksum (of compressed file)
        _log_info(f"Calculating SHA256 checksum for '{endpoint_key}'...")
        sha256_hex = storage.calculate_sha256_checksum(compressed_filepath)
        if not sha256_hex:
            _log_error(f"Failed to calculate checksum for '{endpoint_key}'.")
            endpoint_summary["status"] = "checksum_failed"
            endpoint_summary["error_message"] = f"storage.calculate_sha256_checksum returned None for {compressed_filepath}"
            endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
            state.record_processed_item(
                data_date=target_day_iso, endpoint_key=endpoint_key, records_fetched=len(records),
                jsonl_file_path=jsonl_filepath, compressed_file_path=compressed_filepath, sha256_checksum=None,
                ia_identifier=None, ia_item_url=None, upload_status="checksum_failed",
                notes=endpoint_summary["error_message"]
            )
            final_overall_status = "failed"
            storage.cleanup_local_file(jsonl_filepath) # Clean up raw jsonl
            continue
        endpoint_summary["sha256_checksum"] = sha256_hex

        # 4. Upload to Internet Archive using storage module
        ia_identifier = f"{config['file_prefix']}-{target_day_iso}" # e.g., pncp-ctrt-2023-01-01
        ia_title = f"{config['ia_title_prefix']} {target_day_iso}"  # e.g., PNCP Contratações 2023-01-01
        endpoint_summary["ia_identifier"] = ia_identifier

        _log_info(f"Attempting to upload '{compressed_filepath}' to Internet Archive as '{ia_identifier}'...")
        upload_details = storage.upload_to_internet_archive(
            filepath=compressed_filepath,
            ia_identifier=ia_identifier,
            ia_title=ia_title,
            day_iso=target_day_iso,
            sha256_checksum=sha256_hex,
            endpoint_key=endpoint_key,
            endpoint_cfg=config
        )

        # Update endpoint summary with upload details
        if upload_details:
            endpoint_summary["upload_status"] = upload_details.get("upload_status", "unknown_ia_response")
            endpoint_summary["ia_item_url"] = upload_details.get("ia_item_url")
            if upload_details["upload_status"] == "success":
                endpoint_summary["status"] = "success"
                _log_info(f"Successfully processed and uploaded data for '{endpoint_key}'. IA URL: {endpoint_summary['ia_item_url']}")
            elif upload_details["upload_status"].startswith("skipped_"):
                 endpoint_summary["status"] = "upload_skipped" # Not a hard failure for the overall process
                 _log_info(f"Upload skipped for '{endpoint_key}': {upload_details['upload_status']}")
                 # If skipped due to no credentials, it's a soft failure for this endpoint
                 if upload_details["upload_status"] == "skipped_no_credentials" and final_overall_status != "failed":
                     final_overall_status = "partial_success" # Downgrade overall if not already failed
            else: # various failure modes
                endpoint_summary["status"] = "upload_failed"
                endpoint_summary["error_message"] = f"Upload failed with status: {upload_details['upload_status']}"
                _log_error(f"Upload failed for '{endpoint_key}': {upload_details['upload_status']}")
                final_overall_status = "failed" # Critical failure for this endpoint
        else:
            # This case should ideally be handled by upload_to_internet_archive returning a dict
            _log_error(f"Upload function returned None for '{endpoint_key}', which is unexpected.")
            endpoint_summary["status"] = "upload_failed"
            endpoint_summary["upload_status"] = "failed_unexpected_none_response"
            endpoint_summary["error_message"] = "storage.upload_to_internet_archive returned None"
            final_overall_status = "failed"

        # 5. Record state
        state.record_processed_item(
            data_date=target_day_iso,
            endpoint_key=endpoint_key,
            records_fetched=len(records),
            jsonl_file_path=jsonl_filepath,
            compressed_file_path=compressed_filepath,
            sha256_checksum=sha256_hex,
            ia_identifier=ia_identifier,
            ia_item_url=endpoint_summary["ia_item_url"],
            upload_status=endpoint_summary["upload_status"],
            notes=endpoint_summary["error_message"] or ""
        )

        # 6. Cleanup local JSONL file (compressed file is kept)
        storage.cleanup_local_file(jsonl_filepath)
        endpoint_summary["end_time_utc"] = datetime.datetime.utcnow().isoformat()
        _log_info(f"Finished processing endpoint: '{endpoint_key}'. Status: {endpoint_summary['status']}")


    # --- Finalize Run ---
    run_summary["overall_status"] = final_overall_status
    run_summary["run_end_time_utc"] = datetime.datetime.utcnow().isoformat()
    _log_summary(run_summary)

    _log_info(f"BALIZA process finished for date: {target_day_iso}. Overall status: {run_summary['overall_status']}")

    if run_summary["overall_status"] == "failed":
        raise typer.Exit(code=1)
    elif run_summary["overall_status"] == "partial_success": # e.g. upload skipped due to missing creds
        # Could also use a different exit code for partial success if needed by CI/CD
        raise typer.Exit(code=0) # For now, treat as non-fatal for the runner
    else: # success or skipped_all_done
        raise typer.Exit(code=0)


# --- Additional CLI Commands (for Week 2) ---
@app.command()
def list_processed(
    status: Annotated[str, typer.Option(help="Filter by upload status (e.g., success, failed).")] = None,
    date: Annotated[str, typer.Option(help="Filter by data date (YYYY-MM-DD).")] = None,
    endpoint: Annotated[str, typer.Option(help="Filter by endpoint key.")] = None
):
    """
    Lists processed items from the state file/database.
    (Placeholder - to be fully implemented in Week 2 with DuckDB)
    """
    _log_info("Listing processed items (CSV placeholder)...")
    if not os.path.exists(state.PROCESSED_CSV_PATH):
        _log_info(f"State file {state.PROCESSED_CSV_PATH} not found.")
        return

    with open(state.PROCESSED_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            # Basic filtering for demonstration
            display_row = True
            if status and row.get("upload_status") != status:
                display_row = False
            if date and row.get("data_date") != date:
                display_row = False
            if endpoint and row.get("endpoint_key") != endpoint:
                display_row = False

            if display_row:
                typer.echo(f"- Date: {row['data_date']}, Endpoint: {row['endpoint_key']}, Status: {row['upload_status']}, IA ID: {row['ia_identifier']}")
                count +=1
        if count == 0:
            _log_info("No records found matching your criteria.")
        else:
            _log_info(f"Found {count} matching records.")


@app.command()
def reprocess(
    identifier: Annotated[str, typer.Argument(help="The identifier for the item to reprocess. Can be 'YYYY-MM-DD' to reprocess all endpoints for a date, or 'YYYY-MM-DD/endpoint_key' for a specific endpoint.")]
):
    """
    Marks an item for reprocessing.
    (Placeholder - to be fully implemented in Week 2 with DuckDB)
    This command would typically modify the state (e.g., remove or flag an entry in DuckDB).
    For CSV, it's harder to safely modify; this might just involve user guidance for now.
    """
    _log_info(f"Reprocessing command called for: {identifier}")
    _log_info("Reprocessing logic (modifying CSV state) is complex and not fully implemented in this CSV-based version.")
    _log_info("For now, to reprocess, you might need to manually remove the corresponding successful entry from:")
    _log_info(f"{state.PROCESSED_CSV_PATH}")
    _log_info("Then, re-run `baliza run-baliza --date YYYY-MM-DD` for the desired date.")
    _log_info("Full 'reprocess' functionality will be available with DuckDB state management in Week 2.")


if __name__ == "__main__":
    # This allows running the CLI directly, e.g., `python src/baliza/cli.py run-baliza --date YYYY-MM-DD`
    # Ensure environment variables like IA_KEY, IA_SECRET, BALIZA_DATE are set if needed.
    # Example:
    # export IA_KEY="your_ia_key"
    # export IA_SECRET="your_ia_secret"
    # python src/baliza/cli.py run-baliza --date 2023-10-01
    # python src/baliza/cli.py list-processed --status success
    app()
