import os
import datetime
import json
import zstandard
import hashlib
import requests
import typer # Changed from argparse
from typing_extensions import Annotated # For Typer argument help
from internetarchive import upload
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError
import csv

PROCESSED_CSV_PATH = os.path.join("state", "processed.csv")

def _write_to_processed_csv(data):
    """Appends a row to the processed.csv file, creating it with headers if it doesn't exist."""
    file_exists = os.path.exists(PROCESSED_CSV_PATH)
    with open(PROCESSED_CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

# Create a Typer app instance
app = typer.Typer(help="BALIZA: Backup Aberto de Licitações Zelando pelo Acesso. Downloads data from PNCP and uploads to Internet Archive.")

# Base URL for PNCP API
BASE_URL = "https://pncp.gov.br/api/consulta"

# Endpoints to fetch data from.
ENDPOINTS_CONFIG = {
    "contratacoes_publicacao": {
        "api_path": "/v1/contratacoes/publicacao",
        "file_prefix": "pncp-contratacoes-publicacao",
        "ia_title_prefix": "PNCP Contratações Publicação",
        "tamanhoPagina": 50,
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "required_params": {} # Needs codigoModalidadeContratacao (long). Assuming API is lenient if omitted.
    },
    "contratacoes_atualizacao": {
        "api_path": "/v1/contratacoes/atualizacao",
        "file_prefix": "pncp-contratacoes-atualizacao",
        "ia_title_prefix": "PNCP Contratações Atualização",
        "tamanhoPagina": 50,
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "required_params": {} # Needs codigoModalidadeContratacao (long). Assuming API is lenient if omitted.
    },
    "contratos_publicacao": {
        "api_path": "/v1/contratos",
        "file_prefix": "pncp-contratos-publicacao",
        "ia_title_prefix": "PNCP Contratos Publicação",
        "tamanhoPagina": 500,
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "required_params": {}
    },
    "contratos_atualizacao": {
        "api_path": "/v1/contratos/atualizacao",
        "file_prefix": "pncp-contratos-atualizacao",
        "ia_title_prefix": "PNCP Contratos Atualização",
        "tamanhoPagina": 500,
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "required_params": {}
    },
    "atas_vigencia": {
        "api_path": "/v1/atas",
        "file_prefix": "pncp-atas-vigencia",
        "ia_title_prefix": "PNCP Atas Vigência",
        "tamanhoPagina": 500,
        "date_param_initial": "dataInicial", # For period of validity
        "date_param_final": "dataFinal",   # For period of validity
        "required_params": {}
    },
    "atas_atualizacao": {
        "api_path": "/v1/atas/atualizacao",
        "file_prefix": "pncp-atas-atualizacao",
        "ia_title_prefix": "PNCP Atas Atualização",
        "tamanhoPagina": 500,
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "required_params": {}
    },
    "instrumentoscobranca_inclusao": {
        "api_path": "/v1/instrumentoscobranca/inclusao",
        "file_prefix": "pncp-instrumentoscobranca-inclusao",
        "ia_title_prefix": "PNCP Instrumentos Cobrança Inclusão",
        "tamanhoPagina": 100,
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "required_params": {}
    },
    "pca_atualizacao": {
        "api_path": "/v1/pca/atualizacao",
        "file_prefix": "pncp-pca-atualizacao",
        "ia_title_prefix": "PNCP PCA Atualização",
        "tamanhoPagina": 500,
        "date_param_initial": "dataInicio", # Note: API uses dataInicio/dataFim
        "date_param_final": "dataFim",
        "required_params": {}
    }
    # Example for another endpoint:
    # "example_endpoint": {
    #     "api_path": "/v1/example/path",
    #     "file_prefix": "pncp-example",
    #     "ia_title_prefix": "PNCP Example",
    #     "tamanhoPagina": 100,
    #     "date_param_initial": "dataInicial",
    #     "date_param_final": "dataFinal",
    #     "required_params": {"param_name": "param_value"}
    # }
}

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(5))
def fetch_data_from_pncp(endpoint_path, params):
    """Fetches data from a PNCP endpoint with retries."""
    try:
        response = requests.get(f"{BASE_URL}{endpoint_path}", params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Using typer.echo for consistent output, though print works fine too
        typer.echo(f"Request failed for {endpoint_path} with params {params}: {e}", err=True)
        raise

def harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg):
    """Harvests all data for a given day and endpoint from PNCP."""
    all_records = []
    current_page = 1

    base_params = {
        "tamanhoPagina": endpoint_cfg["tamanhoPagina"],
        endpoint_cfg["date_param_initial"]: day_iso,
        endpoint_cfg["date_param_final"]: day_iso,
    }
    if "required_params" in endpoint_cfg and endpoint_cfg["required_params"]:
        base_params.update(endpoint_cfg["required_params"])

    # Create a dictionary for initial logging that excludes 'pagina'
    log_params = base_params.copy()
    typer.echo(f"Starting harvest for '{endpoint_key}' on {day_iso} with initial params: {log_params} (excluding page number)...")

    while True:
        request_params = base_params.copy()
        request_params["pagina"] = current_page

        typer.echo(f"Fetching page {current_page} for '{endpoint_key}' with params {request_params}...")
        try:
            data = fetch_data_from_pncp(endpoint_cfg["api_path"], request_params)
        except RetryError as e:
            typer.echo(f"Failed to fetch data for {endpoint_key} page {current_page} after multiple retries: {e}", err=True)
            return None

        if not data or "items" not in data or not data["items"]:
            typer.echo(f"No more items found for '{endpoint_key}' on page {current_page}.")
            break

        all_records.extend(data["items"])

        total_pages = data.get("totalPaginas", 0)
        if not total_pages or current_page >= total_pages: # Handle cases where total_pages might be 0 or missing
            typer.echo(f"Reached last page ({current_page}/{total_pages}) for '{endpoint_key}'.")
            break

        current_page += 1

    typer.echo(f"Harvested {len(all_records)} records for '{endpoint_key}' on {day_iso}.")
    return all_records

def process_and_upload_data(day_iso, endpoint_key, endpoint_cfg, records, run_summary_data):
    """Processes records, saves to a compressed JSONL file, and uploads to Internet Archive."""
    if not records:
        typer.echo(f"No records to process for '{endpoint_key}' on {day_iso}.")
        run_summary_data[endpoint_key]["status"] = "no_data"
        run_summary_data[endpoint_key]["files_generated"] = []
        return

    output_dir = "baliza_data"
    os.makedirs(output_dir, exist_ok=True)

    base_filename = f"{endpoint_cfg['file_prefix']}-{day_iso}"
    jsonl_filename = os.path.join(output_dir, f"{base_filename}.jsonl")
    compressed_filename = os.path.join(output_dir, f"{base_filename}.jsonl.zst")

    file_details = {
        "jsonl_file": jsonl_filename,
        "compressed_file": compressed_filename,
        "sha256_checksum": None,
        "ia_identifier": None,
        "ia_item_url": None,
        "upload_status": "pending"
    }

    typer.echo(f"Writing records to {jsonl_filename} and compressing to {compressed_filename}...")
    try:
        with open(jsonl_filename, "wb") as f_jsonl:
            for record in records:
                f_jsonl.write(json.dumps(record, ensure_ascii=False).encode('utf-8') + b"\n")

        cctx = zstandard.ZstdCompressor(level=3)
        with open(jsonl_filename, "rb") as f_in, open(compressed_filename, "wb") as f_out:
            cctx.copy_stream(f_in, f_out)
        typer.echo(f"Successfully created compressed file: {compressed_filename}")
    except Exception as e:
        typer.echo(f"Error during file writing or compression for {endpoint_key}: {e}", err=True)
        run_summary_data[endpoint_key]["status"] = "file_error"
        file_details["upload_status"] = "failed_file_error"
        run_summary_data[endpoint_key]["files_generated"].append(file_details)
        return

    file_hash = hashlib.sha256()
    try:
        with open(compressed_filename, "rb") as f:
            while chunk := f.read(8192):
                file_hash.update(chunk)
        sha256_checksum = file_hash.hexdigest()
        file_details["sha256_checksum"] = sha256_checksum
        typer.echo(f"SHA256 checksum for {compressed_filename}: {sha256_checksum}")
    except IOError as e:
        typer.echo(f"Error calculating checksum for {compressed_filename}: {e}", err=True)
        run_summary_data[endpoint_key]["status"] = "checksum_error"
        file_details["upload_status"] = "failed_checksum_error"
        run_summary_data[endpoint_key]["files_generated"].append(file_details)
        return

    ia_identifier = f"{endpoint_cfg['file_prefix']}-{day_iso}"
    file_details["ia_identifier"] = ia_identifier
    file_details["ia_item_url"] = f"https://archive.org/details/{ia_identifier}"


    metadata = {
        "title": f"{endpoint_cfg['ia_title_prefix']} {day_iso}",
        "collection": "opensource_data",
        "subject": "public procurement Brazil; PNCP",
        "creator": "Baliza PNCP Mirror Bot",
        "language": "pt",
        "date": day_iso,
        "sha256": sha256_checksum,
        "original_source": "Portal Nacional de Contratações Públicas (PNCP)",
        "description": f"Daily mirror of {endpoint_cfg['ia_title_prefix']} from Brazil's PNCP for {day_iso}. Raw data in JSONL format, compressed with Zstandard."
    }

    ia_access_key = os.getenv("IA_KEY")
    ia_secret_key = os.getenv("IA_SECRET")

    try:
        if not ia_access_key or not ia_secret_key:
            typer.echo("Internet Archive credentials (IA_KEY, IA_SECRET) not found. Skipping upload.", err=True)
            file_details["upload_status"] = "skipped_no_credentials"
            run_summary_data[endpoint_key]["status"] = "upload_skipped"
            # Upload will not be attempted
        else:
            typer.echo(f"Uploading {compressed_filename} to Internet Archive with identifier '{ia_identifier}'...")
            upload(
                identifier=ia_identifier,
                files={os.path.basename(compressed_filename): compressed_filename},
                metadata=metadata,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                retries=3,
            )
            typer.echo(f"Successfully uploaded {compressed_filename} to Internet Archive.")
            file_details["upload_status"] = "success"
            run_summary_data[endpoint_key]["status"] = "success"

    except Exception as e:
        # This catches exceptions from the upload block, or if other unexpected error happens before/during upload decision
        # For instance, if getenv itself failed, though unlikely.
        # More relevant for the actual upload() call.
        if file_details["upload_status"] == "pending": # Only update if not already set to skipped
            typer.echo(f"Failed to upload {compressed_filename} to Internet Archive: {e}", err=True)
            file_details["upload_status"] = "failed_upload_error"
            run_summary_data[endpoint_key]["status"] = "upload_failed"
        elif file_details["upload_status"] == "skipped_no_credentials":
            # This means the error happened somewhere outside the direct upload call, but after skip decision
            typer.echo(f"An error occurred after deciding to skip upload: {e}", err=True)
            # Keep status as skipped, but log error.
        else: # Should not happen if logic is correct (e.g. success path)
            typer.echo(f"An unexpected error occurred during upload/post-upload phase: {e}", err=True)
            file_details["upload_status"] = "failed_unknown_error" # Generic error status
            run_summary_data[endpoint_key]["status"] = "upload_failed"

    finally:
        # This block executes regardless of upload success, failure, or skip.
        run_summary_data[endpoint_key]["files_generated"].append(file_details)

        _write_to_processed_csv({
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "data_date": day_iso,
            "endpoint_key": endpoint_key,
            "compressed_file": compressed_filename,
            "sha256_checksum": file_details["sha256_checksum"], # Should be valid unless checksum failed
            "ia_identifier": file_details["ia_identifier"],
            "ia_item_url": file_details["ia_item_url"],
            "upload_status": file_details["upload_status"], # Critical: this must reflect the final outcome
            "records_fetched": len(records) if records else 0
        })

        # Clean up the temporary JSONL file
        if os.path.exists(jsonl_filename): # Check existence, as prior steps might have failed
            try:
                os.remove(jsonl_filename)
                typer.echo(f"Cleaned up temporary file: {jsonl_filename}")
            except OSError as e:
                typer.echo(f"Error removing temporary file {jsonl_filename}: {e}", err=True)
        else:
            typer.echo(f"Temporary file {jsonl_filename} not found for cleanup (may indicate earlier error).")


# Typer command decorator
@app.command()
def run_baliza(
    date_str: Annotated[str, typer.Option(..., help="The date to fetch data for, in YYYY-MM-DD format.", envvar="BALIZA_DATE")]
):
    """
    Main command to run the Baliza fetching and archiving process for a specific date.
    """
    try:
        target_day_iso = datetime.datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
    except ValueError:
        typer.echo("Error: Date must be in YYYY-MM-DD format.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"BALIZA process starting for date: {target_day_iso}")

    # Structured summary data for this run
    run_summary = {
        "run_date_iso": datetime.datetime.utcnow().isoformat(),
        "target_data_date": target_day_iso,
        "overall_status": "pending",
        "endpoints": {}
    }

    all_endpoints_successful = True

    for endpoint_key, endpoint_config in ENDPOINTS_CONFIG.items():
        typer.echo(f"\nProcessing endpoint: {endpoint_key}")
        run_summary["endpoints"][endpoint_key] = {
            "status": "pending",
            "records_fetched": 0,
            "files_generated": [] # List to hold details of generated files
        }

        records = harvest_endpoint_data(target_day_iso, endpoint_key, endpoint_config)

        if records is not None:
            run_summary["endpoints"][endpoint_key]["records_fetched"] = len(records)
            process_and_upload_data(target_day_iso, endpoint_key, endpoint_config, records, run_summary["endpoints"])
            if run_summary["endpoints"][endpoint_key]["status"] not in ["success", "no_data", "upload_skipped"]: # "upload_skipped" is not a hard failure for the script itself
                all_endpoints_successful = False
        else:
            typer.echo(f"Skipping processing and upload for '{endpoint_key}' due to harvesting errors.", err=True)
            run_summary["endpoints"][endpoint_key]["status"] = "harvest_failed"
            all_endpoints_successful = False

    if all_endpoints_successful and any(e["status"] == "success" for e in run_summary["endpoints"].values()):
         run_summary["overall_status"] = "success"
    elif any(e["status"] == "upload_skipped" for e in run_summary["endpoints"].values()) and \
         all(e["status"] in ["success", "upload_skipped", "no_data"] for e in run_summary["endpoints"].values()):
        run_summary["overall_status"] = "completed_upload_skipped"
    else:
        run_summary["overall_status"] = "failed"


    # Output the structured summary as a JSON line to stdout
    # This can be captured by GitHub Actions or other tools for Phase 2 processing.
    typer.echo("\n--- BALIZA RUN SUMMARY ---")
    typer.echo(json.dumps(run_summary))
    typer.echo("--- END BALIZA RUN SUMMARY ---\n")

    typer.echo(f"BALIZA process finished for date: {target_day_iso} with overall status: {run_summary['overall_status']}")

    if run_summary["overall_status"] == "failed":
        raise typer.Exit(code=1) # Exit with error code if any critical part failed

if __name__ == "__main__":
    app()
