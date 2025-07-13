import os
import datetime
import json
import hashlib
import requests
import typer
from typing_extensions import Annotated
from internetarchive import upload
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError
import concurrent.futures
import time
import duckdb
from .ia_federation import InternetArchiveFederation

BALIZA_DB_PATH = os.path.join("state", "baliza.duckdb")

def _init_baliza_db():
    """Initialize Baliza database with PSA (Persistent Staging Area) and control tables."""
    os.makedirs("state", exist_ok=True)
    conn = duckdb.connect(BALIZA_DB_PATH)
    
    # Check if we should update IA federation
    _ensure_ia_federation_updated()
    
    # PSA Schema: Raw data staging tables
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS psa
    """)
    
    # PSA: Raw contratos data (all historical data)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS psa.contratos_raw (
            -- Data collection metadata
            baliza_extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            baliza_data_date DATE,
            baliza_endpoint VARCHAR,
            baliza_run_id VARCHAR,
            
            -- PNCP API raw fields (flexible schema)
            numeroControlePncpCompra VARCHAR,
            codigoPaisFornecedor VARCHAR,
            anoContrato INTEGER,
            numeroContratoEmpenho VARCHAR,
            dataAssinatura VARCHAR,
            dataVigenciaInicio VARCHAR,
            dataVigenciaFim VARCHAR,
            niFornecedor VARCHAR,
            tipoPessoa VARCHAR,
            nomeRazaoSocialFornecedor VARCHAR,
            objetoContrato VARCHAR,
            valorInicial DOUBLE,
            valorParcela DOUBLE,
            valorGlobal DOUBLE,
            valorAcumulado DOUBLE,
            
            -- JSON blob for complex nested fields
            tipoContrato_json VARCHAR,
            orgaoEntidade_json VARCHAR,
            categoriaProcesso_json VARCHAR,
            unidadeOrgao_json VARCHAR,
            
            -- All other fields as JSON for flexibility
            raw_data_json VARCHAR
        )
    """)
    
    # Control Schema: Metadata and tracking
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS control
    """)
    
    # Control table for tracking executions
    conn.execute("""
        CREATE TABLE IF NOT EXISTS control.runs (
            run_id VARCHAR PRIMARY KEY,
            timestamp TIMESTAMP,
            data_date DATE,
            endpoint_key VARCHAR,
            parquet_file VARCHAR,
            sha256_checksum VARCHAR,
            ia_identifier VARCHAR,
            ia_item_url VARCHAR,
            upload_status VARCHAR,
            records_fetched INTEGER,
            file_size_bytes BIGINT,
            psa_loaded BOOLEAN DEFAULT FALSE,
            psa_records_inserted INTEGER DEFAULT 0
        )
    """)
    
    # Data quality control table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS control.data_quality (
            run_id VARCHAR,
            table_name VARCHAR,
            check_name VARCHAR,
            check_result VARCHAR,
            check_value DOUBLE,
            check_threshold DOUBLE,
            check_status VARCHAR, -- PASS/FAIL/WARNING
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes for performance
    conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_date_endpoint ON control.runs(data_date, endpoint_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_contratos_raw_date ON psa.contratos_raw(baliza_data_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_contratos_raw_numero ON psa.contratos_raw(numeroControlePncpCompra)")
    
    conn.close()

def _ensure_ia_federation_updated():
    """Ensure Internet Archive federation is updated with latest data."""
    try:
        # Check if federation should be updated (daily)
        update_flag_file = "state/ia_federation_last_update"
        should_update = True
        
        if os.path.exists(update_flag_file):
            with open(update_flag_file, 'r') as f:
                last_update = f.read().strip()
                if last_update == datetime.date.today().isoformat():
                    should_update = False
        
        if should_update:
            typer.echo("🔄 Updating Internet Archive federation...")
            federation = InternetArchiveFederation(BALIZA_DB_PATH)
            federation.update_federation()
            
            # Mark as updated today
            with open(update_flag_file, 'w') as f:
                f.write(datetime.date.today().isoformat())
            
            typer.echo("✅ IA federation updated")
        
    except Exception as e:
        typer.echo(f"⚠️ Warning: Could not update IA federation: {e}", err=True)
        # Continue execution - federation is optional enhancement

def _check_existing_data_in_ia(day_iso: str, endpoint_key: str) -> bool:
    """Check if data for this day already exists in Internet Archive."""
    try:
        federation = InternetArchiveFederation(BALIZA_DB_PATH)
        
        # Check IA catalog for existing data
        conn = duckdb.connect(BALIZA_DB_PATH)
        
        # Try to check if federated views exist
        existing = conn.execute("""
            SELECT COUNT(*) 
            FROM federated.contratos_ia 
            WHERE ia_data_date = ? 
            LIMIT 1
        """, [day_iso]).fetchone()
        
        conn.close()
        
        if existing and existing[0] > 0:
            typer.echo(f"📦 Data for {day_iso} already available in Internet Archive")
            return True
            
    except Exception as e:
        # If federation not available, continue with normal processing
        typer.echo(f"⚠️ Could not check IA data availability: {e}", err=True)
    
    return False

def _generate_run_id(data_date, endpoint_key):
    """Generate unique run ID."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{endpoint_key}_{data_date}_{timestamp}"

def _load_to_psa(run_id, data_date, endpoint_key, records):
    """Load raw records to PSA (Persistent Staging Area)."""
    if not records:
        return 0
    
    conn = duckdb.connect(BALIZA_DB_PATH)
    
    try:
        # Prepare records for PSA insertion
        psa_records = []
        for record in records:
            # Extract main fields safely
            psa_record = {
                'baliza_run_id': run_id,
                'baliza_data_date': data_date,
                'baliza_endpoint': endpoint_key,
                'numeroControlePncpCompra': record.get('numeroControlePncpCompra'),
                'codigoPaisFornecedor': record.get('codigoPaisFornecedor'),
                'anoContrato': record.get('anoContrato'),
                'numeroContratoEmpenho': record.get('numeroContratoEmpenho'),
                'dataAssinatura': record.get('dataAssinatura'),
                'dataVigenciaInicio': record.get('dataVigenciaInicio'),
                'dataVigenciaFim': record.get('dataVigenciaFim'),
                'niFornecedor': record.get('niFornecedor'),
                'tipoPessoa': record.get('tipoPessoa'),
                'nomeRazaoSocialFornecedor': record.get('nomeRazaoSocialFornecedor'),
                'objetoContrato': record.get('objetoContrato'),
                'valorInicial': record.get('valorInicial'),
                'valorParcela': record.get('valorParcela'),
                'valorGlobal': record.get('valorGlobal'),
                'valorAcumulado': record.get('valorAcumulado'),
                
                # Store complex nested objects as JSON
                'tipoContrato_json': json.dumps(record.get('tipoContrato')) if record.get('tipoContrato') else None,
                'orgaoEntidade_json': json.dumps(record.get('orgaoEntidade')) if record.get('orgaoEntidade') else None,
                'categoriaProcesso_json': json.dumps(record.get('categoriaProcesso')) if record.get('categoriaProcesso') else None,
                'unidadeOrgao_json': json.dumps(record.get('unidadeOrgao')) if record.get('unidadeOrgao') else None,
                
                # Store full record as JSON for completeness
                'raw_data_json': json.dumps(record, ensure_ascii=False)
            }
            psa_records.append(psa_record)
        
        # Bulk insert to PSA
        placeholders = ', '.join(['?' for _ in psa_records[0].keys()])
        columns = ', '.join(psa_records[0].keys())
        
        values = []
        for record in psa_records:
            values.append(list(record.values()))
        
        conn.executemany(f"""
            INSERT INTO psa.contratos_raw ({columns})
            VALUES ({placeholders})
        """, values)
        
        records_inserted = len(psa_records)
        typer.echo(f"Loaded {records_inserted} records to PSA for run {run_id}")
        
        conn.close()
        return records_inserted
        
    except Exception as e:
        typer.echo(f"Error loading to PSA: {e}", err=True)
        conn.close()
        return 0

def _log_run_to_db(data):
    """Log run data to control.runs table."""
    conn = duckdb.connect(BALIZA_DB_PATH)
    
    # Get file size if file exists
    file_size = 0
    if data.get("parquet_file") and os.path.exists(data["parquet_file"]):
        file_size = os.path.getsize(data["parquet_file"])
    
    conn.execute("""
        INSERT INTO control.runs (
            run_id, timestamp, data_date, endpoint_key, parquet_file, 
            sha256_checksum, ia_identifier, ia_item_url, 
            upload_status, records_fetched, file_size_bytes,
            psa_loaded, psa_records_inserted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        data.get("run_id"),
        data["timestamp"],
        data["data_date"], 
        data["endpoint_key"],
        data.get("parquet_file"),
        data.get("sha256_checksum"),
        data.get("ia_identifier"),
        data.get("ia_item_url"),
        data["upload_status"],
        data["records_fetched"],
        file_size,
        data.get("psa_loaded", False),
        data.get("psa_records_inserted", 0)
    ])
    
    conn.close()

# Create a Typer app instance
app = typer.Typer(help="BALIZA: Backup Aberto de Licitações Zelando pelo Acesso. Downloads data from PNCP and uploads to Internet Archive.")

# Base URL for PNCP API
BASE_URL = "https://pncp.gov.br/api/consulta"

# Endpoints to fetch data from.
# Working endpoints - focus on /v1/contratos which we confirmed has data
ENDPOINTS_CONFIG = {
    "contratos_publicacao": {
        "api_path": "/v1/contratos",
        "file_prefix": "pncp-contratos-publicacao",
        "ia_title_prefix": "PNCP Contratos Publicação",
        "tamanhoPagina": 100,  # Conservative page size to ensure success
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "date_format": "yyyyMMdd",
        "required_params": {}
    }
}

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(5))
def fetch_data_from_pncp(endpoint_path, params):
    """Fetches data from a PNCP endpoint with retries."""
    try:
        full_url = f"{BASE_URL}{endpoint_path}"
        typer.echo(f"Making request to: {full_url} with params: {params}")
        response = requests.get(full_url, params=params, timeout=30)
        typer.echo(f"Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        typer.echo(f"Response data summary: {len(data.get('data', []))} items, total: {data.get('totalRegistros', 0)}")
        return data
    except requests.exceptions.RequestException as e:
        typer.echo(f"Request failed for {endpoint_path} with params {params}: {e}", err=True)
        raise

def fetch_page_data(endpoint_cfg, base_params, page_num, endpoint_key):
    """Fetch a single page of data."""
    request_params = base_params.copy()
    request_params["pagina"] = page_num
    
    try:
        data = fetch_data_from_pncp(endpoint_cfg["api_path"], request_params)
        # PNCP API uses "data" field, not "items"
        items = data.get("data", data.get("items", []))
        return page_num, items, data.get("totalPaginas", 0)
    except RetryError as e:
        typer.echo(f"Failed to fetch data for {endpoint_key} page {page_num}: {e}", err=True)
        return page_num, None, 0

def harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg):
    """Harvests all data for a given day and endpoint from PNCP with concurrent requests."""
    base_params = {
        "tamanhoPagina": endpoint_cfg["tamanhoPagina"],
    }
    
    # Handle date parameters with correct format
    if endpoint_cfg["date_param_initial"] is not None:
        # Convert from YYYY-MM-DD to YYYYMMDD format required by API
        date_formatted = day_iso.replace("-", "")
        base_params[endpoint_cfg["date_param_initial"]] = date_formatted
    if endpoint_cfg["date_param_final"] is not None:
        date_formatted = day_iso.replace("-", "")
        base_params[endpoint_cfg["date_param_final"]] = date_formatted
    
    # Add required parameters
    if "required_params" in endpoint_cfg and endpoint_cfg["required_params"]:
        base_params.update(endpoint_cfg["required_params"])

    log_params = base_params.copy()
    typer.echo(f"Starting harvest for '{endpoint_key}' on {day_iso} with params: {log_params}")

    # First, get page 1 to determine total pages
    page_num, items, total_pages = fetch_page_data(endpoint_cfg, base_params, 1, endpoint_key)
    
    if items is None:
        typer.echo(f"Failed to fetch initial page for '{endpoint_key}'", err=True)
        return None
        
    if not items:
        typer.echo(f"No data found for '{endpoint_key}' on {day_iso}.")
        return []

    all_records = items
    typer.echo(f"Found {total_pages} total pages for '{endpoint_key}', fetching remaining pages concurrently...")

    if total_pages > 1:
        # Fetch remaining pages concurrently (max 5 concurrent requests)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_page = {
                executor.submit(fetch_page_data, endpoint_cfg, base_params, page, endpoint_key): page 
                for page in range(2, total_pages + 1)
            }
            
            for future in concurrent.futures.as_completed(future_to_page):
                page_num, page_items, _ = future.result()
                if page_items is not None:
                    all_records.extend(page_items)
                    typer.echo(f"Completed page {page_num}/{total_pages} for '{endpoint_key}' ({len(page_items)} items)")
                else:
                    typer.echo(f"Failed to fetch page {page_num} for '{endpoint_key}'", err=True)
                
                # Small delay to be respectful to the API
                time.sleep(0.1)

    typer.echo(f"Harvested {len(all_records)} records for '{endpoint_key}' on {day_iso}.")
    return all_records

def get_monthly_filename(day_iso, endpoint_cfg):
    """Generate monthly filename for incremental updates."""
    date_obj = datetime.datetime.strptime(day_iso, "%Y-%m-%d")
    month_key = date_obj.strftime("%Y-%m")
    return f"{endpoint_cfg['file_prefix']}-{month_key}.parquet"

def check_file_size_threshold(filepath, max_size_mb=100):
    """Check if file exceeds size threshold (default 100MB)."""
    if not os.path.exists(filepath):
        return False
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    return size_mb > max_size_mb

def append_to_monthly_parquet(day_iso, endpoint_key, endpoint_cfg, records, run_summary_data):
    """Append records to monthly Parquet file or create new one if needed."""
    if not records:
        typer.echo(f"No records to process for '{endpoint_key}' on {day_iso}.")
        run_summary_data[endpoint_key]["status"] = "no_data"
        run_summary_data[endpoint_key]["files_generated"] = []
        return

    output_dir = "baliza_data"
    os.makedirs(output_dir, exist_ok=True)

    monthly_filename = get_monthly_filename(day_iso, endpoint_cfg)
    parquet_filename = os.path.join(output_dir, monthly_filename)
    
    # Check if we need to roll over to a new file due to size
    if check_file_size_threshold(parquet_filename):
        typer.echo(f"File {parquet_filename} exceeds size threshold, creating new file with timestamp...")
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        date_obj = datetime.datetime.strptime(day_iso, "%Y-%m-%d")
        month_key = date_obj.strftime("%Y-%m")
        parquet_filename = os.path.join(output_dir, f"{endpoint_cfg['file_prefix']}-{month_key}-{timestamp}.parquet")

    typer.echo(f"Appending {len(records)} records to monthly file: {parquet_filename}...")
    
    try:
        conn = duckdb.connect()
        
        # Add data_date column to new records
        for record in records:
            record['data_date'] = day_iso
        
        # Create temporary JSON file for new records
        temp_json_file = parquet_filename.replace('.parquet', f'_temp_{day_iso}.json')
        with open(temp_json_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        if os.path.exists(parquet_filename):
            # Append to existing file with schema compatibility
            typer.echo(f"Appending to existing monthly file: {parquet_filename}")
            conn.execute(f"CREATE TEMPORARY TABLE existing_data AS SELECT * FROM '{parquet_filename}'")
            conn.execute(f"CREATE TEMPORARY TABLE new_data AS SELECT * FROM read_json('{temp_json_file}', auto_detect=true, format='newline_delimited', union_by_name=true)")
            
            # Use UNION BY NAME to handle schema differences
            conn.execute("""
                CREATE TEMPORARY TABLE combined_data AS 
                SELECT * FROM existing_data 
                UNION ALL BY NAME 
                SELECT * FROM new_data
            """)
            conn.execute(f"COPY combined_data TO '{parquet_filename}' (FORMAT PARQUET, COMPRESSION 'snappy')")
        else:
            # Create new file
            typer.echo(f"Creating new monthly file: {parquet_filename}")
            conn.execute(f"CREATE TEMPORARY TABLE new_data AS SELECT * FROM read_json('{temp_json_file}', auto_detect=true, format='newline_delimited')")
            conn.execute(f"COPY new_data TO '{parquet_filename}' (FORMAT PARQUET, COMPRESSION 'snappy')")
        
        # Clean up
        os.remove(temp_json_file)
        conn.close()
        
        typer.echo(f"Successfully updated monthly Parquet file: {parquet_filename}")
        return parquet_filename
        
    except Exception as e:
        typer.echo(f"Error during monthly Parquet update for {endpoint_key}: {e}", err=True)
        run_summary_data[endpoint_key]["status"] = "file_error"
        return None

def process_and_upload_data(day_iso, endpoint_key, endpoint_cfg, records, run_summary_data):
    """Processes records: PSA loading, monthly Parquet file, and Internet Archive upload."""
    # Generate unique run ID for this execution
    run_id = _generate_run_id(day_iso, endpoint_key)
    typer.echo(f"Processing run {run_id}")
    
    # Load to PSA (Persistent Staging Area) first
    psa_records_inserted = _load_to_psa(run_id, day_iso, endpoint_key, records)
    
    # Create monthly Parquet file for IA upload
    parquet_filename = append_to_monthly_parquet(day_iso, endpoint_key, endpoint_cfg, records, run_summary_data)
    
    if not parquet_filename:
        run_summary_data[endpoint_key]["files_generated"] = []
        return

    file_details = {
        "parquet_file": parquet_filename,
        "sha256_checksum": None,
        "ia_identifier": None,
        "ia_item_url": None,
        "upload_status": "pending"
    }

    typer.echo(f"Writing {len(records)} records to Parquet file: {parquet_filename}...")
    try:
        # Use DuckDB to convert JSON records to Parquet with compression
        conn = duckdb.connect()
        
        # Create temporary JSON file for DuckDB to read
        temp_json_file = parquet_filename.replace('.parquet', '_temp.json')
        with open(temp_json_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        # Create table from JSONL file and export to Parquet
        conn.execute(f"CREATE TEMPORARY TABLE temp_data AS SELECT * FROM read_json('{temp_json_file}', auto_detect=true, format='newline_delimited')")
        conn.execute(f"COPY temp_data TO '{parquet_filename}' (FORMAT PARQUET, COMPRESSION 'snappy')")
        
        # Clean up temporary file
        os.remove(temp_json_file)
        conn.close()
        typer.echo(f"Successfully created Parquet file: {parquet_filename}")
    except Exception as e:
        typer.echo(f"Error during Parquet file creation for {endpoint_key}: {e}", err=True)
        run_summary_data[endpoint_key]["status"] = "file_error"
        file_details["upload_status"] = "failed_file_error"
        run_summary_data[endpoint_key]["files_generated"].append(file_details)
        return

    file_hash = hashlib.sha256()
    try:
        with open(parquet_filename, "rb") as f:
            while chunk := f.read(8192):
                file_hash.update(chunk)
        sha256_checksum = file_hash.hexdigest()
        file_details["sha256_checksum"] = sha256_checksum
        typer.echo(f"SHA256 checksum for {parquet_filename}: {sha256_checksum}")
    except IOError as e:
        typer.echo(f"Error calculating checksum for {parquet_filename}: {e}", err=True)
        run_summary_data[endpoint_key]["status"] = "checksum_error"
        file_details["upload_status"] = "failed_checksum_error"
        run_summary_data[endpoint_key]["files_generated"].append(file_details)
        return

    ia_identifier = f"{endpoint_cfg['file_prefix']}-{day_iso}"
    file_details["ia_identifier"] = ia_identifier
    file_details["ia_item_url"] = f"https://archive.org/details/{ia_identifier}"


    metadata = {
        "title": f"{endpoint_cfg['ia_title_prefix']} {day_iso}",
        "collection": "opensource",  # Use accessible collection
        "subject": "public procurement Brazil; PNCP; government data; baliza",
        "creator": "Baliza PNCP Mirror Bot",
        "language": "pt",
        "date": day_iso,
        "sha256": sha256_checksum,
        "original_source": "Portal Nacional de Contratações Públicas (PNCP)",
        "description": f"Daily mirror of {endpoint_cfg['ia_title_prefix']} from Brazil's PNCP for {day_iso}. Raw data in Parquet format with Snappy compression.",
        "mediatype": "data"  # Specify media type for data uploads
    }

    ia_access_key = os.getenv("IA_ACCESS_KEY")
    ia_secret_key = os.getenv("IA_SECRET_KEY")

    try:
        if not ia_access_key or not ia_secret_key:
            typer.echo("Internet Archive credentials (IA_ACCESS_KEY, IA_SECRET_KEY) not found. Skipping upload.", err=True)
            file_details["upload_status"] = "skipped_no_credentials"
            run_summary_data[endpoint_key]["status"] = "upload_skipped"
            # Upload will not be attempted
        else:
            typer.echo(f"Uploading {parquet_filename} to Internet Archive with identifier '{ia_identifier}'...")
            upload(
                identifier=ia_identifier,
                files={os.path.basename(parquet_filename): parquet_filename},
                metadata=metadata,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                retries=3,
            )
            typer.echo(f"Successfully uploaded {parquet_filename} to Internet Archive.")
            file_details["upload_status"] = "success"
            run_summary_data[endpoint_key]["status"] = "success"

    except Exception as e:
        # This catches exceptions from the upload block, or if other unexpected error happens before/during upload decision
        # For instance, if getenv itself failed, though unlikely.
        # More relevant for the actual upload() call.
        if file_details["upload_status"] == "pending": # Only update if not already set to skipped
            typer.echo(f"Failed to upload {parquet_filename} to Internet Archive: {e}", err=True)
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

        _log_run_to_db({
            "run_id": run_id,
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "data_date": day_iso,
            "endpoint_key": endpoint_key,
            "parquet_file": parquet_filename,
            "sha256_checksum": file_details["sha256_checksum"], # Should be valid unless checksum failed
            "ia_identifier": file_details["ia_identifier"],
            "ia_item_url": file_details["ia_item_url"],
            "upload_status": file_details["upload_status"], # Critical: this must reflect the final outcome
            "records_fetched": len(records) if records else 0,
            "psa_loaded": psa_records_inserted > 0,
            "psa_records_inserted": psa_records_inserted
        })

        # No cleanup needed - Parquet file is the final output
        typer.echo(f"Process completed for {endpoint_key}. Parquet file saved: {parquet_filename}")


# Typer command decorator
@app.command()
def run_baliza(
    date_str: Annotated[str, typer.Option(..., help="The date to fetch data for, in YYYY-MM-DD format.", envvar="BALIZA_DATE")]
):
    """
    Main command to run the Baliza fetching and archiving process for a specific date.
    """
    # Initialize database on startup
    _init_baliza_db()
    
    try:
        target_day_iso = datetime.datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
    except ValueError:
        typer.echo("Error: Date must be in YYYY-MM-DD format.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"BALIZA process starting for date: {target_day_iso}")

    # Structured summary data for this run
    run_summary = {
        "run_date_iso": datetime.datetime.now(datetime.timezone.utc).isoformat(),
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
