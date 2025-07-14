import concurrent.futures
import datetime
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Annotated

import duckdb
import requests
import typer
from internetarchive import upload
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential
from rich.progress import Progress, TaskID

from .ia_federation import InternetArchiveFederation

# Data directory structure - use XDG standard locations
DATA_DIR = Path.home() / ".local" / "share" / "baliza"
CACHE_DIR = Path.home() / ".cache" / "baliza"
CONFIG_DIR = Path.home() / ".config" / "baliza"

# For development, use local data directory if not in production
if not os.getenv("BALIZA_PRODUCTION"):
    # Development mode - use local project directory
    DATA_DIR = Path.cwd() / "data"
    CACHE_DIR = Path.cwd() / ".cache"
    CONFIG_DIR = Path.cwd() / ".config"

BALIZA_DB_PATH = DATA_DIR / "baliza.duckdb"


def _init_baliza_db():
    """Initialize Baliza database with PSA (Persistent Staging Area) and control tables."""
    # Create all necessary directories
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(BALIZA_DB_PATH))

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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runs_date_endpoint ON control.runs(data_date, endpoint_key)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_contratos_raw_date ON psa.contratos_raw(baliza_data_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_contratos_raw_numero ON psa.contratos_raw(numeroControlePncpCompra)"
    )

    conn.close()


def _ensure_ia_federation_updated():
    """Ensure Internet Archive federation is updated with latest data."""
    try:
        # Check if federation should be updated (daily)
        update_flag_file = CONFIG_DIR / "ia_federation_last_update"
        should_update = True

        if Path(update_flag_file).exists():
            with Path(update_flag_file).open() as f:
                last_update = f.read().strip()
                if last_update == datetime.date.today().isoformat():
                    should_update = False

        if should_update:
            typer.echo("🔄 Updating Internet Archive federation...")
            federation = InternetArchiveFederation(BALIZA_DB_PATH)
            federation.update_federation()

            # Mark as updated today
            with Path(update_flag_file).open("w") as f:
                f.write(datetime.date.today().isoformat())

            typer.echo("✅ IA federation updated")

    except Exception as e:
        typer.echo(f"⚠️ Warning: Could not update IA federation: {e}", err=True)
        # Continue execution - federation is optional enhancement


def _check_existing_data_in_ia(day_iso: str, endpoint_key: str) -> bool:
    """Check if data for this day already exists in Internet Archive."""
    try:
        InternetArchiveFederation(BALIZA_DB_PATH)

        # Check IA catalog for existing data
        conn = duckdb.connect(str(BALIZA_DB_PATH))

        # Try to check if federated views exist
        existing = conn.execute(
            """
            SELECT COUNT(*)
            FROM federated.contratos_ia
            WHERE ia_data_date = ?
            LIMIT 1
        """,
            [day_iso],
        ).fetchone()

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


def _get_missing_dates(days_back: int = 0) -> list[str]:
    """Find dates that are missing from our database in the last N days."""
    conn = duckdb.connect(str(BALIZA_DB_PATH))
    
    # Get the date range to check
    end_date = datetime.date.today() - datetime.timedelta(days=1)  # Yesterday
    
    if days_back == 0:
        # ALL HISTORY - PNCP started operations around mid-2021
        start_date = datetime.date(2021, 6, 1)  # Start from June 2021 when PNCP began operations
        typer.echo(f"🏛️ FULL HISTORICAL BACKUP MODE: Checking ALL dates from {start_date} to {end_date}")
    else:
        start_date = end_date - datetime.timedelta(days=days_back)
        typer.echo(f"📅 LIMITED RANGE MODE: Checking last {days_back} days from {start_date} to {end_date}")
    
    # Get all dates that should exist
    all_dates = []
    current_date = start_date
    while current_date <= end_date:
        all_dates.append(current_date.isoformat())
        current_date += datetime.timedelta(days=1)
    
    try:
        # Get dates we already have data for
        existing_dates = conn.execute("""
            SELECT DISTINCT data_date 
            FROM control.runs 
            WHERE data_date >= ? AND data_date <= ?
            AND upload_status IN ('success', 'upload_skipped')
        """, [start_date.isoformat(), end_date.isoformat()]).fetchall()
        
        existing_dates_set = {row[0] for row in existing_dates}
        
        # Find missing dates
        missing_dates = [date for date in all_dates if date not in existing_dates_set]
        
        conn.close()
        return sorted(missing_dates)
        
    except Exception as e:
        # If control.runs table doesn't exist yet, return all dates
        typer.echo(f"Warning: Could not check existing dates: {e}")
        conn.close()
        return all_dates


def _process_single_date(target_day_iso: str) -> dict:
    """Process data for a single date and return summary."""
    typer.echo(f"\n📅 Processing date: {target_day_iso}")
    
    # Structured summary data for this run
    run_summary = {
        "run_date_iso": datetime.datetime.now(datetime.UTC).isoformat(),
        "target_data_date": target_day_iso,
        "overall_status": "pending",
        "endpoints": {},
    }

    all_endpoints_successful = True

    for endpoint_key, endpoint_config in ENDPOINTS_CONFIG.items():
        typer.echo(f"\nProcessing endpoint: {endpoint_key}")
        run_summary["endpoints"][endpoint_key] = {
            "status": "pending",
            "records_fetched": 0,
            "files_generated": [],  # List to hold details of generated files
        }

        records = harvest_endpoint_data(target_day_iso, endpoint_key, endpoint_config)

        if records is not None:
            run_summary["endpoints"][endpoint_key]["records_fetched"] = len(records)
            process_data_only(
                target_day_iso,
                endpoint_key,
                endpoint_config,
                records,
                run_summary["endpoints"],
            )
            if run_summary["endpoints"][endpoint_key]["status"] not in [
                "success",
                "no_data",
                "upload_skipped",
            ]:  # "upload_skipped" is not a hard failure for the script itself
                all_endpoints_successful = False
        else:
            typer.echo(
                f"Skipping processing and upload for '{endpoint_key}' due to harvesting errors.",
                err=True,
            )
            run_summary["endpoints"][endpoint_key]["status"] = "harvest_failed"
            all_endpoints_successful = False

    if all_endpoints_successful and any(
        e["status"] == "success" for e in run_summary["endpoints"].values()
    ):
        run_summary["overall_status"] = "success"
    elif any(
        e["status"] == "upload_skipped" for e in run_summary["endpoints"].values()
    ) and all(
        e["status"] in ["success", "upload_skipped", "no_data"]
        for e in run_summary["endpoints"].values()
    ):
        run_summary["overall_status"] = "completed_upload_skipped"
    else:
        run_summary["overall_status"] = "failed"

    # Output the structured summary as a JSON line to stdout
    typer.echo(f"\n📊 Summary for {target_day_iso}: {run_summary['overall_status']}")
    
    return run_summary


def _load_to_psa(run_id, data_date, endpoint_key, records):
    """Load raw records to PSA (Persistent Staging Area)."""
    if not records:
        return 0

    conn = duckdb.connect(str(BALIZA_DB_PATH))

    try:
        # Prepare records for PSA insertion
        psa_records = []
        for record in records:
            # Extract main fields safely
            psa_record = {
                "baliza_run_id": run_id,
                "baliza_data_date": data_date,
                "baliza_endpoint": endpoint_key,
                "numeroControlePncpCompra": record.get("numeroControlePncpCompra"),
                "codigoPaisFornecedor": record.get("codigoPaisFornecedor"),
                "anoContrato": record.get("anoContrato"),
                "numeroContratoEmpenho": record.get("numeroContratoEmpenho"),
                "dataAssinatura": record.get("dataAssinatura"),
                "dataVigenciaInicio": record.get("dataVigenciaInicio"),
                "dataVigenciaFim": record.get("dataVigenciaFim"),
                "niFornecedor": record.get("niFornecedor"),
                "tipoPessoa": record.get("tipoPessoa"),
                "nomeRazaoSocialFornecedor": record.get("nomeRazaoSocialFornecedor"),
                "objetoContrato": record.get("objetoContrato"),
                "valorInicial": record.get("valorInicial"),
                "valorParcela": record.get("valorParcela"),
                "valorGlobal": record.get("valorGlobal"),
                "valorAcumulado": record.get("valorAcumulado"),
                # Store complex nested objects as JSON
                "tipoContrato_json": json.dumps(record.get("tipoContrato"))
                if record.get("tipoContrato")
                else None,
                "orgaoEntidade_json": json.dumps(record.get("orgaoEntidade"))
                if record.get("orgaoEntidade")
                else None,
                "categoriaProcesso_json": json.dumps(record.get("categoriaProcesso"))
                if record.get("categoriaProcesso")
                else None,
                "unidadeOrgao_json": json.dumps(record.get("unidadeOrgao"))
                if record.get("unidadeOrgao")
                else None,
                # Store full record as JSON for completeness
                "raw_data_json": json.dumps(record, ensure_ascii=False),
            }
            psa_records.append(psa_record)

        # Bulk insert to PSA
        placeholders = ", ".join(["?" for _ in psa_records[0]])
        columns = ", ".join(psa_records[0].keys())

        values = []
        for record in psa_records:
            values.append(list(record.values()))

        conn.executemany(
            f"""
            INSERT INTO psa.contratos_raw ({columns})
            VALUES ({placeholders})
        """,
            values,
        )

        records_inserted = len(psa_records)
        conn.close()
        return records_inserted

    except Exception as e:
        typer.echo(f"Error loading to PSA: {e}", err=True)
        conn.close()
        return 0
    else:
        typer.echo(f"Loaded {records_inserted} records to PSA for run {run_id}")


def _log_run_to_db(data):
    """Log run data to control.runs table."""
    conn = duckdb.connect(str(BALIZA_DB_PATH))

    # Get file size if file exists
    file_size = 0
    if data.get("parquet_file") and Path(data["parquet_file"]).exists():
        file_size = Path(data["parquet_file"]).stat().st_size

    conn.execute(
        """
        INSERT INTO control.runs (
            run_id, timestamp, data_date, endpoint_key, parquet_file,
            sha256_checksum, ia_identifier, ia_item_url,
            upload_status, records_fetched, file_size_bytes,
            psa_loaded, psa_records_inserted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            data.get("run_id"),
            data["timestamp"],
            data["data_date"],
            data["endpoint_key"],
            str(data.get("parquet_file")) if data.get("parquet_file") else None,
            data.get("sha256_checksum"),
            data.get("ia_identifier"),
            data.get("ia_item_url"),
            data["upload_status"],
            data["records_fetched"],
            file_size,
            data.get("psa_loaded", False),
            data.get("psa_records_inserted", 0),
        ],
    )

    conn.close()


# Create a Typer app instance
app = typer.Typer(
    help="BALIZA: Backup Aberto de Licitações Zelando pelo Acesso. Downloads data from PNCP and uploads to Internet Archive."
)

# Base URL for PNCP API
BASE_URL = "https://pncp.gov.br/api/consulta"

# Endpoints to fetch data from.
# Working endpoints - focus on /v1/contratos which we confirmed has data
ENDPOINTS_CONFIG = {
    "contratos_publicacao": {
        "api_path": "/v1/contratos",
        "file_prefix": "pncp-contratos-publicacao",
        "ia_title_prefix": "PNCP Contratos Publicação",
        "tamanhoPagina": 500,  # Conservative page size to ensure success
        "date_param_initial": "dataInicial",
        "date_param_final": "dataFinal",
        "date_format": "yyyyMMdd",
        "required_params": {},
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
        
        # Handle 204 No Content as "no data available" rather than error
        if response.status_code == 204:
            typer.echo("No data available for this date/endpoint (HTTP 204)")
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
        
        # For other status codes, use standard error handling
        response.raise_for_status()
        data = response.json()
        typer.echo(
            f"Response data summary: {len(data.get('data', []))} items, total: {data.get('totalRegistros', 0)}"
        )
        return data
    except requests.exceptions.RequestException as e:
        typer.echo(
            f"Request failed for {endpoint_path} with params {params}: {e}", err=True
        )
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
        typer.echo(
            f"Failed to fetch data for {endpoint_key} page {page_num}: {e}", err=True
        )
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
    if endpoint_cfg.get("required_params"):
        base_params.update(endpoint_cfg["required_params"])

    log_params = base_params.copy()
    typer.echo(
        f"Starting harvest for '{endpoint_key}' on {day_iso} with params: {log_params}"
    )

    # First, get page 1 to determine total pages
    page_num, items, total_pages = fetch_page_data(
        endpoint_cfg, base_params, 1, endpoint_key
    )

    if items is None:
        typer.echo(f"Failed to fetch initial page for '{endpoint_key}'", err=True)
        return None

    if not items:
        typer.echo(f"No data found for '{endpoint_key}' on {day_iso}.")
        return []

    all_records = items
    if total_pages > 1:
        # Fetch remaining pages concurrently 
        typer.echo(f"Fetching {total_pages} pages concurrently...")
        
        # Fetch remaining pages concurrently (max 5 concurrent requests)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_page = {
                executor.submit(
                    fetch_page_data, endpoint_cfg, base_params, page, endpoint_key
                ): page
                for page in range(2, total_pages + 1)
            }

            completed_pages = 1  # Already have page 1
            for future in concurrent.futures.as_completed(future_to_page):
                page_num, page_items, _ = future.result()
                if page_items is not None:
                    all_records.extend(page_items)
                    completed_pages += 1
                    typer.echo(f"✅ Page {page_num}/{total_pages}: {len(page_items)} items ({completed_pages}/{total_pages} complete)")
                else:
                    typer.echo(f"❌ Failed to fetch page {page_num}", err=True)

                # Small delay to be respectful to the API
                time.sleep(0.1)
    else:
        typer.echo(f"Found only 1 page for '{endpoint_key}'")

    typer.echo(
        f"Harvested {len(all_records)} records for '{endpoint_key}' on {day_iso}."
    )
    return all_records


def get_monthly_filename(day_iso, endpoint_cfg):
    """Generate monthly filename for incremental updates."""
    date_obj = datetime.datetime.strptime(day_iso, "%Y-%m-%d")
    month_key = date_obj.strftime("%Y-%m")
    return f"{endpoint_cfg['file_prefix']}-{month_key}.parquet"


def check_file_size_threshold(filepath, max_size_mb=100):
    """Check if file exceeds size threshold (default 100MB)."""
    if not Path(filepath).exists():
        return False
    size_mb = Path(filepath).stat().st_size / (1024 * 1024)
    return size_mb > max_size_mb


def append_to_monthly_parquet(
    day_iso, endpoint_key, endpoint_cfg, records, run_summary_data
):
    """Append records to monthly Parquet file or create new one if needed."""
    if not records:
        typer.echo(f"No records to process for '{endpoint_key}' on {day_iso}.")
        run_summary_data[endpoint_key]["status"] = "no_data"
        run_summary_data[endpoint_key]["files_generated"] = []
        return

    output_dir = DATA_DIR / "parquet_files"
    output_dir.mkdir(parents=True, exist_ok=True)

    monthly_filename = get_monthly_filename(day_iso, endpoint_cfg)
    parquet_filename = output_dir / monthly_filename

    # Check if we need to roll over to a new file due to size
    if check_file_size_threshold(parquet_filename):
        typer.echo(
            f"File {parquet_filename} exceeds size threshold, creating new file with timestamp..."
        )
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        date_obj = datetime.datetime.strptime(day_iso, "%Y-%m-%d")
        month_key = date_obj.strftime("%Y-%m")
        parquet_filename = (
            output_dir
            / f"{endpoint_cfg['file_prefix']}-{month_key}-{timestamp}.parquet"
        )

    typer.echo(
        f"Appending {len(records)} records to monthly file: {parquet_filename}..."
    )

    try:
        conn = duckdb.connect()

        # Add data_date column to new records
        for record in records:
            record["data_date"] = day_iso

        # Create temporary JSON file for new records
        temp_json_file = str(parquet_filename).replace(".parquet", f"_temp_{day_iso}.json")
        with Path(temp_json_file).open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        if Path(parquet_filename).exists():
            # Append to existing file with schema compatibility
            typer.echo(f"Appending to existing monthly file: {parquet_filename}")
            conn.execute(
                f"CREATE TEMPORARY TABLE existing_data AS SELECT * FROM '{parquet_filename}'"
            )
            conn.execute(
                f"CREATE TEMPORARY TABLE new_data AS SELECT * FROM read_json('{temp_json_file}', auto_detect=true, format='newline_delimited', union_by_name=true)"
            )

            # Use UNION BY NAME to handle schema differences
            conn.execute("""
                CREATE TEMPORARY TABLE combined_data AS
                SELECT * FROM existing_data
                UNION ALL BY NAME
                SELECT * FROM new_data
            """)
            conn.execute(
                f"COPY combined_data TO '{parquet_filename}' (FORMAT PARQUET, COMPRESSION 'snappy')"
            )
        else:
            # Create new file
            typer.echo(f"Creating new monthly file: {parquet_filename}")
            conn.execute(
                f"CREATE TEMPORARY TABLE new_data AS SELECT * FROM read_json('{temp_json_file}', auto_detect=true, format='newline_delimited')"
            )
            conn.execute(
                f"COPY new_data TO '{parquet_filename}' (FORMAT PARQUET, COMPRESSION 'snappy')"
            )

        # Clean up
        Path(temp_json_file).unlink()
        conn.close()

        return str(parquet_filename)  # Convert PosixPath to string

    except Exception as e:
        typer.echo(
            f"Error during monthly Parquet update for {endpoint_key}: {e}", err=True
        )
        run_summary_data[endpoint_key]["status"] = "file_error"
        return None
    else:
        typer.echo(f"Successfully updated monthly Parquet file: {parquet_filename}")


def _create_parquet_file(
    parquet_filename: str, records: list, endpoint_key: str, run_summary_data: dict
) -> dict:
    """Create Parquet file from records and return file details."""
    file_details = {
        "parquet_file": str(parquet_filename),  # Convert PosixPath to string
        "sha256_checksum": None,
        "ia_identifier": None,
        "ia_item_url": None,
        "upload_status": "pending",
    }

    typer.echo(f"Writing {len(records)} records to Parquet file: {parquet_filename}...")
    try:
        # Use DuckDB to convert JSON records to Parquet with compression
        conn = duckdb.connect()

        # Create temporary JSON file for DuckDB to read
        temp_json_file = str(parquet_filename).replace(".parquet", "_temp.json")
        with Path(temp_json_file).open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # Create table from JSONL file and export to Parquet
        conn.execute(
            f"CREATE TEMPORARY TABLE temp_data AS SELECT * FROM read_json('{temp_json_file}', auto_detect=true, format='newline_delimited')"
        )
        conn.execute(
            f"COPY temp_data TO '{parquet_filename}' (FORMAT PARQUET, COMPRESSION 'snappy')"
        )

        # Clean up temporary file
        Path(temp_json_file).unlink()
        conn.close()

    except Exception as e:
        typer.echo(
            f"Error during Parquet file creation for {endpoint_key}: {e}", err=True
        )
        run_summary_data[endpoint_key]["status"] = "file_error"
        file_details["upload_status"] = "failed_file_error"
        run_summary_data[endpoint_key]["files_generated"].append(file_details)
        return file_details
    else:
        typer.echo(f"Successfully created Parquet file: {parquet_filename}")

    return file_details


def _calculate_file_checksum(
    parquet_filename: str, file_details: dict, endpoint_key: str, run_summary_data: dict
) -> bool:
    """Calculate SHA256 checksum for the file. Returns True on success."""
    file_hash = hashlib.sha256()
    try:
        with Path(parquet_filename).open("rb") as f:
            while chunk := f.read(8192):
                file_hash.update(chunk)
        file_details["sha256_checksum"] = file_hash.hexdigest()
        typer.echo(
            f"SHA256 checksum for {parquet_filename}: {file_details['sha256_checksum']}"
        )
        return True
    except OSError as e:
        typer.echo(f"Error calculating checksum for {parquet_filename}: {e}", err=True)
        run_summary_data[endpoint_key]["status"] = "checksum_error"
        file_details["upload_status"] = "failed_checksum_error"
        run_summary_data[endpoint_key]["files_generated"].append(file_details)
        return False


def _upload_to_internet_archive(
    parquet_filename: str,
    file_details: dict,
    endpoint_cfg: dict,
    day_iso: str,
    run_summary_data: dict,
    endpoint_key: str,
) -> None:
    """Upload file to Internet Archive."""
    ia_identifier = f"{endpoint_cfg['file_prefix']}-{day_iso}"
    file_details["ia_identifier"] = ia_identifier
    file_details["ia_item_url"] = f"https://archive.org/details/{ia_identifier}"

    metadata = {
        "title": f"{endpoint_cfg['ia_title_prefix']} {day_iso}",
        "collection": "opensource",
        "subject": "public procurement Brazil; PNCP; government data; baliza",
        "creator": "Baliza PNCP Mirror Bot",
        "language": "pt",
        "date": day_iso,
        "sha256": file_details["sha256_checksum"],
        "original_source": "Portal Nacional de Contratações Públicas (PNCP)",
        "description": f"Daily mirror of {endpoint_cfg['ia_title_prefix']} from Brazil's PNCP for {day_iso}. Raw data in Parquet format with Snappy compression.",
        "mediatype": "data",
    }

    ia_access_key = os.getenv("IA_ACCESS_KEY")
    ia_secret_key = os.getenv("IA_SECRET_KEY")

    try:
        if not ia_access_key or not ia_secret_key:
            typer.echo(
                "Internet Archive credentials (IA_ACCESS_KEY, IA_SECRET_KEY) not found. Skipping upload.",
                err=True,
            )
            file_details["upload_status"] = "skipped_no_credentials"
            run_summary_data[endpoint_key]["status"] = "upload_skipped"
        else:
            typer.echo(
                f"Uploading {parquet_filename} to Internet Archive with identifier '{ia_identifier}'..."
            )
            upload(
                identifier=ia_identifier,
                files={Path(parquet_filename).name: parquet_filename},
                metadata=metadata,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                retries=3,
            )
            typer.echo(f"Successfully uploaded {parquet_filename} to Internet Archive.")
            file_details["upload_status"] = "success"
            run_summary_data[endpoint_key]["status"] = "success"
    except Exception as e:
        _handle_upload_error(
            e, file_details, run_summary_data, endpoint_key, parquet_filename
        )


def _handle_upload_error(
    e: Exception,
    file_details: dict,
    run_summary_data: dict,
    endpoint_key: str,
    parquet_filename: str,
) -> None:
    """Handle upload errors with appropriate status updates."""
    if file_details["upload_status"] == "pending":
        typer.echo(
            f"Failed to upload {parquet_filename} to Internet Archive: {e}",
            err=True,
        )
        file_details["upload_status"] = "failed_upload_error"
        run_summary_data[endpoint_key]["status"] = "upload_failed"
    elif file_details["upload_status"] == "skipped_no_credentials":
        typer.echo(f"An error occurred after deciding to skip upload: {e}", err=True)
    else:
        typer.echo(
            f"An unexpected error occurred during upload/post-upload phase: {e}",
            err=True,
        )
        file_details["upload_status"] = "failed_unknown_error"
        run_summary_data[endpoint_key]["status"] = "upload_failed"


def _batch_upload_to_ia(print_func=typer.echo):
    """Upload all monthly Parquet files to Internet Archive after data collection is complete."""
    print_func("🚀 Starting batch upload to Internet Archive...")
    
    # Find all monthly parquet files that need to be uploaded
    parquet_dir = DATA_DIR / "parquet_files"
    if not parquet_dir.exists():
        print_func("📁 No parquet files directory found")
        return
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        print_func("📄 No parquet files found to upload")
        return
    
    print_func(f"📦 Found {len(parquet_files)} monthly files to upload")
    
    uploaded_count = 0
    failed_count = 0
    
    for parquet_file in parquet_files:
        try:
            # Extract metadata from filename: pncp-contratos-publicacao-2024-07.parquet
            filename_parts = parquet_file.stem.split('-')
            if len(filename_parts) >= 4:
                endpoint_type = '-'.join(filename_parts[1:-2])  # contratos-publicacao
                year_month = '-'.join(filename_parts[-2:])      # 2024-07
                
                # Create IA identifier
                ia_identifier = f"pncp-{endpoint_type}-{year_month}"
                
                print_func(f"📤 Uploading {parquet_file.name} as {ia_identifier}...")
                
                # Calculate checksum
                file_hash = hashlib.sha256()
                with parquet_file.open("rb") as f:
                    while chunk := f.read(8192):
                        file_hash.update(chunk)
                sha256_checksum = file_hash.hexdigest()
                
                # Prepare metadata
                metadata = {
                    "title": f"PNCP {endpoint_type.title().replace('-', ' ')} Data - {year_month}",
                    "creator": "BALIZA - Backup Aberto de Licitações Zelando pelo Acesso",
                    "subject": ["public procurement", "Brazil", "PNCP", "government data"],
                    "description": f"Monthly consolidated data from Brazilian PNCP (Portal Nacional de Contratações Públicas) for {year_month}. Contains {endpoint_type.replace('-', ' ')} records in Parquet format.",
                    "mediatype": "data",
                    "collection": "opensource_data",
                    "sha256": sha256_checksum,
                    "baliza_file_type": "monthly_consolidated",
                    "baliza_year_month": year_month,
                    "baliza_endpoint": endpoint_type,
                }
                
                # Upload to Internet Archive
                response = upload(
                    ia_identifier,
                    files={parquet_file.name: str(parquet_file)},
                    metadata=metadata,
                    verbose=False,
                    verify=True,
                )
                
                if response[0].status_code in [200, 201]:
                    uploaded_count += 1
                    print_func(f"✅ Successfully uploaded {parquet_file.name}")
                    
                    # Update database records for this month
                    _update_db_upload_status(year_month, endpoint_type, ia_identifier, sha256_checksum)
                else:
                    failed_count += 1
                    print_func(f"❌ Failed to upload {parquet_file.name}: HTTP {response[0].status_code}")
            else:
                failed_count += 1
                print_func(f"❌ Cannot parse filename: {parquet_file.name}")
                
        except Exception as e:
            failed_count += 1
            print_func(f"❌ Error uploading {parquet_file.name}: {e}")
    
    # Final upload summary
    print_func(f"\n📊 Batch Upload Summary:")
    print_func(f"   ✅ Uploaded: {uploaded_count}")
    print_func(f"   ❌ Failed: {failed_count}")
    print_func(f"   📈 Total: {len(parquet_files)}")


def _update_db_upload_status(year_month: str, endpoint_type: str, ia_identifier: str, sha256_checksum: str):
    """Update database records with IA upload information."""
    try:
        conn = duckdb.connect(str(BALIZA_DB_PATH))
        
        # Update all records for this month/endpoint with IA details
        conn.execute("""
            UPDATE control.runs 
            SET 
                ia_identifier = ?,
                sha256_checksum = ?,
                upload_status = 'success',
                ia_item_url = ?
            WHERE data_date LIKE ? 
              AND endpoint_key LIKE ?
              AND upload_status = 'data_collected'
        """, [
            ia_identifier,
            sha256_checksum, 
            f"https://archive.org/details/{ia_identifier}",
            f"{year_month}-%",  # Matches YYYY-MM-DD pattern
            f"%{endpoint_type.replace('-', '_')}%"  # Flexible endpoint matching
        ])
        
        conn.close()
    except Exception as e:
        typer.echo(f"Warning: Could not update database with IA details: {e}", err=True)


def process_data_only(
    day_iso, endpoint_key, endpoint_cfg, records, run_summary_data
):
    """Processes records: PSA loading and monthly Parquet file creation (no IA upload)."""
    # Generate unique run ID for this execution
    run_id = _generate_run_id(day_iso, endpoint_key)
    typer.echo(f"Processing run {run_id}")

    # Load to PSA (Persistent Staging Area) first
    psa_records_inserted = _load_to_psa(run_id, day_iso, endpoint_key, records)

    # Append to monthly Parquet file (consolidating data)
    parquet_filename = append_to_monthly_parquet(
        day_iso, endpoint_key, endpoint_cfg, records, run_summary_data
    )

    if parquet_filename:
        run_summary_data[endpoint_key]["status"] = "success"
        run_summary_data[endpoint_key]["files_generated"] = [{
            "parquet_file": str(parquet_filename),
            "upload_status": "pending_batch_upload"
        }]
        
        # Log to database (without IA details yet)
        _log_run_to_db({
            "run_id": run_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "data_date": day_iso,
            "endpoint_key": endpoint_key,
            "parquet_file": str(parquet_filename),
            "upload_status": "data_collected",
            "records_fetched": len(records),
            "psa_loaded": True,
            "psa_records_inserted": psa_records_inserted,
        })
    else:
        run_summary_data[endpoint_key]["status"] = "file_error"
        run_summary_data[endpoint_key]["files_generated"] = []


def process_and_upload_data(
    day_iso, endpoint_key, endpoint_cfg, records, run_summary_data
):
    """Processes records: PSA loading, monthly Parquet file, and Internet Archive upload."""
    # Generate unique run ID for this execution
    run_id = _generate_run_id(day_iso, endpoint_key)
    typer.echo(f"Processing run {run_id}")

    # Load to PSA (Persistent Staging Area) first
    psa_records_inserted = _load_to_psa(run_id, day_iso, endpoint_key, records)

    # Create monthly Parquet file for IA upload
    parquet_filename = append_to_monthly_parquet(
        day_iso, endpoint_key, endpoint_cfg, records, run_summary_data
    )

    if not parquet_filename:
        run_summary_data[endpoint_key]["files_generated"] = []
        return

    # Create Parquet file and get file details
    file_details = _create_parquet_file(
        parquet_filename, records, endpoint_key, run_summary_data
    )
    if file_details["upload_status"].startswith("failed"):
        return

    # Calculate file checksum
    if not _calculate_file_checksum(
        parquet_filename, file_details, endpoint_key, run_summary_data
    ):
        return

    # Upload to Internet Archive
    _upload_to_internet_archive(
        parquet_filename,
        file_details,
        endpoint_cfg,
        day_iso,
        run_summary_data,
        endpoint_key,
    )

    file_hash = hashlib.sha256()
    try:
        with Path(parquet_filename).open("rb") as f:
            while chunk := f.read(8192):
                file_hash.update(chunk)
        sha256_checksum = file_hash.hexdigest()
        file_details["sha256_checksum"] = sha256_checksum
        typer.echo(f"SHA256 checksum for {parquet_filename}: {sha256_checksum}")
    except OSError as e:
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
        "mediatype": "data",  # Specify media type for data uploads
    }

    ia_access_key = os.getenv("IA_ACCESS_KEY")
    ia_secret_key = os.getenv("IA_SECRET_KEY")

    try:
        if not ia_access_key or not ia_secret_key:
            typer.echo(
                "Internet Archive credentials (IA_ACCESS_KEY, IA_SECRET_KEY) not found. Skipping upload.",
                err=True,
            )
            file_details["upload_status"] = "skipped_no_credentials"
            run_summary_data[endpoint_key]["status"] = "upload_skipped"
            # Upload will not be attempted
        else:
            typer.echo(
                f"Uploading {parquet_filename} to Internet Archive with identifier '{ia_identifier}'..."
            )
            upload(
                identifier=ia_identifier,
                files={Path(parquet_filename).name: parquet_filename},
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
        if (
            file_details["upload_status"] == "pending"
        ):  # Only update if not already set to skipped
            typer.echo(
                f"Failed to upload {parquet_filename} to Internet Archive: {e}",
                err=True,
            )
            file_details["upload_status"] = "failed_upload_error"
            run_summary_data[endpoint_key]["status"] = "upload_failed"
        elif file_details["upload_status"] == "skipped_no_credentials":
            # This means the error happened somewhere outside the direct upload call, but after skip decision
            typer.echo(
                f"An error occurred after deciding to skip upload: {e}", err=True
            )
            # Keep status as skipped, but log error.
        else:  # Should not happen if logic is correct (e.g. success path)
            typer.echo(
                f"An unexpected error occurred during upload/post-upload phase: {e}",
                err=True,
            )
            file_details["upload_status"] = (
                "failed_unknown_error"  # Generic error status
            )
            run_summary_data[endpoint_key]["status"] = "upload_failed"

    finally:
        # This block executes regardless of upload success, failure, or skip.
        run_summary_data[endpoint_key]["files_generated"].append(file_details)

        _log_run_to_db(
            {
                "run_id": run_id,
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                "data_date": day_iso,
                "endpoint_key": endpoint_key,
                "parquet_file": str(parquet_filename),  # Convert PosixPath to string
                "sha256_checksum": file_details[
                    "sha256_checksum"
                ],  # Should be valid unless checksum failed
                "ia_identifier": file_details["ia_identifier"],
                "ia_item_url": file_details["ia_item_url"],
                "upload_status": file_details[
                    "upload_status"
                ],  # Critical: this must reflect the final outcome
                "records_fetched": len(records) if records else 0,
                "psa_loaded": psa_records_inserted > 0,
                "psa_records_inserted": psa_records_inserted,
            }
        )

        # No cleanup needed - Parquet file is the final output
        typer.echo(
            f"Process completed for {endpoint_key}. Parquet file saved: {parquet_filename}"
        )


# Typer command decorator
@app.command()
def run_baliza(
    date_str: Annotated[
        str | None,
        typer.Option(
            "--date",
            help="The date to fetch data for, in YYYY-MM-DD format. If not provided, automatically processes all missing dates.",
            envvar="BALIZA_DATE",
        ),
    ] = None,
    auto: Annotated[
        bool,
        typer.Option(
            "--auto",
            help="Automatically fetch ALL missing dates from complete historical archive"
        ),
    ] = False,
    days_back: Annotated[
        int,
        typer.Option(
            "--days-back",
            help="Number of days to look back when using auto mode. Use 0 for ALL history (default: ALL)"
        ),
    ] = 0,
):
    """
    Main command to run the Baliza fetching and archiving process.
    
    Can run for a specific date or automatically process all missing dates.
    """
    # Initialize database on startup
    _init_baliza_db()

    # Determine what dates to process
    if auto or date_str is None:
        # Auto mode: find missing dates
        if date_str is not None:
            typer.echo("Warning: Both --auto and date provided. Using auto mode.", err=True)
        
        if days_back == 0:
            typer.echo("🏛️ Finding ALL missing dates from complete historical archive...")
        else:
            typer.echo(f"🔍 Finding missing dates from the last {days_back} days...")
        missing_dates = _get_missing_dates(days_back)
        
        if not missing_dates:
            typer.echo("✅ No missing dates found! All recent dates already processed.")
            return
        
        if len(missing_dates) <= 10:
            typer.echo(f"📅 Found {len(missing_dates)} missing dates: {', '.join(missing_dates)}")
        else:
            typer.echo(f"📅 Found {len(missing_dates)} missing dates: {missing_dates[0]} to {missing_dates[-1]}")
            typer.echo(f"   (Range: {(datetime.date.fromisoformat(missing_dates[-1]) - datetime.date.fromisoformat(missing_dates[0])).days + 1} days total)")
        target_dates = missing_dates
        
    else:
        # Single date mode
        try:
            target_day_iso = (
                datetime.datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
            )
        except ValueError as e:
            typer.echo("Error: Date must be in YYYY-MM-DD format.", err=True)
            raise typer.Exit(code=1) from e
        
        target_dates = [target_day_iso]
    
    # Process all target dates
    all_summaries = []
    successful_dates = 0
    failed_dates = 0
    
    typer.echo(f"🚀 BALIZA starting to process {len(target_dates)} date(s)")
    
    # Use rich progress bar for multiple dates
    if len(target_dates) > 1:
        with Progress() as progress:
            main_task = progress.add_task(
                f"[green]Processing {len(target_dates)} dates...", 
                total=len(target_dates)
            )
            
            for i, date in enumerate(target_dates, 1):
                progress.update(
                    main_task, 
                    description=f"[green]Processing date {i}/{len(target_dates)}: {date}",
                    completed=i-1
                )
                
                run_summary = _process_single_date(date)
                all_summaries.append(run_summary)
                
                if run_summary["overall_status"] in ["success", "completed_upload_skipped"]:
                    successful_dates += 1
                    progress.console.print(f"✅ {date}: {run_summary['overall_status']}")
                else:
                    failed_dates += 1
                    progress.console.print(f"❌ {date}: {run_summary['overall_status']}")
                
                progress.update(main_task, completed=i)
            
            # After all dates processed, upload monthly files to Internet Archive
            if len(target_dates) > 1:
                progress.update(main_task, description="[blue]Uploading consolidated files to Internet Archive...")
                _batch_upload_to_ia(progress.console.print)
    else:
        # Single date - no progress bar needed
        for i, date in enumerate(target_dates, 1):
            typer.echo(f"\n{'='*60}")
            typer.echo(f"📅 Processing date {i}/{len(target_dates)}: {date}")
            typer.echo(f"{'='*60}")
            
            run_summary = _process_single_date(date)
            all_summaries.append(run_summary)
            
            if run_summary["overall_status"] in ["success", "completed_upload_skipped"]:
                successful_dates += 1
                typer.echo(f"✅ {date}: {run_summary['overall_status']}")
            else:
                failed_dates += 1
                typer.echo(f"❌ {date}: {run_summary['overall_status']}")
    
    # After all dates processed, upload monthly files to Internet Archive (single date mode)
    if len(target_dates) == 1:
        _batch_upload_to_ia(typer.echo)
    elif len(target_dates) > 1:
        typer.echo("\n📤 Uploading consolidated files to Internet Archive...")
        _batch_upload_to_ia(typer.echo)
    
    # Final summary
    typer.echo(f"\n{'='*60}")
    typer.echo("📊 FINAL SUMMARY")
    typer.echo(f"{'='*60}")
    typer.echo(f"Total dates processed: {len(target_dates)}")
    typer.echo(f"Successful: {successful_dates}")
    typer.echo(f"Failed: {failed_dates}")
    
    if len(target_dates) == 1:
        # Single date mode - output detailed JSON for compatibility
        typer.echo("\n--- BALIZA RUN SUMMARY ---")
        typer.echo(json.dumps(all_summaries[0]))
        typer.echo("--- END BALIZA RUN SUMMARY ---\n")
        
        if all_summaries[0]["overall_status"] == "failed":
            raise typer.Exit(code=1)
    else:
        # Multi-date mode - show summary statistics
        if failed_dates > 0:
            typer.echo(f"\n⚠️  {failed_dates} dates failed processing")
            
        if failed_dates == len(target_dates):
            typer.echo("❌ All dates failed!")
            raise typer.Exit(code=1)
        else:
            typer.echo(f"✅ Successfully processed {successful_dates}/{len(target_dates)} dates")
    
    typer.echo("🎉 BALIZA run completed!")


if __name__ == "__main__":
    app()
