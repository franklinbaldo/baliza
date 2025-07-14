import datetime
import os
import subprocess
import sys
from pathlib import Path
from typing import Annotated

import duckdb
import typer

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


def _init_baliza_db(base_path: Path = None):
    """Initialize Baliza database with PSA (Persistent Staging Area) and control tables."""
    global DATA_DIR, CACHE_DIR, CONFIG_DIR, BALIZA_DB_PATH

    if base_path:
        DATA_DIR = base_path / "data"
        CACHE_DIR = base_path / ".cache"
        CONFIG_DIR = base_path / ".config"
        BALIZA_DB_PATH = base_path / "state" / "baliza.duckdb"

    # Create all necessary directories
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    BALIZA_DB_PATH.parent.mkdir(parents=True, exist_ok=True)  # Ensure state directory exists

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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_date_endpoint ON control.runs(data_date, endpoint_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_contratos_raw_date ON psa.contratos_raw(baliza_data_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_contratos_raw_numero ON psa.contratos_raw(numeroControlePncpCompra)")

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





def _get_missing_dates(days_back: int = 0, force_retry: bool = False) -> list[tuple[str, str]]:
    """Find date ranges that are missing from our database. Returns list of (start_date, end_date) tuples."""
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

    try:
        # Get dates we already have data for
        if force_retry:
            # When force_retry is enabled, only skip dates with actual data (not no_data)
            existing_dates_query = """
                SELECT DISTINCT data_date
                FROM control.runs
                WHERE data_date >= ? AND data_date <= ?
                AND upload_status IN ('success', 'upload_skipped', 'data_collected')
                ORDER BY data_date
            """
            if existing_dates_query:
                typer.echo("🔄 Force retry enabled: Will retry dates that previously returned no data")
        else:
            # Normal mode: skip dates with data OR that returned no data
            existing_dates_query = """
                SELECT DISTINCT data_date
                FROM control.runs
                WHERE data_date >= ? AND data_date <= ?
                AND upload_status IN ('success', 'upload_skipped', 'no_data', 'data_collected')
                ORDER BY data_date
            """

        existing_dates = conn.execute(existing_dates_query, [start_date.isoformat(), end_date.isoformat()]).fetchall()
        existing_dates_set = {row[0] for row in existing_dates}

        conn.close()

        # Generate optimal date ranges for missing periods
        return _generate_optimal_date_ranges(start_date, end_date, existing_dates_set)

    except Exception as e:
        # If control.runs table doesn't exist yet, generate ranges for full period
        typer.echo(f"Warning: Could not check existing dates: {e}")
        conn.close()
        return _generate_optimal_date_ranges(start_date, end_date, set())


def _generate_optimal_date_ranges(
    start_date: datetime.date, end_date: datetime.date, existing_dates: set[str]
) -> list[tuple[str, str]]:
    """Generate optimal date ranges using 365-day windows, monthly chunks, and 1-day overlap."""
    ranges = []
    current_date = start_date

    while current_date <= end_date:
        # Skip dates we already have
        if current_date.isoformat() in existing_dates:
            current_date += datetime.timedelta(days=1)
            continue

        # Find the start of a missing period
        range_start = current_date

        # Find the end of this missing period
        range_end = current_date
        while range_end <= end_date and range_end.isoformat() not in existing_dates:
            range_end += datetime.timedelta(days=1)

        # range_end is now the first existing date or end_date + 1
        range_end = range_end - datetime.timedelta(days=1)  # Last missing date

        # Split large missing periods into optimal chunks
        period_ranges = _split_into_optimal_chunks(range_start, range_end)
        ranges.extend(period_ranges)

        # Move to the next period
        current_date = range_end + datetime.timedelta(days=1)

    if ranges:
        total_days = sum(
            (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days + 1 for start, end in ranges
        )
        typer.echo(f"📅 Found {len(ranges)} missing date ranges covering {total_days} total days")

        # Show first few ranges for user visibility
        for i, (start, end) in enumerate(ranges[:3]):
            days_in_range = (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days + 1
            typer.echo(f"   📄 Range {i + 1}: {start} to {end} ({days_in_range} days)")

        if len(ranges) > 3:
            typer.echo(f"   ... and {len(ranges) - 3} more ranges")

    return ranges


def _split_into_optimal_chunks(start_date: datetime.date, end_date: datetime.date) -> list[tuple[str, str]]:
    """Split a date range into optimal chunks with 1-day overlap."""
    chunks = []
    current_start = start_date

    while current_start <= end_date:
        # Determine chunk size based on remaining days
        remaining_days = (end_date - current_start).days + 1

        if remaining_days >= 365:
            # Use 365-day chunks for large periods
            chunk_days = 365
        elif remaining_days >= 60:
            # Use monthly chunks (30-31 days) for medium periods
            chunk_days = min(remaining_days, 31)
        else:
            # Use remaining days for small periods
            chunk_days = remaining_days

        current_end = current_start + datetime.timedelta(days=chunk_days - 1)
        current_end = min(current_end, end_date)  # Don't exceed the range

        chunks.append((current_start.isoformat(), current_end.isoformat()))

        # Move to next chunk with 1-day overlap for safety
        if current_end < end_date:
            current_start = current_end  # 1-day overlap (reprocess last day)
        else:
            break

    return chunks








# Create a Typer app instance
app = typer.Typer(
    help="BALIZA: Backup Aberto de Licitações Zelando pelo Acesso. Downloads data from PNCP and uploads to Internet Archive."
)


def _determine_target_date_ranges(date_str, auto, days_back, force_retry):
    """Determine what date ranges to process based on CLI arguments."""
    if auto or date_str is None:
        # Auto mode: find missing dates
        if date_str is not None:
            typer.echo("Warning: Both --auto and date provided. Using auto mode.", err=True)

        if days_back == 0:
            typer.echo("🏛️ Finding ALL missing dates from complete historical archive...")
        else:
            typer.echo(f"🔍 Finding missing dates from the last {days_back} days...")
        missing_date_ranges = _get_missing_dates(days_back, force_retry)

        if not missing_date_ranges:
            typer.echo("✅ No missing date ranges found! All recent dates already processed.")
            return []

        return missing_date_ranges

    else:
        # Single date mode - convert to range format
        try:
            target_day_iso = datetime.datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
        except ValueError as e:
            typer.echo("Error: Date must be in YYYY-MM-DD format.", err=True)
            raise typer.Exit(code=1) from e

        return [(target_day_iso, target_day_iso)]


def _process_date_range(date_str: str):
    """Calls the ingestion script for a given date."""
    typer.echo(f"Calling ingestion for date: {date_str}")
    try:
        # Construct the command to run scripts/ingestion.py
        # Assuming scripts/ingestion.py is executable or can be run with python
        command = [sys.executable, str(Path(__file__).parent.parent / "scripts" / "ingestion.py"), date_str]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        typer.echo(result.stdout)
        if result.stderr:
            typer.echo(result.stderr, err=True)
        return {"overall_status": "success", "date": date_str}
    except subprocess.CalledProcessError as e:
        typer.echo(f"Error running ingestion for {date_str}: {e}", err=True)
        typer.echo(e.stdout, err=True)
        typer.echo(e.stderr, err=True)
        return {"overall_status": "failed", "date": date_str, "error": str(e)}

def _process_multiple_date_ranges(target_date_ranges):
    """Process multiple date ranges with progress bar."""
    all_summaries = []
    successful_ranges = 0
    failed_ranges = 0

    for i, (start_date, end_date) in enumerate(target_date_ranges, 1):
        # For now, we'll just process each day individually within the range
        # This can be optimized later if needed
        current_date = datetime.date.fromisoformat(start_date)
        end_date_obj = datetime.date.fromisoformat(end_date)

        while current_date <= end_date_obj:
            date_to_process = current_date.isoformat()
            run_summary = _process_date_range(date_to_process)
            all_summaries.append(run_summary)

            if run_summary["overall_status"] == "success":
                successful_ranges += 1
            else:
                failed_ranges += 1
            current_date += datetime.timedelta(days=1)

    return all_summaries, successful_ranges, failed_ranges





def _print_final_summary(target_date_ranges, all_summaries, successful_ranges, failed_ranges):
    """Print final summary and handle exit codes."""
    total_days = sum(
        (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days + 1
        for start, end in target_date_ranges
    )

    # Final summary
    typer.echo(f"\n{'=' * 60}")
    typer.echo("📊 FINAL SUMMARY")
    typer.echo(f"{'=' * 60}")
    typer.echo(f"Total date ranges processed: {len(target_date_ranges)}")
    typer.echo(f"Total days covered: {total_days}")
    typer.echo(f"Successful ranges: {successful_ranges}")
    typer.echo(f"Failed ranges: {failed_ranges}")

    if len(target_date_ranges) == 1:
        # Single date mode - output detailed JSON for compatibility
        typer.echo("\n--- BALIZA RUN SUMMARY ---")
        typer.echo(json.dumps(all_summaries[0]))
        typer.echo("--- END BALIZA RUN SUMMARY ---\n")

        if all_summaries[0]["overall_status"] == "failed":
            raise typer.Exit(code=1)
    else:
        # Multi-range mode - show summary statistics
        if failed_ranges > 0:
            typer.echo(f"\n⚠️  {failed_ranges} date ranges failed processing")

        if failed_ranges == len(target_date_ranges):
            typer.echo("❌ All date ranges failed!")
            raise typer.Exit(code=1)
        else:
            typer.echo(f"✅ Successfully processed {successful_ranges}/{len(target_date_ranges)} date ranges")

    typer.echo("🎉 BALIZA run completed!")


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
            help="Automatically fetch ALL missing dates from complete historical archive",
        ),
    ] = False,
    days_back: Annotated[
        int,
        typer.Option(
            "--days-back",
            help="Number of days to look back when using auto mode. Use 0 for ALL history (default: ALL)",
        ),
    ] = 0,
    force_retry: Annotated[
        bool,
        typer.Option(
            "--force-retry",
            help="Force retry dates that previously returned no data (HTTP 204)",
        ),
    ] = False,
):
    """
    Main command to run the Baliza fetching and archiving process.

    Can run for a specific date or automatically process all missing dates.
    """
    # Initialize database on startup
    _init_baliza_db()

    # Determine what dates to process
    target_date_ranges = _determine_target_date_ranges(date_str, auto, days_back, force_retry)
    if not target_date_ranges:
        return

    # Calculate total coverage
    total_days = sum(
        (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days + 1
        for start, end in target_date_ranges
    )
    typer.echo(f"🚀 BALIZA starting to process {len(target_date_ranges)} date range(s) covering {total_days} days")

    # Process date ranges
    if len(target_date_ranges) > 1:
        all_summaries, successful_ranges, failed_ranges = _process_multiple_date_ranges(target_date_ranges)
    else:
        all_summaries, successful_ranges, failed_ranges = _process_single_date_range(target_date_ranges)

    # Print final summary and handle exit codes
    _print_final_summary(target_date_ranges, all_summaries, successful_ranges, failed_ranges)


if __name__ == "__main__":
    app()
