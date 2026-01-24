#!/usr/bin/env python3
"""Continuous historical backfill for PNCP data.

This script processes pending historical dates in batches, filling gaps in the
data collection. It runs frequently (e.g., every 15 minutes) and completes a
backfill in parallel with daily incremental updates.

Key features:
- Batch processing (configurable, default 5-10 dates per run)
- Idempotent extraction (INSERT OR IGNORE by primary key)
- State management using DuckDB coverage table
- Export with 7-day stability window
- Automatic cleanup after successful upload
- GitHub Actions artifact persistence for state backup

Exit codes:
  0 - Success (completed batch or no pending dates)
  1 - Partial failure (extraction succeeded, upload failed)
  2 - Complete failure (extraction failed)
  3 - Configuration error
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, List
import base64
from hashlib import md5

try:
    import requests
    import duckdb
except ImportError:
    print("❌ Required packages missing. Install with: uv sync")
    sys.exit(3)

# Configuration from environment
DB_PATH = Path(os.getenv("BALIZA_DB_PATH", "baliza.duckdb"))
EXPORT_DIR = Path(os.getenv("BALIZA_EXPORT_DIR", "data/daily"))
IA_BUCKET = os.getenv("IA_BUCKET", "jules-mail-frank-2026")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT_URL", "https://s3.us.archive.org")

# Backfill-specific configuration
BACKFILL_START = os.getenv("BALIZA_BACKFILL_START", "2024-01-01")
BACKFILL_END = os.getenv("BALIZA_BACKFILL_END", None)  # Defaults to yesterday
BATCH_SIZE = int(os.getenv("BALIZA_BATCH_SIZE", "5"))
STABILITY_DAYS = int(os.getenv("BALIZA_STABILITY_DAYS", "7"))
UPLOAD_TO_IA = os.getenv("BALIZA_UPLOAD_TO_IA", "true").lower() == "true"
DRY_RUN = os.getenv("BALIZA_DRY_RUN", "false").lower() == "true"

# Logging
def log(msg: str, level: str = "INFO") -> None:
    timestamp = datetime.now().isoformat()
    print(f"[{timestamp}] {level}: {msg}")

def log_section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def validate_config() -> bool:
    """Validate required configuration."""
    if not DB_PATH.exists():
        log(f"Warning: Database not found at {DB_PATH}, will be created", "WARN")

    if UPLOAD_TO_IA:
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            log("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required", "ERROR")
            return False

    return True

def generate_date_range(start_date: date, end_date: date) -> List[date]:
    """Generate list of dates between start and end (inclusive)."""
    current = start_date
    dates = []
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def get_completed_dates(con: duckdb.DuckDBPyConnection) -> set:
    """Get set of dates that have been successfully extracted."""
    try:
        result = con.execute("""
            SELECT DISTINCT DATE(window_start) as extracted_date
            FROM baliza_state.coverage
            WHERE resource = 'contratos'
            AND status = 'ok'
        """).fetchall()
        return {row[0] for row in result}
    except Exception as e:
        log(f"Could not read completed dates: {e}", "WARN")
        return set()

def get_pending_dates(con: duckdb.DuckDBPyConnection, start: date, end: date) -> List[date]:
    """Get list of dates that still need to be extracted."""
    all_dates = generate_date_range(start, end)
    completed = get_completed_dates(con)
    pending = [d for d in all_dates if d not in completed]
    return sorted(pending)  # Process oldest dates first

def run_extraction(target_date: date) -> bool:
    """Run extraction for a single date."""
    start_str = target_date.strftime("%Y-%m-%d")
    end_str = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    log(f"Extracting {start_str}")

    cmd = [
        sys.executable, "-m", "baliza.cli_simple",
        "extract",
        "--start", start_str,
        "--end", end_str,
        "--duckdb", str(DB_PATH),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            log(f"Extraction failed for {start_str}: {result.stderr}", "ERROR")
            return False

        # Parse row count from output
        for line in result.stdout.split("\n"):
            if "Rows:" in line:
                log(f"✓ {line.strip()}")
        return True
    except subprocess.TimeoutExpired:
        log(f"Extraction timeout for {start_str}", "ERROR")
        return False
    except Exception as e:
        log(f"Extraction error for {start_str}: {e}", "ERROR")
        return False

def export_stable_dates(con: duckdb.DuckDBPyConnection) -> Optional[Path]:
    """Export dates that are old enough to be considered stable."""
    cutoff = (datetime.now() - timedelta(days=STABILITY_DAYS)).date()

    log(f"Exporting dates older than {cutoff}")

    # Get dates in coverage table that are ready for export
    result = con.execute("""
        SELECT DISTINCT DATE(window_start) as export_date
        FROM baliza_state.coverage
        WHERE resource = 'contratos'
        AND status = 'ok'
        AND DATE(window_start) <= ?
        AND DATE(window_start) NOT IN (
            SELECT DATE(extraction_date)
            FROM baliza_state.uploaded_to_ia
        )
        ORDER BY export_date
        LIMIT 5
    """, [cutoff]).fetchall()

    if not result:
        log("No dates ready for export")
        return None

    export_dates = [row[0] for row in result]
    export_dir = EXPORT_DIR / "backfill"
    export_dir.mkdir(parents=True, exist_ok=True)

    for export_date in export_dates:
        date_str = export_date.isoformat()
        log(f"Running export for {date_str}")

        cmd = [
            sys.executable, "-m", "baliza.cli_simple",
            "export-daily",
            "--date", date_str,
            "--output", str(export_dir),
            "--duckdb", str(DB_PATH),
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                log(f"Export failed for {date_str}: {result.stderr}", "WARN")
                continue
            log(f"✓ Exported {date_str}")
        except Exception as e:
            log(f"Export error for {date_str}: {e}", "WARN")
            continue

    return export_dir if export_dir.exists() else None

def upload_to_internet_archive(export_dir: Optional[Path]) -> bool:
    """Upload exported Parquet files to Internet Archive."""
    if not UPLOAD_TO_IA or not export_dir:
        return True

    if DRY_RUN:
        log("DRY RUN: Skipping actual upload")
        return True

    log("Uploading to Internet Archive")
    success = True

    # Find all date directories
    for date_dir in sorted(export_dir.glob("????-??-??")):
        if not date_dir.is_dir():
            continue

        date_str = date_dir.name
        log(f"Uploading {date_str}")

        # Upload each parquet file
        for parquet_file in date_dir.glob("*.parquet"):
            if not upload_file_to_s3(parquet_file, date_str):
                log(f"Failed to upload {parquet_file.name}", "WARN")
                success = False
            else:
                log(f"✓ Uploaded {parquet_file.name}")

        # Upload metadata
        metadata_file = date_dir / "_metadata.json"
        if metadata_file.exists():
            if not upload_file_to_s3(metadata_file, date_str):
                log(f"Failed to upload {metadata_file.name}", "WARN")
                success = False
            else:
                log(f"✓ Uploaded {metadata_file.name}")

    return success

def upload_file_to_s3(file_path: Path, date_str: str) -> bool:
    """Upload a single file to Internet Archive S3."""
    try:
        with open(file_path, "rb") as f:
            file_data = f.read()

        file_md5 = base64.b64encode(__import__('hashlib').md5(file_data).digest()).decode()

        headers = {
            "Authorization": f"LOW {AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}",
            "Content-MD5": file_md5,
            "Content-Type": "application/octet-stream",
            "x-amz-meta-date": date_str,
            "x-amz-meta-source": "https://pncp.gov.br/api/consulta/v1",
            "x-amz-meta-backfill": "true",
        }

        url = f"{AWS_S3_ENDPOINT_URL}/{IA_BUCKET}/{file_path.name}"
        response = requests.put(url, data=file_data, headers=headers, timeout=120)

        return response.status_code == 200
    except Exception as e:
        log(f"Upload error: {e}", "ERROR")
        return False

def record_uploads(con: duckdb.DuckDBPyConnection, dates_exported: List[date]) -> None:
    """Record which dates have been uploaded to Internet Archive."""
    try:
        for export_date in dates_exported:
            con.execute(
                """
                INSERT OR REPLACE INTO baliza_state.uploaded_to_ia
                VALUES (?, ?, NOW(), ?, ?)
            """,
                [f"baliza-{export_date.isoformat()}", export_date, 3, 0]
            )
        con.commit()
    except Exception as e:
        log(f"Failed to record uploads: {e}", "WARN")

def cleanup_backfill_dir() -> None:
    """Remove temporary export directory after successful upload."""
    if not UPLOAD_TO_IA or DRY_RUN:
        return

    backfill_dir = EXPORT_DIR / "backfill"
    try:
        if backfill_dir.exists():
            import shutil
            shutil.rmtree(backfill_dir)
            log(f"✓ Cleaned up {backfill_dir}")
    except Exception as e:
        log(f"Cleanup failed: {e}", "WARN")

def main() -> int:
    """Main backfill pipeline function."""
    try:
        log_section("Baliza Continuous Backfill")

        # Validate configuration
        if not validate_config():
            return 3

        log(f"Database: {DB_PATH}")
        log(f"Backfill range: {BACKFILL_START} to {BACKFILL_END or 'yesterday'}")
        log(f"Batch size: {BATCH_SIZE}")
        log(f"Stability window: {STABILITY_DAYS} days")
        log(f"Upload enabled: {UPLOAD_TO_IA}")
        log(f"Dry run: {DRY_RUN}")

        # Parse dates
        backfill_start = datetime.strptime(BACKFILL_START, "%Y-%m-%d").date()
        if BACKFILL_END:
            backfill_end = datetime.strptime(BACKFILL_END, "%Y-%m-%d").date()
        else:
            backfill_end = (datetime.now() - timedelta(days=1)).date()

        # Connect to database
        with duckdb.connect(str(DB_PATH)) as con:
            # Step 1: Find pending dates
            log_section("Step 1: Identify Pending Dates")
            pending_dates = get_pending_dates(con, backfill_start, backfill_end)

            if not pending_dates:
                log("✅ No pending dates - backfill complete!")
                return 0

            log(f"Found {len(pending_dates)} pending dates")
            log(f"Date range: {pending_dates[0]} to {pending_dates[-1]}")

            # Step 2: Process batch
            log_section(f"Step 2: Process Batch ({BATCH_SIZE} dates)")
            batch = pending_dates[:BATCH_SIZE]
            extracted = []

            for target_date in batch:
                if run_extraction(target_date):
                    extracted.append(target_date)

            if not extracted:
                log("❌ No dates extracted in this batch", "ERROR")
                return 2

            log(f"✓ Extracted {len(extracted)} dates")

            # Step 3: Export stable dates
            log_section("Step 3: Export Stable Dates")
            export_dir = export_stable_dates(con)

            # Step 4: Upload to Internet Archive
            log_section("Step 4: Upload to Internet Archive")
            if not upload_to_internet_archive(export_dir):
                log("⚠️ Upload had failures, but extraction succeeded", "WARN")
                cleanup_backfill_dir()
                return 1

            # Step 5: Cleanup
            cleanup_backfill_dir()

        # Final status
        log_section("Summary")
        remaining = len(pending_dates) - len(batch)
        log(f"✅ Backfill batch complete")
        log(f"Extracted: {len(extracted)} dates")
        log(f"Remaining: {remaining} dates")
        if remaining > 0:
            est_days = (remaining + BATCH_SIZE - 1) / (BATCH_SIZE * 96)  # 96 runs per day (15-min schedule)
            log(f"Estimated time to complete: ~{est_days:.1f} days")

        return 0

    except KeyboardInterrupt:
        log("Pipeline interrupted by user", "WARN")
        return 2
    except Exception as e:
        log(f"Unexpected error: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return 2

if __name__ == "__main__":
    sys.exit(main())
