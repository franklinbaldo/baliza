#!/usr/bin/env python3
"""Daily Baliza extraction, export, and upload to Internet Archive.

This script:
1. Extracts yesterday's data from PNCP API
2. Exports to Parquet (contratos, orgaos, unidades)
3. Uploads to Internet Archive
4. Records metadata in database

Exit codes:
  0 - Success
  1 - Partial failure (extraction or export succeeded but upload failed)
  2 - Complete failure (extraction or export failed)
  3 - Configuration error (missing credentials)
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
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
IA_BUCKET = os.getenv("IA_BUCKET", "jules-mail-frank-2026")  # Default to existing bucket
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT_URL", "https://s3.us.archive.org")
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
    if UPLOAD_TO_IA:
        if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
            log("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required", "ERROR")
            return False
    return True

def run_command(cmd: list[str], description: str) -> tuple[bool, str]:
    """Run a shell command and return success status and output."""
    log(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
        )
        if result.returncode != 0:
            log(f"{description} failed with code {result.returncode}", "ERROR")
            log(f"stderr: {result.stderr}", "ERROR")
            return False, result.stderr
        log(f"✓ {description}")
        return True, result.stdout
    except subprocess.TimeoutExpired:
        log(f"{description} timed out", "ERROR")
        return False, "Timeout"
    except Exception as e:
        log(f"{description} error: {e}", "ERROR")
        return False, str(e)

def extract_data(yesterday: datetime) -> bool:
    """Extract yesterday's data from PNCP API."""
    log_section("STEP 1: Extract Data from PNCP API")

    start_date = yesterday.strftime("%Y-%m-%d")
    end_date = (yesterday + timedelta(days=1)).strftime("%Y-%m-%d")

    log(f"Extracting data from {start_date} to {end_date}")

    cmd = [
        sys.executable, "-m", "baliza.cli_simple",
        "extract",
        "--start", start_date,
        "--end", end_date,
        "--duckdb", str(DB_PATH),
    ]

    success, output = run_command(cmd, "Data extraction")
    if success:
        # Parse output for row count
        for line in output.split("\n"):
            if "Rows:" in line:
                log(line.strip())
    return success

def export_data(yesterday: datetime) -> Optional[Path]:
    """Export yesterday's data to Parquet."""
    log_section("STEP 2: Export Data to Parquet")

    date_str = yesterday.strftime("%Y-%m-%d")
    log(f"Exporting {date_str} to Parquet")

    cmd = [
        sys.executable, "-m", "baliza.cli_simple",
        "export-daily",
        "--date", date_str,
        "--output", str(EXPORT_DIR),
        "--duckdb", str(DB_PATH),
    ]

    success, output = run_command(cmd, "Data export")
    if success:
        # Parse output for row counts
        for line in output.split("\n"):
            if "rows" in line.lower():
                log(line.strip())
        return EXPORT_DIR / date_str
    return None

def upload_to_internet_archive(data_dir: Path, yesterday: datetime) -> bool:
    """Upload exported data to Internet Archive."""
    log_section("STEP 3: Upload to Internet Archive")

    if not UPLOAD_TO_IA:
        log("Upload to IA disabled (BALIZA_UPLOAD_TO_IA=false)")
        return True

    if DRY_RUN:
        log("DRY RUN: Skipping actual upload")
        return True

    date_str = yesterday.strftime("%Y-%m-%d")
    success = True

    # Upload each parquet file
    for parquet_file in data_dir.glob("*.parquet"):
        log(f"Uploading {parquet_file.name}")

        if not upload_file_to_s3(parquet_file, date_str):
            log(f"Failed to upload {parquet_file.name}", "WARN")
            success = False
        else:
            log(f"✓ Uploaded {parquet_file.name}")

    # Upload metadata
    metadata_file = data_dir / "_metadata.json"
    if metadata_file.exists():
        log(f"Uploading {metadata_file.name}")
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

        # Calculate MD5
        file_md5 = base64.b64encode(md5(file_data).digest()).decode()

        headers = {
            "Authorization": f"LOW {AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}",
            "Content-MD5": file_md5,
            "Content-Type": "application/octet-stream",
            "x-amz-meta-date": date_str,
            "x-amz-meta-source": "https://pncp.gov.br/api/consulta/v1",
        }

        url = f"{AWS_S3_ENDPOINT_URL}/{IA_BUCKET}/{file_path.name}"
        response = requests.put(url, data=file_data, headers=headers, timeout=120)

        if response.status_code == 200:
            return True
        else:
            log(f"S3 upload failed: {response.status_code} {response.text[:200]}", "ERROR")
            return False
    except Exception as e:
        log(f"Upload error: {e}", "ERROR")
        return False

def record_metadata(yesterday: datetime, export_dir: Path, success: bool) -> None:
    """Record upload metadata in database."""
    try:
        metadata_file = export_dir / "_metadata.json"
        if not metadata_file.exists():
            return

        with open(metadata_file) as f:
            metadata = json.load(f)

        con = duckdb.connect(str(DB_PATH))
        con.execute("CREATE SCHEMA IF NOT EXISTS baliza_state")
        con.execute("""
            CREATE TABLE IF NOT EXISTS baliza_state.uploaded_to_ia (
                item_id VARCHAR PRIMARY KEY,
                extraction_date DATE,
                uploaded_at TIMESTAMP,
                file_count INTEGER,
                total_rows INTEGER,
                status VARCHAR
            )
        """)

        total_rows = sum(t.get("row_count", 0) for t in metadata["tables"].values())
        item_id = f"baliza-{yesterday.strftime('%Y-%m-%d')}"

        con.execute(
            """
            INSERT OR REPLACE INTO baliza_state.uploaded_to_ia
            VALUES (?, ?, NOW(), ?, ?)
        """,
            [item_id, yesterday.date(), 3, total_rows]
        )
        con.close()
    except Exception as e:
        log(f"Failed to record metadata: {e}", "WARN")

def cleanup(export_dir: Path) -> None:
    """Cleanup temporary files if configured."""
    cleanup_enabled = os.getenv("BALIZA_CLEANUP_AFTER_UPLOAD", "true").lower() == "true"
    if not cleanup_enabled or DRY_RUN:
        return

    log_section("STEP 4: Cleanup")
    try:
        import shutil
        if export_dir.exists():
            shutil.rmtree(export_dir.parent)
            log(f"✓ Cleaned up {export_dir.parent}")
    except Exception as e:
        log(f"Cleanup failed: {e}", "WARN")

def main() -> int:
    """Main pipeline function."""
    try:
        # Validate configuration
        if not validate_config():
            return 3

        log_section("Baliza Daily Pipeline")
        log(f"Database: {DB_PATH}")
        log(f"Export directory: {EXPORT_DIR}")
        log(f"IA Bucket: {IA_BUCKET}")
        log(f"Upload enabled: {UPLOAD_TO_IA}")
        log(f"Dry run: {DRY_RUN}")

        # Calculate yesterday
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")
        log(f"Processing date: {date_str}")

        # Step 1: Extract
        if not extract_data(yesterday):
            return 2

        # Step 2: Export
        export_dir = export_data(yesterday)
        if not export_dir:
            return 2

        # Step 3: Upload
        upload_success = upload_to_internet_archive(export_dir, yesterday)

        # Step 4: Record metadata
        record_metadata(yesterday, export_dir, upload_success)

        # Step 5: Cleanup
        cleanup(export_dir)

        # Final status
        log_section("Summary")
        if upload_success:
            log("✅ Pipeline completed successfully")
            if UPLOAD_TO_IA:
                item_id = f"baliza-{date_str}"
                log(f"View on Internet Archive: https://archive.org/details/{IA_BUCKET}")
            return 0
        else:
            log("⚠️  Pipeline completed with warnings (upload failed)")
            return 1

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
