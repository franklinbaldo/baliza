"""Parquet builder: download raw ZIP from IA, ingest into DuckDB, export and upload Parquet.

Reads the ZIP that ``mirror`` deposited on Internet Archive and produces the
canonical monthly Parquet file.  Engine is created fresh per month (stateless,
in-memory DuckDB) so this step can run independently of the fetch phase.
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import structlog

from .daily_exporter import SCHEMA_VERSION
from .engine import BalizaEngine
from .extractor import PNCPExtractor
from .ia_uploader import IAUploader, try_read_manifest_from_ia

logger = structlog.get_logger()


def _pending_build_months(
    start_date: date,
    batch_size: int | None,
    *,
    backfill: bool = False,
) -> list[date]:
    """Return months that need a Parquet build, newest-first.

    Normal mode (backfill=False):
        Months with ``mirror_uploaded_at`` set but ``parquet_uploaded_at`` empty.
        This covers only months that went through the new two-phase pipeline.

    Backfill mode (backfill=True):
        Months with ``raw_zip_url`` set and ``parquet_schema_version != SCHEMA_VERSION``
        (or Parquet missing).  This includes months uploaded via the old monolithic
        ``sync`` command that already have a ZIP on IA.
    """
    raw_manifest = try_read_manifest_from_ia()
    today = date.today()
    last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    start = start_date.replace(day=1)

    pending: list[date] = []
    for row in raw_manifest:
        part = row.get("data_particao") or ""
        if not part:
            continue
        try:
            month_date = datetime.strptime(part, "%Y-%m").date()
        except ValueError:
            continue

        if month_date < start or month_date > last_month:
            continue

        if not row.get("raw_zip_url"):
            continue  # no ZIP on IA — nothing to build from

        if backfill:
            # Rebuild if schema version is absent or outdated
            if row.get("parquet_schema_version") != SCHEMA_VERSION:
                pending.append(month_date)
        elif row.get("mirror_uploaded_at") and not row.get("parquet_uploaded_at"):
            # Only process months that went through mirror but have not yet been built
            pending.append(month_date)

    pending.sort(reverse=True)
    return pending[:batch_size] if batch_size else pending


def _download_raw_zip(item_id: str, month_str: str, dest: Path) -> Path:
    """Download raw-{month_str}.zip from IA to dest directory.  Returns the zip path."""
    zip_url = f"https://archive.org/download/{item_id}/raw-{month_str}.zip"
    zip_path = dest / f"raw-{month_str}.zip"
    with httpx.Client(follow_redirects=True, timeout=120.0) as client:
        with client.stream("GET", zip_url) as resp:
            resp.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
    return zip_path


def build_month(
    start_of_month: date,
    *,
    ia_access_key: str,
    ia_secret_key: str,
    dry_run: bool = False,
    log_fn: object = None,
) -> dict[str, object]:
    """Download the raw ZIP from IA, ingest into DuckDB, export and upload Parquet.

    Args:
        start_of_month: First day of the target month.
        ia_access_key: IA S3-like access key.
        ia_secret_key: IA S3-like secret key.
        dry_run: Skip actual upload (ingest and export only).
        log_fn: Optional callable(str) for progress messages.

    Returns:
        Dict with keys: ``month``, ``valid``, ``quarantine``, ``uploaded``.
    """
    month_str = start_of_month.strftime("%Y-%m")
    item_id = f"baliza-pncp-{month_str}"

    def _emit(msg: str) -> None:
        if log_fn is not None:
            log_fn(msg)

    result: dict[str, object] = {
        "month": month_str,
        "valid": 0,
        "quarantine": 0,
        "uploaded": False,
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # 1. Download ZIP from IA
        _emit(f"{month_str}: downloading raw ZIP from IA...")
        try:
            zip_path = _download_raw_zip(item_id, month_str, tmp_path)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                _emit(f"{month_str}: no raw ZIP on IA (404) — skipping")
                return result
            raise

        # 2. Extract to raw_dir structure
        raw_dir = tmp_path / "raw" / month_str
        raw_dir.mkdir(parents=True)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(raw_dir)

        # Temporarily move raw_dir to data/raw/{month_str} so PNCPExtractor
        # ingest_range (which hardcodes data/raw/{month_str}) can find the files.
        local_raw = Path("data/raw") / month_str
        local_raw.parent.mkdir(parents=True, exist_ok=True)
        if local_raw.exists():
            shutil.rmtree(local_raw)
        shutil.move(str(raw_dir), str(local_raw))

        try:
            # 3. Ingest into fresh in-memory DuckDB
            _emit(f"{month_str}: ingesting...")
            engine = BalizaEngine()
            uploader = IAUploader(engine=engine)

            month_start_dt = datetime.combine(start_of_month, datetime.min.time())
            with PNCPExtractor(engine=engine) as extractor:
                stats = extractor.ingest_range(month_start_dt)
                result["valid"] = stats.get("valid", 0)
                result["quarantine"] = stats.get("quarantine", 0)

                q_csv = Path(f"data/quarentena-{month_str}.csv")
                has_q = extractor.export_quarantine(month_start_dt, q_csv)

            if dry_run:
                _emit(
                    f"[dry-run] {month_str}: {result['valid']} records, "
                    f"{result['quarantine']} quarantined"
                )
                return result

            # 4. Export Parquet and upload
            _emit(f"{month_str}: uploading Parquet...")
            uploaded = uploader.upload_parquet(
                start_of_month,
                ia_access_key,
                ia_secret_key,
                quarantine_stats=stats,
                quarantine_csv=q_csv if has_q else None,
                schema_version=SCHEMA_VERSION,
            )
            result["uploaded"] = uploaded

            if q_csv.exists():
                q_csv.unlink()

        finally:
            # Always clean up the temp raw pages
            if local_raw.exists():
                shutil.rmtree(local_raw)

    return result
