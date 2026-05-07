"""Parquet builder: download raw ZIP from IA, ingest into DuckDB, export and upload Parquet.

Reads the ZIP that ``mirror`` deposited on Internet Archive and produces the
canonical monthly Parquet file.  Engine is created fresh per month (stateless,
in-memory DuckDB) so this step can run independently of the fetch phase.
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx
import structlog

from .constants import RESOURCE_CONTRATOS
from .daily_exporter import SCHEMA_VERSION
from .engine import BalizaEngine
from .extractor import PNCPExtractor
from .ia_uploader import IAUploader, read_manifest_from_ia
from .partitioning import (
    clamp_to_data_start,
    current_partition_start,
    parse_partition_label,
    partition_for,
    previous_partition_start,
)
from .resources import get_resource
from .resources.specs import PartitionStrategy

logger = structlog.get_logger()


def _pending_build_months(
    start_date: date,
    batch_size: int | None,
    *,
    resource: str = RESOURCE_CONTRATOS,
    backfill: bool = False,
    manifest: list[dict] | None = None,
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
    raw_manifest = manifest if manifest is not None else read_manifest_from_ia()
    resource_obj = get_resource(resource)
    # Drift-D wiring: partition floor / ceiling come from the
    # resource's PartitionStrategy.
    # Monthly resources use previous_partition_start — the current
    # month is still receiving daily updates so building it mid-month
    # would produce a partial canonical.
    # Annual resources use current_partition_start — PCA data is
    # published year-round; a weekly build should always snapshot the
    # current year's rows rather than lagging a full calendar year
    # waiting for the next January (Codex P1, PR #576).
    if resource_obj.raw_dataset.partition_strategy == PartitionStrategy.ANNUAL:
        last_complete = current_partition_start(resource_obj)
    else:
        last_complete = previous_partition_start(resource_obj)
    start = clamp_to_data_start(resource_obj, start_date)

    pending_set: set[date] = set()
    for row in raw_manifest:
        # Per-resource scoping: ignore rows that belong to a different
        # canonical table so contratos rows don't trigger atas rebuilds
        # (and vice versa) once the manifest carries multiple resources.
        if row.get("table_name") != resource:
            continue
        part = row.get("data_particao") or ""
        if not part:
            continue
        partition_start = parse_partition_label(resource_obj, part)
        if partition_start is None:
            # Label written under a different partitioning scheme (a
            # 'YYYY-MM' row on an annual resource, etc.) — skip
            # rather than treat it as the resource's current shape.
            continue

        if partition_start < start or partition_start > last_complete:
            continue

        if not row.get("raw_zip_url"):
            continue  # no ZIP on IA — nothing to build from

        if backfill:
            # Rebuild if schema version is absent or outdated
            if row.get("parquet_schema_version") != SCHEMA_VERSION:
                pending_set.add(partition_start)
        elif row.get("mirror_uploaded_at") and not row.get("parquet_uploaded_at"):
            # Only process partitions that went through mirror but have not yet been built
            pending_set.add(partition_start)

    pending = sorted(pending_set, reverse=True)
    return pending[:batch_size] if batch_size else pending


def _download_raw_zip(zip_url: str, month_str: str, dest: Path) -> Path:
    """Download raw-{month_str}.zip from zip_url to dest directory.  Returns the zip path."""
    zip_path = dest / f"raw-{month_str}.zip"
    with httpx.Client(follow_redirects=True, timeout=120.0) as client:
        with client.stream("GET", zip_url) as resp:
            resp.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
    return zip_path


def build_month(  # noqa: PLR0913, PLR0915
    start_of_month: date,
    *,
    ia_access_key: str,
    ia_secret_key: str,
    dry_run: bool = False,
    log_fn: object = None,
    manifest: list[dict] | None = None,
    resource: str = RESOURCE_CONTRATOS,
) -> dict[str, object]:
    """Download the raw ZIP from IA, ingest into DuckDB, export and upload Parquet.

    Args:
        start_of_month: First day of the target month.
        ia_access_key: IA S3-like access key.
        ia_secret_key: IA S3-like secret key.
        dry_run: Skip actual upload (ingest and export only).
        log_fn: Optional callable(str) for progress messages.
        manifest: Pre-fetched manifest rows. When None, fetches from IA.
            Pass this when building multiple months to avoid one network
            call per month.
        resource: PNCP resource name (default 'contratos'). Routes the
            ingestion through the resource's entity_model and canonical
            tables — see PNCPResource.

    Returns:
        Dict with keys: ``month``, ``valid``, ``quarantine``, ``uploaded``.
    """
    resource_obj = get_resource(resource)
    # Drift-D wiring: partition label comes from the resource's
    # PartitionStrategy. ``month_str`` keeps the legacy variable name
    # (and the result key below) for diff hygiene at every call site;
    # for annual resources it carries a YYYY label.
    month_str = partition_for(resource_obj, start_of_month).label

    def _emit(msg: str) -> None:
        if log_fn is not None:
            log_fn(msg)

    result: dict[str, object] = {
        "month": month_str,
        "valid": 0,
        "quarantine": 0,
        "uploaded": False,
    }

    # Resolve raw_zip_url from manifest — decoupled from item naming so it
    # works whether the ZIP lives in baliza-pncp-raw or a per-month item.
    # Scope by table_name so atas rows don't feed contratos builds (and
    # vice versa) once the manifest carries multiple resources.
    rows = manifest if manifest is not None else read_manifest_from_ia()
    manifest_row = next(
        (
            r
            for r in rows
            if r.get("data_particao") == month_str
            and r.get("table_name") == resource
            and r.get("raw_zip_url")
        ),
        None,
    )
    if not manifest_row:
        _emit(f"{month_str}: no raw_zip_url in manifest — skipping")
        return result
    raw_zip_url = manifest_row["raw_zip_url"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # 1. Download ZIP from IA
        _emit(f"{month_str}: downloading raw ZIP from {raw_zip_url}...")
        try:
            zip_path = _download_raw_zip(raw_zip_url, month_str, tmp_path)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                _emit(f"{month_str}: no raw ZIP on IA (404) — skipping")
                return result
            raise

        # 2. Extract to raw_dir structure — validate member paths to prevent traversal
        raw_dir = tmp_path / "raw" / month_str
        raw_dir.mkdir(parents=True)
        resolved_raw_dir = raw_dir.resolve()
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.infolist():
                target = (raw_dir / member.filename).resolve()
                if not target.is_relative_to(resolved_raw_dir):
                    raise ValueError(f"Unsafe ZIP entry (path traversal): {member.filename}")
                zf.extract(member, raw_dir)

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
                stats = extractor.ingest_range(month_start_dt, resource=resource)
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
                resource=resource,
            )
            result["uploaded"] = uploaded

            if q_csv.exists():
                q_csv.unlink()

        finally:
            # Always clean up the temp raw pages
            if local_raw.exists():
                shutil.rmtree(local_raw)

    return result
