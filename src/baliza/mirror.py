"""Raw mirror: fetch PNCP JSON pages and upload monthly ZIPs to Internet Archive.

No DuckDB, no Parquet — purely a data capture step. The resulting ZIP at
archive.org/download/baliza-pncp-{YYYY-MM}/raw-{YYYY-MM}.zip is a navigable
mirror of the PNCP API responses (each contratos_p{N}.json accessible as a
direct URL).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

import structlog

from .constants import RESOURCE_CONTRATOS, clamp_to_known_data_start_month
from .extractor import FETCHED_SENTINEL, PNCPExtractor, _validate_resource
from .ia_uploader import IAUploader, read_manifest_from_ia

logger = structlog.get_logger()


def _pending_mirror_months(
    start_date: date,
    batch_size: int | None,
    *,
    resource: str = RESOURCE_CONTRATOS,
) -> list[date]:
    """Return months not yet mirrored (no raw_zip_url in manifest), newest-first."""
    raw_manifest = read_manifest_from_ia()  # strict: raises ManifestReadError on failure
    # A month is "mirrored" if it has a non-empty raw_zip_url in its canonical row.
    mirrored: set[str] = {
        row["data_particao"]
        for row in raw_manifest
        if row.get("data_particao") and row.get("raw_zip_url")
    }

    today = date.today()
    last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    start = clamp_to_known_data_start_month(resource, start_date)

    pending: list[date] = []
    curr = start
    while curr <= last_month:
        if curr.strftime("%Y-%m") not in mirrored:
            pending.append(curr)
        if curr.month == 12:
            curr = curr.replace(year=curr.year + 1, month=1)
        else:
            curr = curr.replace(month=curr.month + 1)

    pending.sort(reverse=True)
    return pending[:batch_size] if batch_size else pending


def mirror_month(  # noqa: PLR0912, PLR0913, PLR0915
    start_of_month: date,
    *,
    ia_access_key: str,
    ia_secret_key: str,
    use_curl: bool = False,
    dry_run: bool = False,
    log_fn: object = None,
) -> dict[str, object]:
    """Fetch all PNCP JSON pages for a month, zip them, and upload to IA.

    Args:
        start_of_month: First day of the target month.
        ia_access_key: IA S3-like access key.
        ia_secret_key: IA S3-like secret key.
        use_curl: Use system cURL instead of httpx.
        dry_run: Skip actual upload (verify only).
        log_fn: Optional callable(str) for progress messages.

    Returns:
        Dict with keys: ``month``, ``pages_fetched``, ``pages_cached``, ``uploaded``.
    """
    _validate_resource(RESOURCE_CONTRATOS)

    month_str = start_of_month.strftime("%Y-%m")
    if start_of_month.month == 12:
        next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
    else:
        next_month = start_of_month.replace(month=start_of_month.month + 1)
    end_of_month = next_month - timedelta(days=1)

    month_start_dt = datetime.combine(start_of_month, datetime.min.time())
    month_end_dt = datetime.combine(end_of_month, datetime.min.time())

    raw_month_dir = Path("data/raw") / month_str
    sentinel = raw_month_dir / FETCHED_SENTINEL

    def _emit(msg: str) -> None:
        if log_fn is not None:
            log_fn(msg)

    result: dict[str, object] = {
        "month": month_str,
        "pages_fetched": 0,
        "pages_cached": 0,
        "uploaded": False,
    }

    # engine=None — fetch-only path, no DuckDB
    with PNCPExtractor(engine=None, use_curl=use_curl) as extractor:

        def _page_is_cached(p: int) -> bool:
            path = raw_month_dir / f"contratos_p{p}.json"
            if not path.exists() or path.stat().st_size == 0:
                return False
            try:
                with open(path) as fh:
                    data = json.load(fh)
            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("corrupt_cache_found", file=str(path), error=str(e))
                try:
                    path.unlink()
                except OSError:
                    pass
                return False
            if isinstance(data, dict) and ("data" in data or "totalPaginas" in data):
                return True
            logger.warning(
                "corrupt_cache_found",
                file=str(path),
                error="schema mismatch (no 'data' or 'totalPaginas' key)",
            )
            try:
                path.unlink()
            except OSError:
                pass
            return False

        # Determine total pages (probe or sentinel shortcut)
        total_pages: int | None = None
        if sentinel.exists():
            p1 = raw_month_dir / "contratos_p1.json"
            if p1.exists() and p1.stat().st_size > 0:
                try:
                    with open(p1) as fh:
                        _d = json.load(fh)
                    if isinstance(_d, dict) and isinstance(_d.get("totalPaginas"), int):
                        total_pages = _d["totalPaginas"]
                except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                    pass
            if total_pages is None:
                logger.warning("sentinel_cache_regressed", month=month_str, reason="page1_bad")
                try:
                    sentinel.unlink()
                except OSError:
                    pass

        if total_pages is None:
            res = extractor.probe_range(RESOURCE_CONTRATOS, month_start_dt, month_end_dt)
            total_pages = res["total_pages"]

        missing_pages = [p for p in range(1, total_pages + 1) if not _page_is_cached(p)]
        cached_count = total_pages - len(missing_pages)
        result["pages_cached"] = cached_count

        if sentinel.exists() and missing_pages:
            logger.warning(
                "sentinel_cache_regressed",
                month=month_str,
                reason="missing_pages",
                pages_missing=len(missing_pages),
            )
            try:
                sentinel.unlink()
            except OSError:
                pass

        for p in missing_pages:
            extractor.fetch_page(RESOURCE_CONTRATOS, month_start_dt, month_end_dt, p)
            result["pages_fetched"] = int(result["pages_fetched"]) + 1
            _emit(f"{month_str} page {p}/{total_pages}")

        # Write sentinel so next run can skip probe
        raw_month_dir.mkdir(parents=True, exist_ok=True)
        sentinel.touch()

    if dry_run:
        _emit(f"[dry-run] {month_str}: {total_pages} pages ready for zipping")
        return result

    uploader = IAUploader(engine=None)
    uploaded = uploader.upload_raw_zip(
        start_of_month, raw_month_dir, ia_access_key, ia_secret_key
    )
    result["uploaded"] = uploaded
    return result
