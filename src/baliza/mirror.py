"""Raw mirror: fetch PNCP JSON pages and upload monthly ZIPs to Internet Archive.

No DuckDB, no Parquet — purely a data capture step. All ZIPs land in the single
item baliza-pncp-raw as raw-{YYYY-MM}.zip, making the full history discoverable
from one URL: archive.org/details/baliza-pncp-raw
"""

from __future__ import annotations

import itertools
import json
from datetime import date, datetime
from pathlib import Path

import structlog

from .constants import RESOURCE_CONTRATOS
from .extractor import FETCHED_SENTINEL, PNCPExtractor, _validate_resource
from .ia_uploader import IAUploader, read_manifest_from_ia
from .partitioning import (
    clamp_to_data_start,
    current_partition_start,
    iter_partitions,
    partition_for,
    previous_partition_start,
)
from .resources import get_resource

logger = structlog.get_logger()


def _pending_mirror_months(
    start_date: date,
    batch_size: int | None,
    *,
    resource: str = RESOURCE_CONTRATOS,
) -> list[date]:
    """Return partition start-dates to mirror, newest-first.

    Past partitions are included only when they have no ``raw_zip_url``
    in the manifest. The current partition is always included — its ZIP
    grows daily (monthly resources) or as new annual updates land.

    Drift-D wiring: iteration now goes through ``iter_partitions``
    instead of hardcoding ``relativedelta(months=1)`` steps. Behavior
    is byte-identical for monthly resources; annual resources (PCA
    once promoted) walk one period per calendar year automatically.
    The historical ``_months`` name is kept to avoid churn at every
    call site — the function returns partition starts regardless of
    cadence.
    """
    raw_manifest = read_manifest_from_ia()  # strict: raises ManifestReadError on failure
    resource_obj = get_resource(resource)
    # A past partition is "done" when it has a non-empty raw_zip_url
    # for the selected resource. Without the table_name filter,
    # contratos rows would mark partitions as mirrored and skip the
    # atas backfill entirely.
    mirrored: set[str] = {
        row["data_particao"]
        for row in raw_manifest
        if row.get("data_particao")
        and row.get("raw_zip_url")
        and row.get("table_name") == resource
    }

    current = current_partition_start(resource_obj)
    last_complete = previous_partition_start(resource_obj)
    start = clamp_to_data_start(resource_obj, start_date)

    pending: list[date] = []

    # Past partitions: only if not yet mirrored. iter_partitions
    # consults the resource's PartitionStrategy for the step size.
    if start <= last_complete:
        for period in iter_partitions(resource_obj, start, last_complete):
            if period.label not in mirrored:
                pending.append(period.start)

    # Current partition: always include (incremental updates).
    pending.append(current)

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
    is_current_month: bool | None = None,
    resource: str = RESOURCE_CONTRATOS,
) -> dict[str, object]:
    """Fetch all PNCP JSON pages for a month, zip them, and upload to IA.

    Args:
        start_of_month: First day of the target month.
        ia_access_key: IA S3-like access key.
        ia_secret_key: IA S3-like secret key.
        use_curl: Use system cURL instead of httpx.
        dry_run: Skip actual upload (verify only).
        log_fn: Optional callable(str) for progress messages.
        is_current_month: When True, keeps local page cache after upload (so
            tomorrow's run only fetches new pages) and removes the sentinel so
            the next run re-probes totalPaginas from the API. Auto-detected
            when None (default).
        resource: PNCP resource name (default 'contratos'). Determines the
            per-page raw filename and which API endpoint is queried.

    Returns:
        Dict with keys: ``month``, ``pages_fetched``, ``pages_cached``, ``uploaded``.
    """
    _validate_resource(resource)
    resource_obj = get_resource(resource)

    # Drift-D wiring: partition window comes from the resource's
    # PartitionStrategy. For monthly the period is one calendar
    # month (identical to the legacy strftime/relativedelta math);
    # for annual it spans Jan 1 .. Dec 31 of the year automatically.
    period = partition_for(resource_obj, start_of_month)
    month_str = period.label  # kept the variable name for diff hygiene
    end_of_month = period.end

    if is_current_month is None:
        is_current_month = period.start == current_partition_start(resource_obj)

    month_start_dt = datetime.combine(period.start, datetime.min.time())
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

    # Drift-B publicacoes wiring: when the resource declares
    # required_params, we fan out the page iteration across every
    # combination. Resources without required_params (contratos, atas)
    # iterate exactly once with extra_params={} — byte-identical to the
    # pre-fan-out shape because the extractor's _param_suffix returns
    # "" for empty params, leaving the cache filenames unchanged.
    required_params = resource_obj.fetch.required_params or {}
    if required_params:
        keys = sorted(required_params)
        combos: list[dict[str, str | int]] = [
            dict(zip(keys, vals, strict=True))
            for vals in itertools.product(*(required_params[k] for k in keys))
        ]
    else:
        combos = [{}]

    def _combo_suffix(combo: dict[str, str | int]) -> str:
        if not combo:
            return ""
        return "_" + "_".join(f"{k}{v}" for k, v in sorted(combo.items()))

    # engine=None — fetch-only path, no DuckDB
    with PNCPExtractor(engine=None, use_curl=use_curl) as extractor:

        def _combo_page_filename(combo: dict[str, str | int], p: int) -> str:
            return f"{resource}{_combo_suffix(combo)}_p{p}.json"

        def _page_is_cached(combo: dict[str, str | int], p: int) -> bool:
            path = raw_month_dir / _combo_page_filename(combo, p)
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

        for combo in combos:
            extra_params = combo or None
            combo_label = _combo_suffix(combo) or "(default)"

            total_pages: int | None = None
            # Sentinel + page-1 shortcut only applies in the no-fanout
            # case today — the sentinel is a single per-month flag and
            # would lie about freshness for a multi-combo resource.
            # Skip it for fan-out resources; their probe is cheap (one
            # request per combo).
            if sentinel.exists() and not combo:
                p1 = raw_month_dir / _combo_page_filename(combo, 1)
                if p1.exists() and p1.stat().st_size > 0:
                    try:
                        with open(p1) as fh:
                            _d = json.load(fh)
                        if isinstance(_d, dict) and isinstance(_d.get("totalPaginas"), int):
                            total_pages = _d["totalPaginas"]
                    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.debug(
                            "page1_metadata_load_failed", month=month_str, error=str(e),
                        )
                if total_pages is None:
                    logger.warning(
                        "sentinel_cache_regressed", month=month_str, reason="page1_bad",
                    )
                    try:
                        sentinel.unlink()
                    except OSError:
                        pass

            if total_pages is None:
                try:
                    res = extractor.probe_range(
                        resource, month_start_dt, month_end_dt, extra_params=extra_params,
                    )
                except Exception as exc:  # noqa: BLE001
                    # A single fan-out value failing (e.g. modalidade
                    # 14 returns 204 / empty) shouldn't tank the
                    # whole partition — log and move on.
                    logger.warning(
                        "fanout_probe_failed",
                        month=month_str, combo=combo_label, error=str(exc),
                    )
                    continue
                total_pages = res["total_pages"]

            if is_current_month:
                cached_page_nums = [
                    p for p in range(1, total_pages + 1) if _page_is_cached(combo, p)
                ]
                if cached_page_nums:
                    last_cached = max(cached_page_nums)
                    last_cached_path = raw_month_dir / _combo_page_filename(combo, last_cached)
                    try:
                        last_cached_path.unlink()
                        logger.info(
                            "current_month_last_page_invalidated",
                            month=month_str, combo=combo_label, page=last_cached,
                        )
                    except OSError:
                        pass

            missing_pages = [
                p for p in range(1, total_pages + 1) if not _page_is_cached(combo, p)
            ]
            result["pages_cached"] = (
                int(result["pages_cached"]) + (total_pages - len(missing_pages))
            )

            for p in missing_pages:
                try:
                    extractor.fetch_page(
                        resource, month_start_dt, month_end_dt, p,
                        extra_params=extra_params,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "fanout_page_fetch_failed",
                        month=month_str, combo=combo_label, page=p, error=str(exc),
                    )
                    continue
                result["pages_fetched"] = int(result["pages_fetched"]) + 1
                _emit(f"{month_str} {combo_label} page {p}/{total_pages}")

        # Write sentinel so next run can skip probe (no-fanout only).
        raw_month_dir.mkdir(parents=True, exist_ok=True)
        if not required_params:
            sentinel.touch()

    if dry_run:
        _emit(f"[dry-run] {month_str}: {total_pages} pages ready for zipping")
        return result

    uploader = IAUploader(engine=None)
    uploaded = uploader.upload_raw_zip(
        start_of_month,
        raw_month_dir,
        ia_access_key,
        ia_secret_key,
        keep_raw_dir=is_current_month,
        resource=resource,
    )
    result["uploaded"] = uploaded

    # For the current month: remove the sentinel after upload so the next
    # daily run re-probes totalPaginas and picks up newly published pages.
    if is_current_month and uploaded:
        try:
            sentinel.unlink(missing_ok=True)
        except OSError:
            pass

    return result
