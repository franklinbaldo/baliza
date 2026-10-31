"""Reconcile live pagination without discarding successfully captured full pages."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from .coverage import IncompletePageError, modalidade_coverage
from .extractor import PNCPExtractor

logger = structlog.get_logger()


class PaginationUnstableError(RuntimeError):
    """The bounded reconciliation budget was exhausted; cached pages are retained."""


def capture_modalidade(  # noqa: PLR0912, PLR0913
    extractor: PNCPExtractor,
    raw_dir: Path,
    combo: dict[str, str | int],
    start: datetime,
    end: datetime,
    emit: Callable[[str], None],
    *,
    max_rounds: int = 3,
) -> tuple[dict[str, Any], int, int]:
    """Require stable before/after totals and exact page counts before certifying.

    A fresh probe writes page 1. Re-fetch the previous tail, missing/malformed
    pages and any specifically rejected page. Never erase a whole modalidade.
    Network failures and duplicate records still fail closed immediately.
    """
    if max_rounds < 1:
        raise ValueError("max_rounds must be positive")
    suffix = "_" + "_".join(f"{k}{v}" for k, v in sorted(combo.items()))

    def path_for(page: int) -> Path:
        return raw_dir / f"publicacoes{suffix}_p{page}.json"

    def cached(page: int) -> bool:
        try:
            return isinstance(json.loads(path_for(page).read_text()).get("data"), list)
        except (OSError, ValueError, AttributeError):
            return False

    fetched = 0
    reason = "pagination totals changed"
    rejected: int | None = None
    for round_number in range(1, max_rounds + 1):
        # Capture the old tail before the fresh probe overwrites page 1.
        existing = [int(p.stem.rsplit("_p", 1)[1])
                    for p in raw_dir.glob(f"publicacoes{suffix}_p*.json")]
        old_tail = max(existing, default=1)
        path_for(1).unlink(missing_ok=True)
        before = extractor.probe_range("publicacoes", start, end, extra_params=combo)
        pages, records = before["total_pages"], before["total_registries"]
        if not isinstance(pages, int) or not isinstance(records, int):
            raise ValueError("Invalid pagination metadata")
        if pages < 0 or records < 0 or pages != (records + 49) // 50:
            raise ValueError("Inconsistent pagination totals")
        for page in range(1, pages + 1):
            if page != 1 and (page in (old_tail, rejected) or not cached(page)):
                path_for(page).unlink(missing_ok=True)
            if not cached(page):
                extractor.fetch_page("publicacoes", start, end, page, extra_params=combo)
                fetched += 1
                emit(f"{raw_dir.name} {suffix} page {page}/{pages}")
        # Probe again over the network, not via the extractor's page-1 cache.
        path_for(1).unlink(missing_ok=True)
        after = extractor.probe_range("publicacoes", start, end, extra_params=combo)
        if before != after:
            reason = f"totals changed: {before} -> {after}"
        else:
            try:
                coverage = modalidade_coverage(raw_dir, combo, pages, records)
            except IncompletePageError as error:
                rejected = error.page
                reason = str(error)
            else:
                wanted = {path_for(p).name for p in range(1, pages + 1)}
                for stale in raw_dir.glob(f"publicacoes{suffix}_p*.json"):
                    if stale.name not in wanted:
                        stale.unlink()
                return coverage, fetched, max(0, pages - fetched)
        logger.warning("publicacoes_pagination_unstable", combo=combo,
                       round=round_number, reason=reason)
    raise PaginationUnstableError(
        f"Pagination remained unstable after {max_rounds} rounds for {combo}: "
        f"{reason}; cache retained"
    )
