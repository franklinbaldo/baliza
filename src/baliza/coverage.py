"""Evidence of complete publicacoes pagination, tied to the actual page bytes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .resources import get_resource


class IncompletePageError(ValueError):
    """A specific page disagrees with the current pagination probe."""

    def __init__(self, path: Path, page: int, expected: int, actual: int):
        self.path = path
        self.page = page
        self.expected = expected
        self.actual = actual
        super().__init__(f"Incomplete page {path.name}: expected {expected}, got {actual}")


def page_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def modalidade_coverage(
    raw_dir: Path,
    combo: dict[str, str | int],
    pages: int,
    expected_records: int,
) -> dict[str, Any]:
    if not isinstance(pages, int) or not isinstance(expected_records, int):
        raise ValueError("Invalid pagination metadata")
    if pages < 0 or expected_records < 0 or pages != (expected_records + 49) // 50:
        raise ValueError("Inconsistent pagination totals")
    suffix = "_" + "_".join(f"{k}{v}" for k, v in sorted(combo.items()))
    records = 0
    dates: list[str] = []
    files = {}
    seen_ids: set[str] = set()
    for page in range(1, pages + 1):
        path = raw_dir / f"publicacoes{suffix}_p{page}.json"
        data = json.loads(path.read_text())
        rows = data["data"]
        if not isinstance(rows, list):
            raise ValueError(f"Invalid records in {path.name}")
        expected_page_size = min(50, expected_records - (page - 1) * 50)
        if len(rows) != expected_page_size:
            raise IncompletePageError(path, page, expected_page_size, len(rows))
        # Cached full pages can predate the latest probe: their historical total
        # metadata is not the new snapshot's total. Validate the actual records.
        for row in rows:
            identifier = row.get("numeroControlePNCP")
            if identifier:
                if identifier in seen_ids:
                    raise ValueError(f"Duplicate publication across pages: {identifier}")
                seen_ids.add(identifier)
        records += len(rows)
        dates.extend(
            str(row["dataPublicacaoPncp"])[:10] for row in rows if row.get("dataPublicacaoPncp")
        )
        files[path.name] = page_digest(path)
    if records != expected_records:
        raise ValueError(f"Incomplete modalidade {combo}: {records}/{expected_records} records")
    return {
        "params": combo,
        "pages": pages,
        "expected_records": expected_records,
        "records": records,
        "last_publication_date": max(dates, default=None),
        "files": files,
        "complete": True,
    }


def validate_coverage(raw_dir: Path, partition: str) -> dict[str, Any]:
    """Reject legacy/unverified, missing, altered or surplus publicacoes pages."""
    coverage = json.loads((raw_dir / "_coverage.json").read_text())
    if (
        coverage.get("resource") != "publicacoes"
        or coverage.get("partition") != partition
        or coverage.get("complete") is not True
        or coverage.get("page_size") != 50
    ):
        raise ValueError("Missing complete publicacoes coverage")
    expected = get_resource("publicacoes").fetch.required_params["codigoModalidadeContratacao"]
    rows = coverage["modalidades"]
    if sorted(row["params"]["codigoModalidadeContratacao"] for row in rows) != sorted(expected):
        raise ValueError("Incomplete modalidade coverage")
    filenames = set()
    for row in rows:
        actual = modalidade_coverage(raw_dir, row["params"], row["pages"], row["expected_records"])
        if actual != row:
            raise ValueError("Coverage does not match publicacoes pages")
        filenames.update(row["files"])
    if filenames != {p.name for p in raw_dir.glob("publicacoes*_p*.json")}:
        raise ValueError("Unexpected publicacoes pages outside coverage")
    return coverage
