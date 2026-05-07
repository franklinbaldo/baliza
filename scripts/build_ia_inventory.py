#!/usr/bin/env -S uv run --quiet
"""Regenerate ``web/public/data/ia_inventory.json`` from the live IA collection.

Queries Internet Archive for all items tagged ``subject:baliza-pncp`` and
writes a structured JSON the ``/arquivo`` page reads at build time.

Pattern mirrors ``build_sync_stats.py``:
- Tolerates IA outage by preserving an existing snapshot.
- Writes a zero-baseline when no file exists so Astro build succeeds.
- Wire into the same web-build pre-pass that runs ``build_sync_stats.py``.

Usage:
    uv run python scripts/build_ia_inventory.py
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

try:
    import internetarchive as ia
except ImportError:
    print(
        "ERROR: internetarchive is not installed. Run: uv pip install internetarchive",
        file=sys.stderr,
    )
    sys.exit(1)

BALIZA_COLLECTION_TAG = "baliza-pncp"

GROUP_LABELS: dict[str, str] = {
    "raw": "Espelhos brutos (ZIP)",
    "monthly_canonical": "Canônicos mensais (Parquet)",
    "annual_canonical": "Consolidados anuais (Parquet)",
    "supporting": "Suporte (manifesto, feeds, dimensões)",
}


def _classify(identifier: str) -> str:
    if identifier == "baliza-pncp-raw":
        return "raw"
    if identifier in ("baliza-pncp-consolidated", "baliza-pncp-dimensions"):
        return "annual_canonical"
    if identifier in ("baliza-pncp-manifest", "baliza-pncp-feeds"):
        return "supporting"
    if re.match(r"^baliza-pncp-\d{4}-\d{2}$", identifier):
        return "monthly_canonical"
    if re.match(r"^baliza-pncp-\d{4}$", identifier):
        return "annual_canonical"
    return "supporting"


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n //= 1024
    return f"{n:.1f} PB"


def _zero_payload() -> dict:
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": [],
        "groups": {k: [] for k in GROUP_LABELS},
        "group_labels": GROUP_LABELS,
    }


def main() -> int:
    out = REPO_ROOT / "web" / "public" / "data" / "ia_inventory.json"
    out.parent.mkdir(parents=True, exist_ok=True)

    session = ia.get_session()

    try:
        results = list(
            session.search_items(
                f"subject:{BALIZA_COLLECTION_TAG}",
                fields=["identifier", "title", "description", "date", "mediatype", "item_size", "num_files"],
            )
        )
    except Exception as exc:
        if out.exists():
            print(
                f"IA search failed; preserving existing ia_inventory.json: {exc}",
                file=sys.stderr,
            )
            return 0
        print(
            f"IA search failed and no ia_inventory.json exists; writing zero baseline: {exc}",
            file=sys.stderr,
        )
        out.write_text(json.dumps(_zero_payload(), indent=2) + "\n", encoding="utf-8")
        return 0

    if not results:
        if out.exists():
            print("IA search returned no items; preserving existing ia_inventory.json", file=sys.stderr)
            return 0
        out.write_text(json.dumps(_zero_payload(), indent=2) + "\n", encoding="utf-8")
        return 0

    items = []
    groups: dict[str, list[str]] = {k: [] for k in GROUP_LABELS}

    for r in sorted(results, key=lambda x: x.get("identifier", "")):
        identifier = r.get("identifier", "")
        try:
            item_size = int(r.get("item_size") or 0)
        except (TypeError, ValueError):
            item_size = 0
        try:
            num_files = int(r.get("num_files") or 0)
        except (TypeError, ValueError):
            num_files = 0

        group = _classify(identifier)
        groups.setdefault(group, []).append(identifier)
        items.append(
            {
                "identifier": identifier,
                "title": r.get("title") or identifier,
                "description": r.get("description") or "",
                "date": r.get("date") or "",
                "mediatype": r.get("mediatype") or "",
                "item_size": item_size,
                "item_size_formatted": _fmt_bytes(item_size),
                "num_files": num_files,
                "archive_url": f"https://archive.org/details/{identifier}",
                "group": group,
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": items,
        "groups": groups,
        "group_labels": GROUP_LABELS,
    }

    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out.relative_to(REPO_ROOT)} — {len(items)} item(s) in {len(groups)} groups")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
