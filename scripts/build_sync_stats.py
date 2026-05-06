#!/usr/bin/env -S uv run --quiet
"""Regenerate ``web/public/data/sync_stats.json`` from the live IA manifest.

The dashboard's "Sinal do arquivo" tile reads this JSON. Without this
script the file was hand-edited once and pinned at numbers that drift
roughly three orders of magnitude from reality. Run it from the
PNCP sync workflow (so every green sync refreshes the published
JSON) and from the web build (so dev/CI builds always see real
numbers).
"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from baliza.ia_uploader import (  # noqa: E402
    ManifestReadError,
    read_manifest_from_ia,
)


def _to_int(raw: object) -> int:
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def _partition_to_date(p: str) -> datetime | None:
    # data_particao is "YYYY-MM" today; tolerate "YYYY-MM-DD" too in case
    # the manifest schema gains daily granularity.
    for fmt in ("%Y-%m", "%Y-%m-%d"):
        try:
            return datetime.strptime(p, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def main() -> int:
    # Use the strict reader so a transient IA outage raises and we exit
    # with a clear error. Otherwise we'd write zeros and overwrite the
    # last good snapshot — see Codex review on PR #541.
    try:
        rows = read_manifest_from_ia()
    except ManifestReadError as exc:
        print(f"manifest unreachable; leaving existing sync_stats.json: {exc}", file=sys.stderr)
        return 1

    if not rows:
        # Empty manifest is genuine only on a brand-new IA item that has
        # never published. In any project with real partitions this means
        # something is wrong; refuse to publish zeros.
        print("manifest is empty; leaving existing sync_stats.json", file=sys.stderr)
        return 1

    total_contracts = sum(_to_int(r.get("row_count")) for r in rows)
    total_quarantine = sum(_to_int(r.get("quarantine_count")) for r in rows)

    partitions = sorted(
        {p for r in rows if (p := r.get("data_particao"))}
    )
    days_on_ia = 0
    if partitions:
        oldest = _partition_to_date(partitions[0])
        newest = _partition_to_date(partitions[-1])
        if oldest and newest:
            days_on_ia = (newest - oldest).days

    payload = {
        "total_contracts": total_contracts,
        "total_quarantine": total_quarantine,
        "days_on_ia": days_on_ia,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    out = REPO_ROOT / "web" / "public" / "data" / "sync_stats.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out.relative_to(REPO_ROOT)} :: {payload}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
