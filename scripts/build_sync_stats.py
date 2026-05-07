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


def _to_int(raw: object, *, field: str) -> int:
    if raw is None or raw == "":
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        # Surface corrupt manifest cells loudly. We still default to 0 so
        # one bad row doesn't block the whole publish, but the warning
        # makes upstream rot visible rather than silently zeroing the
        # totals.
        print(
            f"warning: could not coerce {field}={raw!r} to int; treating as 0",
            file=sys.stderr,
        )
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


def _zero_payload() -> dict:
    """Empty baseline for first-time builds when IA is unreachable.

    Astro static-imports ``sync_stats.json`` at build time, so the file
    *must* exist before ``astro build`` runs. If we ever ship without
    a committed copy and the build can't reach IA, this lets the site
    deploy with honest zeros instead of failing the build outright.
    """
    return {
        "total_contracts": 0,
        "total_quarantine": 0,
        "days_on_ia": 0,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "resources": {},
    }


def main() -> int:
    out = REPO_ROOT / "web" / "public" / "data" / "sync_stats.json"
    out.parent.mkdir(parents=True, exist_ok=True)

    # Use the strict reader so a transient IA outage is surfaced. We
    # never overwrite a good snapshot with zeros — see Codex review on
    # PR #541 — but we do write a zero baseline when no file exists at
    # all so Astro's static import can resolve on a fresh build.
    try:
        rows = read_manifest_from_ia()
    except ManifestReadError as exc:
        if out.exists():
            print(
                f"manifest unreachable; leaving existing sync_stats.json: {exc}",
                file=sys.stderr,
            )
            return 0
        print(
            f"manifest unreachable and no existing sync_stats.json; "
            f"writing zero baseline so the build can proceed: {exc}",
            file=sys.stderr,
        )
        out.write_text(json.dumps(_zero_payload(), indent=2) + "\n", encoding="utf-8")
        return 0

    if not rows:
        # Empty manifest is genuine only on a brand-new IA item that has
        # never published. Don't overwrite a good snapshot if one exists;
        # write zeros only when the file is missing entirely.
        if out.exists():
            print("manifest is empty; leaving existing sync_stats.json", file=sys.stderr)
            return 0
        print(
            "manifest is empty and no existing sync_stats.json; "
            "writing zero baseline so the build can proceed",
            file=sys.stderr,
        )
        out.write_text(json.dumps(_zero_payload(), indent=2) + "\n", encoding="utf-8")
        return 0

    # Canonical monthly partitions only:
    #   - file_type='monthly_canonical' (or empty for backward compat
    #     with rows written before the column existed) so non-canonical
    #     shards like monthly_uf don't double-count.
    canonical_all = [
        r for r in rows
        if (r.get("file_type") or "monthly_canonical") == "monthly_canonical"
    ]

    def _aggregate(resource_rows: list[dict]) -> dict[str, int]:
        partitions = sorted({p for r in resource_rows if (p := r.get("data_particao"))})
        days = 0
        if partitions:
            oldest = _partition_to_date(partitions[0])
            newest = _partition_to_date(partitions[-1])
            if oldest is None or newest is None:
                print(
                    f"warning: could not parse partition span "
                    f"({partitions[0]!r} .. {partitions[-1]!r}); "
                    "leaving days_on_ia at 0",
                    file=sys.stderr,
                )
            else:
                days = (newest - oldest).days
        return {
            "row_count": sum(_to_int(r.get("row_count"), field="row_count") for r in resource_rows),
            "quarantine_count": sum(
                _to_int(r.get("quarantine_count"), field="quarantine_count")
                for r in resource_rows
            ),
            "partition_count": len(partitions),
            "days_on_ia": days,
        }

    # Per-resource breakdown so the dashboard can render any registered
    # resource's tile (atas, contratacoes, …) once data lands. Unknown
    # resources still flow through verbatim — this is a derived view of
    # whatever the manifest reports, not a hardcoded list.
    by_resource: dict[str, list[dict]] = {}
    for r in canonical_all:
        name = r.get("table_name") or ""
        if not name:
            continue
        by_resource.setdefault(name, []).append(r)
    resources_breakdown = {name: _aggregate(rs) for name, rs in by_resource.items()}

    # Top-level fields stay shaped exactly as before so the existing
    # SyncStatsSchema (z.object) still parses without surgery — they
    # describe contratos because the homepage tile is "Contratos
    # citáveis". The dashboard can extend the schema to read
    # `resources.{name}` directly when it adds per-resource tiles.
    contratos_stats = resources_breakdown.get("contratos") or _aggregate([])

    payload = {
        "total_contracts": contratos_stats["row_count"],
        "total_quarantine": contratos_stats["quarantine_count"],
        "days_on_ia": contratos_stats["days_on_ia"],
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "resources": resources_breakdown,
    }

    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out.relative_to(REPO_ROOT)} :: {payload}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
