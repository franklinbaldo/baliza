#!/usr/bin/env python3
"""Backfill the shared ``baliza-pncp`` subject tag onto all existing Baliza IA items.

Every new item Baliza uploads now carries ``subject: baliza-pncp`` (via
``BALIZA_COLLECTION_TAG`` in ``src/baliza/ia_uploader.py``). This script
patches the metadata of pre-existing items that were uploaded before the
tag was introduced.

Discovery query: all IA items whose identifier starts with ``baliza-pncp``.
The script patches metadata using ``item.modify_metadata`` (no re-upload)
and is idempotent — items already carrying the tag are skipped.

Usage:
    IA_ACCESS_KEY=... IA_SECRET_KEY=... uv run python scripts/backfill_ia_subjects.py [--dry-run]

The --dry-run flag lists items that would be patched without touching IA.
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    import internetarchive as ia
except ImportError:
    print("ERROR: internetarchive is not installed. Run: uv pip install internetarchive", file=sys.stderr)
    sys.exit(1)

BALIZA_COLLECTION_TAG = "baliza-pncp"

# Known Baliza item prefixes — used to scope the search safely so we
# never accidentally touch third-party items.
BALIZA_ITEM_PREFIXES = ("baliza-pncp",)

# Expected IA identifiers the pipeline maintains. Extend this list when
# new item families are introduced.
BALIZA_KNOWN_ITEMS = [
    "baliza-pncp-raw",
    "baliza-pncp-manifest",
    "baliza-pncp-consolidated",
    "baliza-pncp-feeds",
    "baliza-pncp-dimensions",
]


def _is_baliza_item(identifier: str) -> bool:
    return any(identifier.startswith(p) for p in BALIZA_ITEM_PREFIXES)


def _already_tagged(item_metadata: dict) -> bool:
    subjects = item_metadata.get("subject", [])
    if isinstance(subjects, str):
        subjects = [subjects]
    return BALIZA_COLLECTION_TAG in subjects


def backfill(*, dry_run: bool = False, access_key: str | None = None, secret_key: str | None = None) -> int:
    """Patch missing ``baliza-pncp`` subject onto all discovered Baliza items.

    Returns the number of items patched (0 in dry-run mode).
    """
    access_key = access_key or os.environ.get("IA_ACCESS_KEY")
    secret_key = secret_key or os.environ.get("IA_SECRET_KEY")

    if not dry_run and not (access_key and secret_key):
        print(
            "ERROR: IA_ACCESS_KEY and IA_SECRET_KEY must be set (or use --dry-run).",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Searching IA for items with identifier matching baliza-pncp*...")
    results = list(ia.search_items(
        "identifier:baliza-pncp*",
        fields=["identifier"],
    ))
    print(f"Found {len(results)} item(s).")

    patched = 0
    for result in results:
        identifier = result.get("identifier", "")
        if not _is_baliza_item(identifier):
            print(f"  SKIP (not a Baliza item): {identifier}")
            continue

        item = ia.get_item(identifier)
        meta = item.metadata or {}

        if _already_tagged(meta):
            print(f"  OK (already tagged): {identifier}")
            continue

        print(f"  {'[DRY RUN] Would patch' if dry_run else 'Patching'}: {identifier}")
        if not dry_run:
            item.modify_metadata(
                {"subject": BALIZA_COLLECTION_TAG},
                access_key=access_key,
                secret_key=secret_key,
                append=True,
            )
            patched += 1
        else:
            patched += 1  # count for dry-run reporting

    print(f"\n{'Would patch' if dry_run else 'Patched'} {patched} item(s).")
    return patched


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List items that would be patched without touching IA.",
    )
    args = parser.parse_args()
    backfill(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
