#!/usr/bin/env python3
"""Backfill the shared ``baliza-pncp`` subject tag onto all existing Baliza IA items.

Every new item Baliza uploads now carries ``subject: baliza-pncp`` (via
``BALIZA_COLLECTION_TAG`` in ``src/baliza/ia_uploader.py``). This script
patches the metadata of pre-existing items that were uploaded before the
tag was introduced.

Discovery: combines a hardcoded list of known stable items with an
authenticated IA search for any additional ``baliza-pncp-*`` identifiers
(e.g. monthly ``baliza-pncp-{YYYY-MM}`` items). The script patches metadata
using ``item.modify_metadata`` (no re-upload) and is idempotent — items
already carrying the tag are skipped.

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

# Stable Baliza IA item identifiers that don't follow a partition pattern.
# The monthly baliza-pncp-{YYYY-MM} items are discovered dynamically.
BALIZA_KNOWN_STABLE_ITEMS = [
    "baliza-pncp-raw",
    "baliza-pncp-manifest",
    "baliza-pncp-consolidated",
    "baliza-pncp-feeds",
    "baliza-pncp-dimensions",
]


def _is_baliza_item(identifier: str) -> bool:
    return identifier.startswith("baliza-pncp")


def _already_tagged(item_metadata: dict) -> bool:
    subjects = item_metadata.get("subject", [])
    if isinstance(subjects, str):
        subjects = [subjects]
    return BALIZA_COLLECTION_TAG in subjects


def _discover_identifiers(session: ia.Session, *, strict: bool = False) -> list[str]:
    """Return all Baliza IA item identifiers: stable items + discovered partitions.

    When *strict* is True (used by --verify), a search failure is re-raised so
    the caller cannot silently pass while skipping all partition items.
    """
    discovered: set[str] = set(BALIZA_KNOWN_STABLE_ITEMS)
    try:
        results = session.search_items(
            "identifier:baliza-pncp*",
            fields=["identifier"],
        )
        for r in results:
            ident = r.get("identifier", "")
            if _is_baliza_item(ident):
                discovered.add(ident)
        print(f"IA search returned {len(discovered)} item(s) total.")
    except Exception as e:
        if strict:
            raise RuntimeError(f"IA search failed in strict/verify mode: {e}") from e
        print(f"WARNING: IA search failed ({e}); using known-items list only.", file=sys.stderr)
    return sorted(discovered)


def backfill(
    *,
    dry_run: bool = False,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> int:
    """Patch missing ``baliza-pncp`` subject onto all discovered Baliza items.

    Returns the number of items patched (or that would be patched in dry-run).
    """
    access_key = access_key or os.environ.get("IA_ACCESS_KEY")
    secret_key = secret_key or os.environ.get("IA_SECRET_KEY")

    if not dry_run and not (access_key and secret_key):
        print(
            "ERROR: IA_ACCESS_KEY and IA_SECRET_KEY must be set (or use --dry-run).",
            file=sys.stderr,
        )
        sys.exit(1)

    config = {}
    if access_key and secret_key:
        config = {"s3": {"access": access_key, "secret": secret_key}}

    session = ia.get_session(config=config)

    identifiers = _discover_identifiers(session)
    print(f"Processing {len(identifiers)} item(s)...")

    patched = 0
    skipped_unreachable = 0
    for identifier in identifiers:
        try:
            item = session.get_item(identifier)
            if not item.exists:
                print(f"  SKIP (does not exist on IA): {identifier}")
                continue
            meta = item.metadata or {}
        except Exception as e:
            print(f"  SKIP (unreachable: {e}): {identifier}", file=sys.stderr)
            skipped_unreachable += 1
            continue

        if _already_tagged(meta):
            print(f"  OK (already tagged): {identifier}")
            continue

        print(f"  {'[DRY RUN] Would patch' if dry_run else 'Patching'}: {identifier}")
        if not dry_run:
            try:
                response = item.modify_metadata(
                    {"subject": BALIZA_COLLECTION_TAG},
                    access_key=access_key,
                    secret_key=secret_key,
                    append_list=True,
                )
                # modify_metadata returns a requests.Response; treat non-2xx as a hard error
                # so CI fails loudly rather than silently skipping items.
                if hasattr(response, "status_code") and not response.ok:
                    print(
                        f"  ERROR patching {identifier}: HTTP {response.status_code} — {response.text[:200]}",
                        file=sys.stderr,
                    )
                    sys.exit(1)
            except Exception as e:
                print(f"  ERROR patching {identifier}: {e}", file=sys.stderr)
                sys.exit(1)
        patched += 1

    if skipped_unreachable:
        print(f"\nWARNING: {skipped_unreachable} item(s) were unreachable and skipped.")
    print(f"\n{'Would patch' if dry_run else 'Patched'} {patched} item(s).")
    return patched


def verify(*, access_key: str | None = None, secret_key: str | None = None) -> bool:
    """Return True when every discovered Baliza item carries the tag."""
    config = {}
    if access_key and secret_key:
        config = {"s3": {"access": access_key, "secret": secret_key}}

    session = ia.get_session(config=config)
    identifiers = _discover_identifiers(session, strict=True)

    if not identifiers:
        print("WARNING: no Baliza items found — IA may be unreachable; treating as pass.")
        return True

    untagged = []
    unreachable = []
    for identifier in identifiers:
        try:
            item = session.get_item(identifier)
            # item.exists is False when the identifier has no files/metadata on IA.
            # Treat non-existent items as skipped rather than untagged.
            if not item.exists:
                print(f"  SKIP (does not exist on IA): {identifier}")
                continue
            meta = item.metadata or {}
        except Exception as e:
            print(f"  ERROR (unreachable: {e}): {identifier}", file=sys.stderr)
            unreachable.append(identifier)
            continue
        if not _already_tagged(meta):
            untagged.append(identifier)

    if unreachable:
        print(
            f"FAIL: {len(unreachable)} item(s) could not be fetched — cannot confirm tag coverage:",
            file=sys.stderr,
        )
        for u in unreachable:
            print(f"  - {u}", file=sys.stderr)
        return False

    if untagged:
        print(f"FAIL: {len(untagged)} item(s) still missing '{BALIZA_COLLECTION_TAG}':")
        for u in untagged:
            print(f"  - {u}")
        return False

    print(f"OK: all {len(identifiers)} item(s) carry '{BALIZA_COLLECTION_TAG}'")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List items that would be patched without touching IA.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Check all items are tagged; exit 1 if any are missing.",
    )
    args = parser.parse_args()

    access_key = os.environ.get("IA_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY")

    if args.verify:
        ok = verify(access_key=access_key, secret_key=secret_key)
        sys.exit(0 if ok else 1)

    backfill(dry_run=args.dry_run, access_key=access_key, secret_key=secret_key)


if __name__ == "__main__":
    main()
