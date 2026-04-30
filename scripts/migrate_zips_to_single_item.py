"""Migrate raw ZIPs from per-month IA items to a single baliza-pncp-raw item.

Internet Archive does not support server-side copy/move. This script downloads
each ZIP to a temp file and re-uploads to the new item, streaming in chunks to
avoid loading large files into memory.

Usage:
    IA_ACCESS_KEY=... IA_SECRET_KEY=... uv run python scripts/migrate_zips_to_single_item.py
    uv run python scripts/migrate_zips_to_single_item.py --dry-run
    uv run python scripts/migrate_zips_to_single_item.py --force-month 2024-03
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

import httpx
import internetarchive as ia
from rich.console import Console

# Add project src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from baliza.ia_uploader import (
    RAW_ITEM_ID,
    RAW_ITEM_METADATA,
    read_manifest_from_ia,
    write_manifest_to_ia,
)

console = Console()


def fetch_existing_zips() -> set[str]:
    """Return the set of zip filenames already present in baliza-pncp-raw."""
    try:
        item = ia.get_item(RAW_ITEM_ID)
        return {f.name for f in item.get_files() if f.name.endswith(".zip")}
    except Exception:
        return set()


def stream_copy_zip(
    month_str: str,
    source_url: str,
    access_key: str,
    secret_key: str,
    existing_zips: set[str],
    dry_run: bool = False,
) -> bool:
    """Download a ZIP from source_url to a temp file and upload to baliza-pncp-raw.

    Returns True on success.
    """
    zip_name = f"raw-{month_str}.zip"

    if zip_name in existing_zips:
        console.print(f"  [dim]{month_str}: already in {RAW_ITEM_ID}, skipping[/dim]")
        return True

    if dry_run:
        console.print(f"  [yellow][dry-run] would copy {zip_name} → {RAW_ITEM_ID}[/yellow]")
        return True

    console.print(f"  [cyan]Copying {zip_name} to {RAW_ITEM_ID}...[/cyan]")

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / zip_name

            # Stream download to temp file — avoids loading the full ZIP into memory
            with httpx.Client(follow_redirects=True, timeout=300.0) as client:
                with client.stream("GET", source_url) as resp:
                    resp.raise_for_status()
                    with open(tmp_path, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=1 << 20):
                            f.write(chunk)

            size_mb = tmp_path.stat().st_size / 1024 / 1024

            ia.upload(
                RAW_ITEM_ID,
                files={zip_name: str(tmp_path)},
                access_key=access_key,
                secret_key=secret_key,
                metadata=RAW_ITEM_METADATA,
                retries=3,
            )
        console.print(f"  [green]✓ {zip_name} uploaded ({size_mb:.1f} MB)[/green]")
        return True

    except Exception as e:
        console.print(f"  [red]✗ {month_str}: {e}[/red]")
        return False


def update_manifest_urls(
    migrated: list[str],
    access_key: str,
    secret_key: str,
) -> None:
    """Update manifest rows for migrated months to point to new item URLs."""
    if not migrated:
        return

    console.print("\n[bold]Updating manifest URLs...[/bold]")
    manifest = read_manifest_from_ia()

    updated = 0
    for row in manifest:
        month_str = row.get("data_particao", "")
        if row.get("file_type") == "monthly_uf":
            continue
        if month_str in migrated:
            new_url = f"https://archive.org/download/{RAW_ITEM_ID}/raw-{month_str}.zip"
            if row.get("raw_zip_url") != new_url:
                row["raw_zip_url"] = new_url
                updated += 1

    if updated:
        write_manifest_to_ia(manifest, access_key, secret_key)
        console.print(f"[green]✓ Updated {updated} manifest rows[/green]")
    else:
        console.print("[dim]No manifest rows needed updating[/dim]")


def main() -> None:  # noqa: PLR0912
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without uploading"
    )
    parser.add_argument("--force-month", help="Migrate only this month (YYYY-MM)")
    parser.add_argument(
        "--no-manifest-update",
        action="store_true",
        help="Skip updating manifest URLs after migration",
    )
    args = parser.parse_args()

    access_key = os.environ.get("IA_ACCESS_KEY") or os.environ.get("IAS3_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY") or os.environ.get("IAS3_SECRET_KEY")

    if not args.dry_run and (not access_key or not secret_key):
        console.print("[red]✗ IA_ACCESS_KEY and IA_SECRET_KEY must be set[/red]")
        sys.exit(1)

    console.print("[bold]Reading manifest from IA...[/bold]")
    manifest = read_manifest_from_ia()

    # Collect months that have a raw_zip_url in the old per-month items
    to_migrate: list[tuple[str, str]] = []  # (month_str, source_url)
    for row in manifest:
        month_str = row.get("data_particao", "")
        raw_zip_url = row.get("raw_zip_url", "")

        if not month_str or not raw_zip_url:
            continue
        # Skip rows that already point to the new item
        if RAW_ITEM_ID in raw_zip_url:
            continue
        # Skip non-monthly rows (shard rows, etc.)
        if row.get("file_type", "") == "monthly_uf":
            continue

        if args.force_month and month_str != args.force_month:
            continue

        to_migrate.append((month_str, raw_zip_url))

    to_migrate.sort()

    if not to_migrate:
        console.print("[green]✓ Nothing to migrate.[/green]")
        return

    console.print(
        f"[cyan]{len(to_migrate)} month(s) to migrate to [bold]{RAW_ITEM_ID}[/bold][/cyan]"
    )
    for m, url in to_migrate:
        console.print(f"  {m}  {url}")

    console.print()

    # Fetch the destination file list once — avoids N IA API calls (one per month)
    existing_zips: set[str] = set() if args.dry_run else fetch_existing_zips()

    migrated: list[str] = []
    errors: list[str] = []

    for month_str, source_url in to_migrate:
        ok = stream_copy_zip(
            month_str,
            source_url,
            access_key or "",
            secret_key or "",
            existing_zips,
            dry_run=args.dry_run,
        )
        if ok:
            migrated.append(month_str)
        else:
            errors.append(month_str)

    console.print(f"\n[bold]Done:[/bold] {len(migrated)} migrated, {len(errors)} errors")
    if errors:
        console.print(f"[red]Errors: {', '.join(errors)}[/red]")

    if not args.dry_run and not args.no_manifest_update and migrated:
        update_manifest_urls(migrated, access_key or "", secret_key or "")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
