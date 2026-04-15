"""Annual consolidation of daily PNCP Parquet packages into single-file annual archives.

Strategy:
- Past years (frozen): built once ~60 days after year end, never touched again.
- Current year (hot): rebuilt weekly, replacing the existing IA file each time.

Reading is done via DuckDB httpfs directly from IA URLs (no local download).
"""

from __future__ import annotations

import csv
import datetime
import io
import tempfile
from pathlib import Path

import duckdb
import httpx
import internetarchive as ia
from rich.console import Console

CONSOLIDATED_IA_ITEM = "baliza-pncp-consolidated"
MANIFEST_IA_ITEM = "baliza-pncp-manifest"
MANIFEST_URL = f"https://archive.org/download/{MANIFEST_IA_ITEM}/manifest.csv"

# A past year is considered frozen 60 days after year-end
GRACE_DAYS = 60

console = Console()


def _is_frozen(year: int) -> bool:
    """A past year is frozen once we're >60 days past January 1 of the next year."""
    freeze_date = datetime.date(year + 1, 1, 1) + datetime.timedelta(days=GRACE_DAYS)
    return datetime.date.today() >= freeze_date


def _consolidated_file_name(year: int) -> str:
    return f"contratos-{year}.parquet"


class IAConsolidator:
    """Reads daily Parquet files from IA (via manifest) and builds annual consolidated files."""

    def __init__(self) -> None:
        pass

    def _get_daily_urls_for_year(self, year: int) -> list[str]:
        """Read the manifest.csv on IA and get all daily contratos file URLs for the year."""
        urls = []
        try:
            with httpx.Client(follow_redirects=True) as client:
                resp = client.get(MANIFEST_URL)
                if resp.status_code == 200:
                    f = io.StringIO(resp.text)
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Extract year from data_particao (YYYY-MM-DD)
                        p_date = row.get("data_particao", "")
                        if p_date.startswith(str(year)) and row.get("table_name") == "contratos":
                            if row.get("parquet_url"):
                                urls.append(row["parquet_url"])
        except Exception as e:
            console.print(f"[red]Error reading manifest for consolidation: {e}[/red]")
        return urls

    def _check_consolidated_exists_on_ia(self, year: int) -> bool:
        """Check if the consolidated annual file already exists in the IA item."""
        filename = _consolidated_file_name(year)
        try:
            item = ia.get_item(CONSOLIDATED_IA_ITEM)
            return any(f.get("name") == filename for f in item.files)
        except Exception:
            return False

    def consolidate_year(
        self,
        year: int,
        ia_access_key: str,
        ia_secret_key: str,
        force: bool = False,
    ) -> bool:
        """Build and upload annual consolidated Parquet for the given year."""
        frozen = _is_frozen(year)
        filename = _consolidated_file_name(year)

        if frozen and not force:
            if self._check_consolidated_exists_on_ia(year):
                console.print(
                    f"[dim]Skipping {year}: frozen year, consolidated file already on IA.[/dim]"
                )
                return False

        console.print(f"[cyan]Consolidating {year}...[/cyan]")

        # 1. Get daily file URLs from manifest.csv
        daily_urls = self._get_daily_urls_for_year(year)
        if not daily_urls:
            console.print(f"[yellow]No daily files found in manifest for {year}.[/yellow]")
            return False

        console.print(f"  Found {len(daily_urls)} daily files.")

        # 2. Use DuckDB httpfs to read all and write one consolidated Parquet
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / filename
            url_list = ", ".join(f"'{u}'" for u in daily_urls)

            with duckdb.connect(":memory:") as con:
                con.execute("INSTALL httpfs; LOAD httpfs;")
                console.print(f"  Merging {len(daily_urls)} files via httpfs...")
                con.execute(f"""
                    COPY (
                        SELECT *
                        FROM read_parquet([{url_list}])
                        ORDER BY data_publicacao, numero_controle_pncp
                    ) TO '{output_path}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
                """)

            size_mb = output_path.stat().st_size / 1_048_576
            console.print(f"  Written {filename} ({size_mb:.1f} MB). Uploading to IA...")

            # 3. Upload to baliza-pncp-consolidated/
            ia.upload(
                CONSOLIDATED_IA_ITEM,
                files={filename: str(output_path)},
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata={
                    "title": "Baliza PNCP Consolidated Data",
                    "mediatype": "data",
                    "collection": "opensource_media",
                },
                retries=3,
            )

        console.print(f"[green]✓ {year} consolidated uploaded ({size_mb:.1f} MB).[/green]")
        return True

    def consolidate_all(
        self,
        start_year: int,
        ia_access_key: str,
        ia_secret_key: str,
        force: bool = False,
    ) -> dict[int, bool]:
        """Consolidate from start_year through the current year."""
        current_year = datetime.date.today().year
        results: dict[int, bool] = {}
        for year in range(start_year, current_year + 1):
            results[year] = self.consolidate_year(year, ia_access_key, ia_secret_key, force=force)
        return results
