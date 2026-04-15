from __future__ import annotations

import csv
import io
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
import internetarchive as ia
from rich.console import Console

from .engine import BalizaEngine

console = Console()


class DailyExporter:
    """Exports daily data from the engine to Parquet/JSON for upload."""

    def __init__(self, engine: BalizaEngine):
        self.engine = engine

    def export_day(self, target_date: date, output_dir: Path) -> dict[str, Path]:
        """Export all tables for a given day to Parquet in output_dir."""
        output_dir.mkdir(parents=True, exist_ok=True)
        files = {}

        # In our simplified stateless model, we mainly care about 'contratos'
        table_name = "contratos"
        filename = f"{table_name}-{target_date.isoformat()}.parquet"
        out_path = output_dir / filename

        try:
            # Query the in-memory engine
            t = self.engine.get_table(table_name, schema="main")
            # Filter by data_publicacao if available, or just take what's in the session
            # Since the session is ephemeral per day in our sync loop, we can just take all
            t.to_parquet(out_path)
            if out_path.exists():
                files[table_name] = out_path
        except Exception as e:
            console.print(
                f"[yellow]⚠ No data found for {table_name} on {target_date}: {e}[/yellow]"
            )

        return files


class IAUploader:
    """Stateless uploader using a remote CSV manifest as source of truth."""

    def __init__(self, engine: BalizaEngine) -> None:
        self.engine = engine
        self.exporter = DailyExporter(engine)
        self.manifest_item_id = "baliza-pncp-manifest"

    def _read_manifest_from_ia(self) -> list[dict[str, Any]]:
        """Read existing manifest.csv from IA, with a fallback to legacy manifest.parquet."""
        # 1. Try manifest.csv first (new standard)
        url_csv = f"https://archive.org/download/{self.manifest_item_id}/manifest.csv"
        try:
            with httpx.Client(follow_redirects=True) as client:
                resp = client.get(url_csv)
                if resp.status_code == 200:
                    f = io.StringIO(resp.text)
                    reader = csv.DictReader(f)
                    return list(reader)
        except Exception as e:
            console.print(f"[dim]Note: manifest.csv not found or unreadable: {e}[/dim]")

        # 2. Migration Fallback: Try manifest.parquet (legacy standard)
        url_pq = f"https://archive.org/download/{self.manifest_item_id}/manifest.parquet"
        try:
            with duckdb.connect(":memory:") as con:
                con.execute("INSTALL httpfs; LOAD httpfs;")
                # Read remote parquet directly via httpfs
                res = con.execute(f"SELECT * FROM read_parquet('{url_pq}')").fetchall()
                cols = [d[0] for d in con.description]
                manifest = [dict(zip(cols, row, strict=True)) for row in res]
                console.print(
                    f"[green]✓ Migrated {len(manifest)} rows from legacy manifest.parquet.[/green]"
                )
                return manifest
        except Exception:
            # If both fail, we start fresh
            pass

        return []

    def get_uploaded_dates(self) -> set[date]:
        """Fetch the set of already synchronized dates from the remote CSV manifest."""
        manifest = self._read_manifest_from_ia()
        dates = set()
        for row in manifest:
            d_str = row.get("data_particao")
            if d_str:
                try:
                    dates.add(datetime.strptime(d_str, "%Y-%m-%d").date())
                except ValueError:
                    continue
        return dates

    def upload_day(  # noqa: PLR0913
        self,
        target_date: date,
        output_dir: Path,
        ia_access_key: str,
        ia_secret_key: str,
        quarantine_stats: dict[str, int] | None = None,
        quarantine_csv: Path | None = None,
    ) -> None:
        """Export, Zip, and Upload daily data to Internet Archive, then cleanup."""
        item_id = f"baliza-pncp-{target_date.year}"
        date_str = target_date.isoformat()

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)

            # 1. Export Parquet from shared engine
            exported_files = self.exporter.export_day(target_date, temp_path)

            # 2. Zip raw JSON files
            raw_dir = Path("data/raw") / date_str
            zip_path = None
            if raw_dir.exists():
                zip_path = temp_path / f"raw-{date_str}.zip"
                shutil.make_archive(str(zip_path.with_suffix("")), "zip", raw_dir)
                zip_path = zip_path.with_suffix(".zip")

            # 3. Preparation for upload
            files_to_upload = {}
            if quarantine_csv and quarantine_csv.exists():
                files_to_upload[quarantine_csv.name] = str(quarantine_csv)

            if zip_path and zip_path.exists():
                files_to_upload[zip_path.name] = str(zip_path)

            for _table, path in exported_files.items():
                files_to_upload[path.name] = str(path)

            if not files_to_upload:
                console.print(f"[yellow]⚠ Nothing to upload for {date_str}[/yellow]")
                return

            # 4. Perform Upload
            console.print(f"  Uploading {len(files_to_upload)} files to {item_id}...")
            ia.upload(
                item_id,
                files=files_to_upload,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata={
                    "title": f"Baliza PNCP Data {target_date.year}",
                    "mediatype": "data",
                    "collection": "opensource_media",
                },
                retries=3,
            )

            # 5. Update remote manifest.csv
            success = False
            try:
                self._update_remote_manifest(
                    target_date,
                    item_id,
                    files_to_upload,
                    ia_access_key,
                    ia_secret_key,
                    quarantine_stats,
                )
                success = True
            except Exception as e:
                console.print(f"[red]✗ Manifest update failed for {date_str}: {e}[/red]")
                # We do NOT cleanup if manifest update failed, to allow retry

            # 6. AUTOMATED CLEANUP (Only on success)
            if success:
                if raw_dir.exists():
                    shutil.rmtree(raw_dir)

                # Clean up the daily processed folder if it exists
                daily_processed = Path("data/daily")
                if daily_processed.exists():
                    shutil.rmtree(daily_processed)

                console.print(f"[green]✓ {date_str} synced and local data cleaned.[/green]")
            else:
                console.print(
                    f"[yellow]⚠ {date_str} uploaded but NOT cleaned due to manifest error.[/yellow]"
                )

    def _update_remote_manifest(  # noqa: PLR0913
        self,
        target_date: date,
        item_id: str,
        uploaded_files: dict[str, str],
        access_key: str,
        secret_key: str,
        q_stats: dict[str, int] | None = None,
    ) -> None:
        """Append a new row to the manifest.csv on IA."""
        manifest = self._read_manifest_from_ia()

        # Build new row
        date_str = target_date.isoformat()
        new_row = {
            "data_particao": date_str,
            "table_name": "contratos",
            "row_count": q_stats.get("valid", 0) if q_stats else 0,
            "quarantine_count": q_stats.get("quarantine", 0) if q_stats else 0,
            "ia_item_id": item_id,
            "raw_zip_url": f"https://archive.org/download/{item_id}/raw-{date_str}.zip",
            "parquet_url": f"https://archive.org/download/{item_id}/contratos-{date_str}.parquet",
            "quarantine_url": f"https://archive.org/download/{item_id}/quarentena-{date_str}.csv"
            if q_stats and q_stats.get("quarantine", 0) > 0
            else "",
            "uploaded_at": datetime.now().isoformat(),
        }

        # Simple deduplication: remove old row for same date/table
        manifest = [
            r
            for r in manifest
            if not (r["data_particao"] == date_str and r["table_name"] == "contratos")
        ]
        manifest.append(new_row)

        # Write back to CSV
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tf:
            fieldnames = [
                "data_particao",
                "table_name",
                "row_count",
                "quarantine_count",
                "ia_item_id",
                "raw_zip_url",
                "parquet_url",
                "quarantine_url",
                "uploaded_at",
            ]
            writer = csv.DictWriter(tf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(manifest)
            temp_csv = tf.name

        try:
            ia.upload(
                self.manifest_item_id,
                files={"manifest.csv": temp_csv},
                access_key=access_key,
                secret_key=secret_key,
                metadata={"title": "Baliza PNCP Manifest", "mediatype": "data"},
                retries=3,
            )
        finally:
            if Path(temp_csv).exists():
                Path(temp_csv).unlink()
