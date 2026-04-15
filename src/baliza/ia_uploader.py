"""Internet Archive uploader for daily PNCP extraction packages.

Design: STATELESS — DuckDB is ephemeral (created fresh each run, discarded at end).
The manifest.parquet on IA is the single source of truth for what has been uploaded.

Upload flow:
  1. Check IA manifest — skip if date already uploaded (idempotent)
  2. Export Parquet files locally from DuckDB
  3. Upload files to monthly IA item
  4. Append new rows to manifest and re-upload to baliza-pncp-manifest/

No GH artifact state is required. The pipeline is fully crash-safe.
"""

from __future__ import annotations

import datetime
import tempfile
from pathlib import Path

import duckdb
import internetarchive as ia
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from .daily_exporter import DailyExporter

console = Console()

MANIFEST_IA_ITEM = "baliza-pncp-manifest"
MANIFEST_URL = f"https://archive.org/download/{MANIFEST_IA_ITEM}/manifest.parquet"

# Manifest schema
_MANIFEST_SCHEMA = pa.schema([
    pa.field("file_url",        pa.string()),
    pa.field("table_name",      pa.string()),
    pa.field("ano",             pa.int32()),
    pa.field("data_particao",   pa.date32()),
    pa.field("uploaded_at",     pa.timestamp("us")),
    pa.field("row_count",       pa.int64()),
    pa.field("file_size_bytes", pa.int64()),
])


def _ia_item_for_date(d: datetime.date) -> str:
    return f"baliza-pncp-{d.strftime('%Y-%m')}"


def _ia_filename(table: str, d: datetime.date, ext: str = ".parquet") -> str:
    return f"{table}-{d.isoformat()}{ext}"


class IAUploader:
    """Stateless uploader — manifest on IA is the source of truth."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.exporter = DailyExporter(db_path)

    # ------------------------------------------------------------------ #
    # Manifest helpers                                                     #
    # ------------------------------------------------------------------ #

    def _read_manifest_from_ia(self) -> pa.Table:
        """Read existing manifest.parquet from IA. Returns empty table on first run or error."""
        try:
            with duckdb.connect(":memory:") as con:
                try:
                    con.execute("LOAD httpfs;")
                except Exception:
                    con.execute("INSTALL httpfs; LOAD httpfs;")

                # Try to read all columns
                try:
                    df = con.execute(f"SELECT * FROM read_parquet('{MANIFEST_URL}')").arrow()
                    
                    # Ensure all columns from _MANIFEST_SCHEMA are present
                    missing_fields = []
                    for field in _MANIFEST_SCHEMA:
                        if field.name not in df.column_names:
                            # Add missing column as nulls
                            null_array = pa.array([None] * df.num_rows, type=field.type)
                            df = df.append_column(field, null_array)
                    
                    # Reorder/select to match schema exactly
                    return df.select(_MANIFEST_SCHEMA.names)
                except Exception as e:
                    # Fallback to manual selection if SELECT * fails
                    console.print(f"[dim]Note: Manifest schema check fallback: {e}[/dim]")
                    rows = con.execute(f"""
                        SELECT file_url, table_name, data_particao
                        FROM read_parquet('{MANIFEST_URL}')
                    """).fetchall()
                    if not rows:
                        return pa.Table.from_batches([], schema=_MANIFEST_SCHEMA)
                    
                    file_urls, table_names, dates = zip(*rows)
                    data = {
                        "file_url": pa.array(file_urls, type=pa.string()),
                        "table_name": pa.array(table_names, type=pa.string()),
                        "data_particao": pa.array(list(dates), type=pa.date32()),
                    }
                    # Add others as null
                    for field in _MANIFEST_SCHEMA:
                        if field.name not in data:
                            data[field.name] = pa.array([None] * len(rows), type=field.type)
                    
                    return pa.table(data).select(_MANIFEST_SCHEMA.names)

        except Exception:
            return pa.Table.from_batches([], schema=_MANIFEST_SCHEMA)


    def get_uploaded_dates(self) -> set[datetime.date]:
        """Return the set of dates already in the manifest (contratos table)."""
        manifest = self._read_manifest_from_ia()
        if manifest.num_rows == 0:
            return set()
        table_names = manifest.column("table_name").to_pylist()
        dates = manifest.column("data_particao").to_pylist()
        return {d for d, t in zip(dates, table_names) if t == "contratos"}


    def _append_to_manifest(
        self,
        existing: pa.Table,
        target_date: datetime.date,
        item_id: str,
        export_stats: dict,
    ) -> pa.Table:
        """Build updated manifest by appending rows for the newly uploaded files."""
        now = datetime.datetime.utcnow()
        new_rows: dict[str, list] = {col.name: [] for col in _MANIFEST_SCHEMA}

        for table_name, info in export_stats.get("tables", {}).items():
            fname = _ia_filename(table_name, target_date)
            new_rows["file_url"].append(
                f"https://archive.org/download/{item_id}/{fname}"
            )
            new_rows["table_name"].append(table_name)
            new_rows["ano"].append(target_date.year)
            new_rows["data_particao"].append(target_date)
            new_rows["uploaded_at"].append(now)
            new_rows["row_count"].append(info.get("row_count", 0))
            new_rows["file_size_bytes"].append(info.get("file_size_bytes", 0))

        new_table = pa.table(
            {k: pa.array(v) for k, v in new_rows.items()},
            schema=_MANIFEST_SCHEMA,
        )

        # Remove existing rows for this date (if re-uploading) then append new
        if existing.num_rows > 0:
            keep_mask = [
                not (d == target_date and t in export_stats.get("tables", {}))
                for d, t in zip(
                    existing.column("data_particao").to_pylist(),
                    existing.column("table_name").to_pylist(),
                )
            ]
            existing = existing.filter(pa.chunked_array([pa.array(keep_mask)]))

        return pa.concat_tables([existing, new_table])

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def upload_day(
        self,
        target_date: datetime.date,
        output_dir: Path,
        ia_access_key: str,
        ia_secret_key: str,
        force: bool = False,
    ) -> bool:
        """Export and upload one date to Internet Archive.

        Returns True if uploaded, False if skipped.
        The pipeline is idempotent — re-running for the same date is safe.
        DuckDB is treated as ephemeral; no local state is persisted.
        """
        # 1. Dedup — check manifest on IA
        if not force:
            already_uploaded = self.get_uploaded_dates()
            if target_date in already_uploaded:
                console.print(f"[dim]{target_date} already in manifest — skipping.[/dim]")
                return False

        # 2. Export Parquet files locally from DuckDB
        console.print(f"Exporting {target_date}...")
        stats = self.exporter.export(target_date, output_dir)
        row_count = stats.get("tables", {}).get("contratos", {}).get("row_count", 0)

        if row_count == 0:
            console.print(f"[yellow]{target_date}: 0 contracts — marking in manifest as empty.[/yellow]")
            # Still record in manifest so we know this date was checked
            stats["tables"]["contratos"] = {"row_count": 0, "file_size_bytes": 0}
            # Don't upload empty files to IA, just update manifest
            item_id = _ia_item_for_date(target_date)
            self._update_manifest_on_ia(target_date, item_id, stats, ia_access_key, ia_secret_key, output_dir)
            return True

        day_dir = output_dir / target_date.isoformat()
        parquet_files = list(day_dir.glob("*.parquet"))
        if not parquet_files:
            console.print(f"[yellow]No Parquet files found for {target_date}.[/yellow]")
            return False

        # 3. Upload to monthly IA item
        item_id = _ia_item_for_date(target_date)
        upload_dict = {
            _ia_filename(f.stem, target_date): str(f)
            for f in parquet_files
        }
        # Also upload metadata JSON
        meta_file = day_dir / "_metadata.json"
        if meta_file.exists():
            upload_dict[_ia_filename("metadata", target_date, ".json")] = str(meta_file)

        console.print(
            f"Uploading {len(upload_dict)} files to IA item [bold cyan]{item_id}[/bold cyan]..."
        )
        try:
            ia.upload(
                item_id,
                files=upload_dict,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata={
                    "title": f"Baliza PNCP Data - {target_date.strftime('%Y-%m')}",
                    "mediatype": "data",
                    "collection": "opensource_media",
                    "creator": "Baliza PNCP Pipeline",
                    "subject": "brazil;pncp;procurement;open-data",
                },
                retries=3,
                retries_sleep=5,
            )
        except Exception as e:
            console.print(f"[red]IA upload failed: {e}[/red]")
            return False

        console.print(f"[green]✓ Uploaded {row_count:,} contracts for {target_date}.[/green]")

        # 4. Update manifest on IA (read → append → re-upload)
        self._update_manifest_on_ia(target_date, item_id, stats, ia_access_key, ia_secret_key, output_dir)
        return True

    def _update_manifest_on_ia(
        self,
        target_date: datetime.date,
        item_id: str,
        export_stats: dict,
        ia_access_key: str,
        ia_secret_key: str,
        output_dir: Path,
    ) -> None:
        """Read existing manifest from IA, append new rows, re-upload."""
        console.print("Updating manifest...")
        existing = self._read_manifest_from_ia()
        updated = self._append_to_manifest(existing, target_date, item_id, export_stats)

        manifest_dir = output_dir / "_manifest"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / "manifest.parquet"
        pq.write_table(updated, manifest_path, compression="zstd")

        try:
            ia.upload(
                MANIFEST_IA_ITEM,
                files={"manifest.parquet": str(manifest_path)},
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata={
                    "title": "Baliza PNCP Data Manifest",
                    "mediatype": "data",
                    "collection": "opensource_media",
                    "creator": "Baliza PNCP Pipeline",
                },
                retries=3,
                retries_sleep=5,
            )
            console.print(
                f"[green]✓ Manifest updated ({updated.num_rows} total rows).[/green]"
            )
        except Exception as e:
            console.print(f"[red]Manifest upload failed: {e}[/red]")
