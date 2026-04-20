from __future__ import annotations

import csv
import hashlib
import io
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import internetarchive as ia
from rich.console import Console

from .engine import BalizaEngine
from .utils import DUCKDB_PARQUET_COPY_OPTIONS, PARQUET_ROW_GROUP_SIZE

console = Console()

# Manifest v2 metadata — record the file properties consumers (web UI,
# researchers) need to reason about the layout without re-reading the file.
# `sort_key` / `bloom_filter_columns` are informational; `sha256` enables
# integrity checks; `file_type` distinguishes canonical vs shard vs
# supplemental rows.
_CONTRATOS_SORT_KEY = "cnpj_orgao,data_publicacao DESC,numero_controle_pncp"
_CONTRATOS_BLOOM_FILTER_COLUMNS = (
    "cnpj_orgao|ni_fornecedor|codigo_ibge"  # dict-encoded columns DuckDB auto-blooms
)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class MonthlyExporter:
    """Exports monthly data from the engine to Parquet/JSON for upload."""

    def __init__(self, engine: BalizaEngine):
        self.engine = engine

    def export_month(self, start_date: date, output_dir: Path) -> dict[str, Path]:
        """Export all tables for a given month to Parquet in output_dir.

        Uses DuckDB ``COPY ... TO`` (via Ibis' underlying connection) so the
        output gets the same WASM-optimized options as the daily and
        consolidated files: 8192-row groups, ZSTD L9, bloom filters on
        dict-encoded columns, and the journey-aware sort order.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        files: dict[str, Path] = {}

        month_str = start_date.strftime("%Y-%m")
        table_name = "contratos"
        filename = f"{table_name}-{month_str}.parquet"
        out_path = output_dir / filename

        try:
            self.engine.con.raw_sql(
                f"""
                COPY (
                    SELECT *
                    FROM main.{table_name}
                    ORDER BY cnpj_orgao, data_publicacao DESC, numero_controle_pncp
                ) TO '{out_path}'
                ({DUCKDB_PARQUET_COPY_OPTIONS})
                """
            )
            if out_path.exists() and out_path.stat().st_size > 0:
                files[table_name] = out_path
        except Exception as e:
            console.print(
                f"[yellow]⚠ No data found for {table_name} on {month_str}: {e}[/yellow]"
            )

        return files


class IAUploader:
    """Stateless uploader using a remote CSV manifest as source of truth."""

    def __init__(self, engine: BalizaEngine) -> None:
        self.engine = engine
        self.exporter = MonthlyExporter(engine)
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

        return []

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

    def upload_month(  # noqa: PLR0913
        self,
        start_date: date,
        output_dir: Path,
        ia_access_key: str,
        ia_secret_key: str,
        quarantine_stats: dict[str, int] | None = None,
        quarantine_csv: Path | None = None,
    ) -> None:
        """Export, Zip, and Upload monthly consolidated data to Internet Archive, then cleanup."""
        month_str = start_date.strftime("%Y-%m")
        item_id = f"baliza-pncp-{month_str}"

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)

            # 1. Export Parquet from shared engine
            exported_files = self.exporter.export_month(start_date, temp_path)

            # 2. Zip raw JSON files
            raw_dir = Path("data/raw") / month_str
            zip_path = None
            if raw_dir.exists():
                zip_path = temp_path / f"raw-{month_str}.zip"
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
                console.print(f"[yellow]⚠ Nothing to upload for {month_str}[/yellow]")
                return

            # 4. Perform Upload
            console.print(f"  Uploading {len(files_to_upload)} files to {item_id}...")
            ia.upload(
                item_id,
                files=files_to_upload,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata={
                    "title": f"Baliza PNCP Data {month_str}",
                    "description": f"Consolidated monthly data for PNCP contracts - {month_str}",
                    "mediatype": "data",
                    "collection": "opensource_media",
                },
                retries=3,
            )

            # 5. Update remote manifest.csv
            success = False
            try:
                self._update_remote_manifest(
                    start_date,
                    item_id,
                    files_to_upload,
                    ia_access_key,
                    ia_secret_key,
                    quarantine_stats,
                )
                success = True
            except Exception as e:
                console.print(f"[red]✗ Manifest update failed for {month_str}: {e}[/red]")
                # We do NOT cleanup if manifest update failed, to allow retry

            # 6. AUTOMATED CLEANUP (Only on success)
            if success:
                if raw_dir.exists():
                    shutil.rmtree(raw_dir)

                # Clean up the daily processed folder if it exists
                daily_processed = Path("data/daily")
                if daily_processed.exists():
                    shutil.rmtree(daily_processed)

                console.print(f"[green]✓ {month_str} synced and local data cleaned.[/green]")
            else:
                console.print(
                    f"[yellow]⚠ {month_str} uploaded but NOT cleaned due to manifest error.[/yellow]"
                )

    def _update_remote_manifest(  # noqa: PLR0913
        self,
        start_date: date,
        item_id: str,
        uploaded_files: dict[str, str],
        access_key: str,
        secret_key: str,
        q_stats: dict[str, int] | None = None,
    ) -> None:
        """Append a new row to the manifest.csv on IA.

        Emits manifest v2 columns (``file_type``, ``sha256``,
        ``file_size_bytes``, ``sort_key``, ``row_group_size``,
        ``bloom_filter_columns``, ``uf_sigla``) alongside the existing
        ones. Legacy columns keep their names so the web loader at
        ``web/src/lib/ia-manifest.ts`` keeps validating rows without
        changes.
        """
        manifest = self._read_manifest_from_ia()

        # Build new row
        month_str = start_date.strftime("%Y-%m")
        parquet_filename = f"contratos-{month_str}.parquet"
        parquet_url = f"https://archive.org/download/{item_id}/{parquet_filename}"

        parquet_local = uploaded_files.get(parquet_filename)
        if parquet_local and Path(parquet_local).exists():
            parquet_sha256 = _sha256(Path(parquet_local))
            parquet_size = Path(parquet_local).stat().st_size
        else:
            parquet_sha256 = ""
            parquet_size = 0

        new_row = {
            "data_particao": month_str,
            "table_name": "contratos",
            "row_count": q_stats.get("valid", 0) if q_stats else 0,
            "quarantine_count": q_stats.get("quarantine", 0) if q_stats else 0,
            "ia_item_id": item_id,
            "raw_zip_url": f"https://archive.org/download/{item_id}/raw-{month_str}.zip",
            "parquet_url": parquet_url,
            "quarantine_url": f"https://archive.org/download/{item_id}/quarentena-{month_str}.csv"
            if q_stats and q_stats.get("quarantine", 0) > 0
            else "",
            "uploaded_at": datetime.now().isoformat(),
            # Manifest v2 columns (all optional in the web schema):
            "file_type": "monthly_canonical",
            "uf_sigla": "",
            "sort_key": _CONTRATOS_SORT_KEY,
            "row_group_size": PARQUET_ROW_GROUP_SIZE,
            "bloom_filter_columns": _CONTRATOS_BLOOM_FILTER_COLUMNS,
            "sha256": parquet_sha256,
            "file_size_bytes": parquet_size,
        }

        # Simple deduplication: remove old row for same partition/table
        manifest = [
            r
            for r in manifest
            if not (r["data_particao"] == month_str and r["table_name"] == "contratos")
        ]
        manifest.append(new_row)

        # Write back to CSV — union v1 + v2 fieldnames so older rows that
        # lack v2 columns still get serialized cleanly.
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
            "file_type",
            "uf_sigla",
            "sort_key",
            "row_group_size",
            "bloom_filter_columns",
            "sha256",
            "file_size_bytes",
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tf:
            writer = csv.DictWriter(tf, fieldnames=fieldnames, extrasaction="ignore")
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
