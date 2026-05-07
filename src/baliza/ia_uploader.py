from __future__ import annotations

import csv
import hashlib
import io
import shutil
import tempfile
import threading
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import internetarchive as ia
from rich.console import Console

from . import rss_feed
from .artifacts import require_artifact, resolve_export_params
from .engine import BalizaEngine
from .extractor import FETCHED_SENTINEL
from .partitioning import partition_label
from .resources import CONTRATOS, get_resource, raw_zip_filename
from .utils import DUCKDB_PARQUET_COPY_OPTIONS, PARQUET_ROW_GROUP_SIZE

console = Console()

# Serializes all manifest read-modify-write cycles within a process.
# ThreadPoolExecutor workers share this lock, preventing concurrent writers
# from overwriting each other's updates to manifest.csv on IA.
_manifest_lock = threading.Lock()


def restore_from_raw_zip(raw_zip_url: str, raw_month_dir: Path) -> bool:
    """Download a raw ZIP from Internet Archive and extract it to raw_month_dir.

    Returns True on success, False on any error (caller falls back to PNCP API).
    Only used for past months where the raw ZIP was previously uploaded.
    """
    raw_month_dir.mkdir(parents=True, exist_ok=True)
    try:
        console.print(f"  Downloading raw ZIP from IA: {raw_zip_url}")
        with httpx.stream("GET", raw_zip_url, follow_redirects=True, timeout=120) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                tmp_path = Path(tmp.name)
                for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                    tmp.write(chunk)
        with zipfile.ZipFile(tmp_path) as zf:
            zf.extractall(raw_month_dir)
        tmp_path.unlink(missing_ok=True)
        console.print(
            f"  [green]✓ Restored from IA ZIP ({len(list(raw_month_dir.iterdir()))} files)[/green]"
        )
        return True
    except Exception as e:
        console.print(
            f"  [yellow]⚠ Could not restore from IA ZIP: {e} — will fetch from PNCP[/yellow]"
        )
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False


# Manifest v2 metadata — record the file properties consumers (web UI,
# researchers) need to reason about the layout without re-reading the file.
# `sort_key` / `bloom_filter_columns` are informational; `sha256` enables
# integrity checks; `file_type` distinguishes canonical vs shard vs
# supplemental rows.
_CONTRATOS_SORT_KEY = "cnpj_orgao,data_publicacao DESC,numero_controle_pncp"
_CONTRATOS_BLOOM_FILTER_COLUMNS = (
    "cnpj_orgao|ni_fornecedor|codigo_ibge"  # dict-encoded columns DuckDB auto-blooms
)

MANIFEST_ITEM_ID = "baliza-pncp-manifest"
RAW_ITEM_ID = "baliza-pncp-raw"
FEEDS_ITEM_ID = "baliza-pncp-feeds"
FEEDS_ITEM_METADATA = {
    "title": "Baliza PNCP — Curated RSS feeds",
    "description": (
        "RSS 2.0 feeds for curated public-watch slugs (see "
        "src/baliza/rss_feed.py:CURATED_WATCHES). Rebuilt by the daily "
        "sync workflow alongside the monthly Parquet."
    ),
    "mediatype": "data",
    "collection": "opensource_media",
    "subject": ["PNCP", "RSS", "alertas", "Brasil", "open data"],
    "creator": "Baliza",
    "language": "pt",
}
FEEDS_PER_FEED_LIMIT = 50
RAW_ITEM_METADATA = {
    "title": "Baliza PNCP — Raw JSON Mirror (all months)",
    "description": (
        "Complete mirror of PNCP (Portal Nacional de Contratações Públicas) API responses. "
        "Each raw-YYYY-MM.zip contains the raw JSON pages fetched from the contratos endpoint "
        "for that month. Part of the Baliza open data project."
    ),
    "mediatype": "data",
    "collection": "opensource_media",
    "subject": ["PNCP", "contratos", "licitações", "governo", "Brasil", "open data"],
    "creator": "Baliza",
    "language": "pt",
}

# Union of v1 + v2 fieldnames — DictWriter with extrasaction="ignore"
# happily writes blanks for legacy rows missing v2 columns.
MANIFEST_FIELDNAMES = [
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
    # v3 fields — two-phase pipeline tracking (mirror then build)
    "mirror_uploaded_at",
    "raw_zip_sha256",
    "raw_zip_size_bytes",
    "parquet_uploaded_at",
    "parquet_schema_version",
]


class ManifestReadError(RuntimeError):
    """Raised when manifest.csv cannot be read/parsed and writers must abort.

    Distinguished from "no manifest yet" (HTTP 404) which returns []: a
    transient network or parse error must NOT trigger a write that would
    overwrite a live manifest with only the partial rows the caller has
    locally.
    """


def read_manifest_from_ia() -> list[dict[str, Any]]:
    """Read manifest.csv from the baliza-pncp-manifest IA item.

    Returns [] only when the manifest is genuinely absent (HTTP 404 — fresh
    IA item before the first publish). Any other failure (network, non-200,
    parse error) raises ManifestReadError so that downstream writers do
    not overwrite a live manifest with their partial view.
    """
    url_csv = f"https://archive.org/download/{MANIFEST_ITEM_ID}/manifest.csv"
    try:
        with httpx.Client(follow_redirects=True) as client:
            resp = client.get(url_csv)
    except Exception as e:
        raise ManifestReadError(f"network error reading manifest.csv: {e}") from e
    if resp.status_code == 404:
        return []
    if resp.status_code != 200:
        raise ManifestReadError(f"unexpected status {resp.status_code} reading manifest.csv")
    try:
        f = io.StringIO(resp.text)
        return list(csv.DictReader(f))
    except Exception as e:
        raise ManifestReadError(f"parse error reading manifest.csv: {e}") from e


def try_read_manifest_from_ia() -> list[dict[str, Any]]:
    """Best-effort read for read-only callers (CLI inspection, status).

    Writers MUST use read_manifest_from_ia() instead so a transient read
    failure aborts the publish rather than truncating the manifest.
    """
    try:
        return read_manifest_from_ia()
    except ManifestReadError as e:
        console.print(f"[dim]Note: manifest.csv not readable: {e}[/dim]")
        return []


def write_manifest_to_ia(rows: list[dict[str, Any]], access_key: str, secret_key: str) -> None:
    """Serialize manifest rows to CSV and upload to IA."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tf:
        writer = csv.DictWriter(tf, fieldnames=MANIFEST_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        temp_csv = tf.name
    try:
        ia.upload(
            MANIFEST_ITEM_ID,
            files={"manifest.csv": temp_csv},
            access_key=access_key,
            secret_key=secret_key,
            metadata={"title": "Baliza PNCP Manifest", "mediatype": "data"},
            retries=3,
        )
    finally:
        if Path(temp_csv).exists():
            Path(temp_csv).unlink()


def register_monthly_uf_shards(
    year: int,
    table_name: str,
    shards: list[dict[str, Any]],
    access_key: str,
    secret_key: str,
) -> None:
    """Publish monthly_uf shard rows to the manifest so the web resolver can find them.

    ``shards`` is a list of dicts carrying ``uf_sigla``, ``parquet_url``,
    ``sha256``, ``file_size_bytes``. Existing monthly_uf rows for the same
    (year, table) are replaced atomically so reruns are idempotent.
    Monthly canonical rows and supplemental rows are left untouched.

    Propagates ManifestReadError — callers should not swallow it, since
    writing partial rows on top of a read failure would wipe the canonical
    history from the live manifest.
    """
    with _manifest_lock:
        year_str = str(year)
        manifest = read_manifest_from_ia()
        # Use the latest canonical month already published for this year as the
        # shard's data_particao. That way the web resolver's
        # `canonical.dataParticao > shard.dataParticao` freshness check can
        # detect month-level lag (e.g. canonical 2025-04 vs shards rebuilt when
        # only 2025-01 was published). Falls back to the year string when no
        # canonical rows exist yet.
        canonical_months = [
            r.get("data_particao", "")
            for r in manifest
            if r.get("table_name") == table_name
            and (r.get("data_particao") or "").startswith(year_str)
            and r.get("file_type", "") in ("", "monthly_canonical")
        ]
        data_particao = max(canonical_months) if canonical_months else year_str
        # Drop prior monthly_uf rows for this year+table across any partition
        # value — we're rebuilding every shard, so stale entries from earlier
        # rebuilds (which may carry a different data_particao) would otherwise
        # accumulate and mislead readers.
        manifest = [
            r
            for r in manifest
            if not (
                r.get("table_name") == table_name
                and r.get("file_type") == "monthly_uf"
                and (r.get("data_particao") or "").startswith(year_str)
            )
        ]
        now = datetime.now().isoformat()
        for s in shards:
            manifest.append(
                {
                    "data_particao": data_particao,
                    "table_name": table_name,
                    "row_count": s.get("row_count", 0),
                    "quarantine_count": 0,
                    "ia_item_id": s.get("ia_item_id", ""),
                    "raw_zip_url": "",
                    "parquet_url": s["parquet_url"],
                    "quarantine_url": "",
                    "uploaded_at": now,
                    "file_type": "monthly_uf",
                    "uf_sigla": s["uf_sigla"],
                    "sort_key": _CONTRATOS_SORT_KEY,
                    "row_group_size": PARQUET_ROW_GROUP_SIZE,
                    "bloom_filter_columns": _CONTRATOS_BLOOM_FILTER_COLUMNS,
                    "sha256": s.get("sha256", ""),
                    "file_size_bytes": s.get("file_size_bytes", 0),
                }
            )
        write_manifest_to_ia(manifest, access_key, secret_key)


def _pack_resource_pages_zip(
    raw_dir: Path,
    zip_path: Path,
    *,
    resource: str,
) -> bool:
    """Pack only ``{resource}_p*.json`` pages and the FETCHED_SENTINEL into ``zip_path``.

    Returns True when at least one page was written. The sentinel is
    content-free so it doesn't carry resource-specific state, but
    including it lets ``restore_from_raw_zip`` skip ``probe_range`` on
    the build/sync side.

    Both upload paths (``upload_raw_zip`` and ``upload_month``) call
    this so the published ``raw_zip_sha256`` represents exactly the
    same set of files in either flow.
    """
    page_glob = f"{resource}_p*.json"
    page_files = sorted(raw_dir.glob(page_glob))
    if not page_files:
        return False
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for page_path in page_files:
            zf.write(page_path, page_path.name)
        sentinel = raw_dir / FETCHED_SENTINEL
        if sentinel.exists():
            zf.write(sentinel, sentinel.name)
    return True


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

    def export_month(
        self,
        start_date: date,
        output_dir: Path,
        *,
        resource: str = CONTRATOS.name,
    ) -> dict[str, Path]:
        """Export the resource's monthly Parquet to output_dir.

        Uses DuckDB ``COPY ... TO`` (via Ibis' underlying connection) so the
        output gets the same WASM-optimized options as the daily and
        consolidated files: 8192-row groups, ZSTD L9, bloom filters on
        dict-encoded columns, and the journey-aware sort order.

        ORDER BY: the resource's ``CanonicalTableSpec.order_by_sql`` is
        used when set (preserves contratos' historical shape); otherwise
        we derive ``sort_columns + pk`` so a new resource gets a
        deterministic ordering by default.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        files: dict[str, Path] = {}

        spec = get_resource(resource)
        table_spec = spec.canonical_tables[0]
        table_name = table_spec.table_name
        # Drift-D wiring: partition label tracks the resource's
        # PartitionStrategy so an annual resource gets ``-2024.parquet``
        # rather than ``-2024-01.parquet``.
        month_str = partition_label(spec, start_date)
        filename = f"{table_name}-{month_str}.parquet"
        out_path = output_dir / filename

        # Drift-B wiring: ORDER BY (and sort/bloom downstream) reads
        # from the matched ArtifactSpec with fallback to the canonical
        # table's defaults. For contratos/atas no artifact overrides
        # the canonical defaults today, so behavior is byte-identical
        # — but a per-artifact override (e.g. annual rollup sorted by
        # year then UF when PCA lands) now flows through without
        # editing this exporter.
        _, _, order_by = resolve_export_params(spec, "monthly_canonical", table_spec)

        try:
            self.engine.con.raw_sql(
                f"""
                COPY (
                    SELECT *
                    FROM main.{table_name}
                    ORDER BY {order_by}
                ) TO '{out_path}'
                ({DUCKDB_PARQUET_COPY_OPTIONS})
                """
            )
            if out_path.exists() and out_path.stat().st_size > 0:
                files[table_name] = out_path
        except Exception as e:
            console.print(f"[yellow]⚠ No data found for {table_name} on {month_str}: {e}[/yellow]")

        return files


class IAUploader:
    """Stateless uploader using a remote CSV manifest as source of truth."""

    def __init__(self, engine: BalizaEngine | None = None) -> None:
        self.engine = engine
        self.exporter = MonthlyExporter(engine) if engine else None
        self.manifest_item_id = "baliza-pncp-manifest"

    def _read_manifest_from_ia(self) -> list[dict[str, Any]]:
        """Best-effort read used by CLI inspectors.

        Writers must use the module-level ``read_manifest_from_ia`` which
        fails closed — overwriting the live manifest with the partial view
        collected after a transient read error would silently wipe rows.
        """
        return try_read_manifest_from_ia()

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

    def _upsert_manifest_row(
        self,
        month_str: str,
        fields: dict[str, Any],
        access_key: str,
        secret_key: str,
        *,
        resource: str = CONTRATOS.name,
    ) -> None:
        """Read manifest, merge fields into the canonical row for (resource, month_str), write back.

        If no canonical row exists for the (resource, month) pair, creates
        one with safe defaults. Existing fields not present in ``fields``
        are preserved (merge, not replace). Uses the strict reader —
        transient read failures abort rather than wipe the live manifest.
        """
        with _manifest_lock:
            manifest = read_manifest_from_ia()
            parquet_item_id = f"baliza-pncp-{month_str}"
            raw_zip_url = (
                f"https://archive.org/download/{RAW_ITEM_ID}/{raw_zip_filename(resource, month_str)}"
            )

            existing_idx: int | None = None
            for i, row in enumerate(manifest):
                if (
                    row.get("data_particao") == month_str
                    and row.get("table_name") == resource
                    and row.get("file_type", "") in ("", "monthly_canonical")
                ):
                    existing_idx = i
                    break

            now = datetime.now().isoformat()
            if existing_idx is not None:
                manifest[existing_idx] = {**manifest[existing_idx], **fields}
            else:
                new_row: dict[str, Any] = {
                    "data_particao": month_str,
                    "table_name": resource,
                    "row_count": 0,
                    "quarantine_count": 0,
                    "ia_item_id": parquet_item_id,
                    "raw_zip_url": raw_zip_url,
                    "parquet_url": "",
                    "quarantine_url": "",
                    "uploaded_at": now,
                    "file_type": "monthly_canonical",
                    "uf_sigla": "",
                    "sort_key": _CONTRATOS_SORT_KEY,
                    "row_group_size": PARQUET_ROW_GROUP_SIZE,
                    "bloom_filter_columns": _CONTRATOS_BLOOM_FILTER_COLUMNS,
                    "sha256": "",
                    "file_size_bytes": 0,
                    "mirror_uploaded_at": "",
                    "raw_zip_sha256": "",
                    "raw_zip_size_bytes": 0,
                    "parquet_uploaded_at": "",
                    "parquet_schema_version": "",
                }
                new_row.update(fields)
                manifest.append(new_row)

            write_manifest_to_ia(manifest, access_key, secret_key)

    def upload_raw_zip(  # noqa: PLR0913
        self,
        start_date: date,
        raw_dir: Path,
        ia_access_key: str,
        ia_secret_key: str,
        keep_raw_dir: bool = False,
        *,
        resource: str = CONTRATOS.name,
    ) -> bool:
        """Zip raw JSON pages and upload to baliza-pncp-raw; update manifest mirror fields.

        Does NOT ingest or export Parquet. Cleans up raw_dir on success so the
        next run fetches fresh from IA (or GHA cache). Returns True on success.

        Args:
            resource: PNCP resource name. Determines the ZIP filename
                (contratos keeps the legacy ``raw-{month}.zip`` shape;
                other resources prefix with the resource name to avoid
                colliding with contratos zips for the same partition)
                and the manifest row's ``table_name`` so per-resource
                mirror state stays separate.
        """
        # Drift-D wiring: partition label comes from the resource's
        # PartitionStrategy (``YYYY-MM`` for monthly, ``YYYY`` for
        # annual) so writers and readers agree on the data_particao
        # value an annual resource emits.
        month_str = partition_label(get_resource(resource), start_date)
        zip_basename = raw_zip_filename(resource, month_str)

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            zip_path = temp_path / zip_basename

            # Per-resource scoping: pack only this resource's pages so a
            # sequential ``mirror --resource contratos`` followed by
            # ``mirror --resource atas`` doesn't end up with one
            # resource's zip containing the other's cached pages.
            _pack_resource_pages_zip(raw_dir, zip_path, resource=resource)

            raw_zip_sha256 = _sha256(zip_path)
            raw_zip_size = zip_path.stat().st_size

            console.print(
                f"  Uploading raw ZIP for {resource}/{month_str} to {RAW_ITEM_ID}..."
            )
            ia.upload(
                RAW_ITEM_ID,
                files={zip_path.name: str(zip_path)},
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata=RAW_ITEM_METADATA,
                retries=3,
            )

        raw_zip_url = f"https://archive.org/download/{RAW_ITEM_ID}/{zip_basename}"
        now = datetime.now().isoformat()
        success = False
        try:
            self._upsert_manifest_row(
                month_str,
                {
                    "raw_zip_url": raw_zip_url,
                    "mirror_uploaded_at": now,
                    "raw_zip_sha256": raw_zip_sha256,
                    "raw_zip_size_bytes": raw_zip_size,
                    "uploaded_at": now,
                },
                ia_access_key,
                ia_secret_key,
                resource=resource,
            )
            success = True
        except Exception as e:
            console.print(f"[red]✗ Manifest update failed for {resource}/{month_str}: {e}[/red]")

        if success and not keep_raw_dir and raw_dir.exists():
            shutil.rmtree(raw_dir)
            console.print(f"[green]✓ {month_str} mirrored and local pages cleaned.[/green]")
        elif success and keep_raw_dir:
            console.print(
                f"[green]✓ {month_str} ZIP updated (pages kept for incremental next run).[/green]"
            )
        else:
            console.print(
                f"[yellow]⚠ {month_str} ZIP uploaded but NOT cleaned due to manifest error.[/yellow]"
            )
        return success

    def upload_parquet(  # noqa: PLR0913
        self,
        start_date: date,
        ia_access_key: str,
        ia_secret_key: str,
        quarantine_stats: dict[str, int] | None = None,
        quarantine_csv: Path | None = None,
        schema_version: str = "",
        *,
        resource: str = CONTRATOS.name,
    ) -> bool:
        """Export Parquet from engine and upload to the monthly IA item; update manifest.

        Requires engine to be set. Does NOT touch raw JSON pages.
        Returns True on success.

        Args:
            resource: PNCP resource name. Routes the export through the
                resource's canonical table and produces ``{table_name}-
                {month}.parquet`` so contratos and atas write distinct
                files in the same monthly IA item.
        """
        if self.engine is None or self.exporter is None:
            raise RuntimeError(
                "upload_parquet requires an engine — construct IAUploader(engine=...)"
            )

        spec = get_resource(resource)
        table_name = spec.canonical_tables[0].table_name
        month_str = partition_label(spec, start_date)
        item_id = f"baliza-pncp-{month_str}"

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            exported_files = self.exporter.export_month(
                start_date, temp_path, resource=resource
            )

            files_to_upload: dict[str, str] = {}
            if quarantine_csv and quarantine_csv.exists():
                files_to_upload[quarantine_csv.name] = str(quarantine_csv)
            for _table, path in exported_files.items():
                files_to_upload[path.name] = str(path)

            parquet_filename = f"{table_name}-{month_str}.parquet"
            if parquet_filename not in files_to_upload:
                # No Parquet produced (zero valid rows) — don't write a broken parquet_url
                # to the manifest. The quarantine CSV can still be uploaded independently.
                console.print(
                    f"[yellow]⚠ No Parquet file produced for {month_str} — skipping upload[/yellow]"
                )
                return False

            parquet_local = files_to_upload.get(parquet_filename)
            parquet_sha256 = ""
            parquet_size = 0
            if parquet_local and Path(parquet_local).exists():
                parquet_sha256 = _sha256(Path(parquet_local))
                parquet_size = Path(parquet_local).stat().st_size

            console.print(f"  Uploading Parquet for {month_str} to {item_id}...")
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

        now = datetime.now().isoformat()
        success = False
        try:
            q_count = quarantine_stats.get("quarantine", 0) if quarantine_stats else 0
            self._upsert_manifest_row(
                month_str,
                {
                    "parquet_url": f"https://archive.org/download/{item_id}/{parquet_filename}",
                    "sha256": parquet_sha256,
                    "file_size_bytes": parquet_size,
                    "row_count": quarantine_stats.get("valid", 0) if quarantine_stats else 0,
                    "quarantine_count": q_count,
                    "quarantine_url": (
                        f"https://archive.org/download/{item_id}/quarentena-{month_str}.csv"
                        if q_count > 0
                        else ""
                    ),
                    "parquet_uploaded_at": now,
                    "parquet_schema_version": schema_version,
                    "uploaded_at": now,
                },
                ia_access_key,
                ia_secret_key,
                resource=resource,
            )
            success = True
        except Exception as e:
            console.print(f"[red]✗ Manifest update failed for {month_str}: {e}[/red]")

        return success

    def upload_feeds(self, ia_access_key: str, ia_secret_key: str) -> list[str]:
        """Build and upload feed-{slug}.xml for each curated watch.

        Returns the list of slugs that were successfully published. Per-feed
        failures are logged but do not abort the rest — RSS publication is
        best-effort and must not block the parquet sync that already succeeded
        by the time this runs.
        """
        if self.engine is None:
            raise RuntimeError("upload_feeds requires an engine — construct IAUploader(engine=...)")

        published: list[str] = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            files_to_upload: dict[str, str] = {}
            for watch in rss_feed.CURATED_WATCHES:
                slug = watch["slug"]
                title = watch["title"]
                where = watch.get("where", "1=1")
                # Alias to the snake_case names rss_feed.generate accepts
                # — the canonical contratos schema exposes the contract
                # object as `objeto_contrato` (no -cao), so we project it
                # under the expected key here rather than teaching the RSS
                # generator about every column-name dialect.
                sql = (
                    "SELECT "
                    "numero_controle_pncp, "
                    "data_publicacao_pncp, "
                    "objeto_contrato AS objeto_contratacao "
                    f"FROM main.{CONTRATOS.name} "
                    f"WHERE {where} "
                    "ORDER BY data_publicacao_pncp DESC "
                    f"LIMIT {FEEDS_PER_FEED_LIMIT}"
                )
                try:
                    cursor = self.engine.con.raw_sql(sql)
                    raw_rows = cursor.fetchall()
                    columns = [d[0] for d in cursor.description]
                    rows = [dict(zip(columns, r, strict=True)) for r in raw_rows]
                    xml = rss_feed.generate(rows, slug, title)
                    out = tmp / f"feed-{slug}.xml"
                    out.write_text(xml, encoding="utf-8")
                    files_to_upload[out.name] = str(out)
                    published.append(slug)
                except Exception as e:
                    console.print(f"[yellow]⚠ feed {slug}: {e}[/yellow]")

            if not files_to_upload:
                console.print("[yellow]⚠ No curated feeds produced[/yellow]")
                return published

            console.print(
                f"  Uploading {len(files_to_upload)} feed(s) to {FEEDS_ITEM_ID}..."
            )
            ia.upload(
                FEEDS_ITEM_ID,
                files=files_to_upload,
                access_key=ia_access_key,
                secret_key=ia_secret_key,
                metadata=FEEDS_ITEM_METADATA,
                retries=3,
            )
        return published

    def upload_month(  # noqa: PLR0912, PLR0913
        self,
        start_date: date,
        output_dir: Path,
        ia_access_key: str,
        ia_secret_key: str,
        quarantine_stats: dict[str, int] | None = None,
        quarantine_csv: Path | None = None,
        *,
        resource: str = CONTRATOS.name,
    ) -> None:
        """Export, Zip, and Upload monthly consolidated data to Internet Archive, then cleanup.

        Args:
            resource: PNCP resource name. Determines the canonical table
                queried by export_month, the parquet filename, and the
                raw ZIP basename so contratos and atas stay isolated
                inside the same monthly IA item.
        """
        if self.engine is None or self.exporter is None:
            raise RuntimeError("upload_month requires an engine — construct IAUploader(engine=...)")

        month_str = partition_label(get_resource(resource), start_date)
        item_id = f"baliza-pncp-{month_str}"
        zip_basename = raw_zip_filename(resource, month_str)

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)

            # 1. Export Parquet from shared engine
            exported_files = self.exporter.export_month(
                start_date, temp_path, resource=resource
            )

            # 2. Zip raw JSON files (per-resource: only this resource's
            # pages, so a contratos+atas same-month run produces two
            # distinct zip basenames sharing the same monthly item).
            raw_dir = Path("data/raw") / month_str
            zip_path = None
            if raw_dir.exists():
                candidate = temp_path / zip_basename
                if _pack_resource_pages_zip(raw_dir, candidate, resource=resource):
                    zip_path = candidate

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
                    resource=resource,
                )
                success = True
            except Exception as e:
                console.print(f"[red]✗ Manifest update failed for {month_str}: {e}[/red]")
                # We do NOT cleanup if manifest update failed, to allow retry

            # 6. AUTOMATED CLEANUP (Only on success)
            #
            # Note on RSS feeds: feed publication is intentionally NOT triggered
            # here. The sync flow (cli_simple.process_month_full) processes
            # months in parallel, each thread holding its own in-memory engine
            # populated only with that month's rows. Calling upload_feeds inside
            # upload_month would race per-feed XML uploads and let whichever
            # month finishes last overwrite the others with partial data. Feed
            # publication must be a separate, post-sync step that runs once
            # against an engine guaranteed to hold the full snapshot.
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
        *,
        resource: str = CONTRATOS.name,
    ) -> None:
        """Append a new row to the manifest.csv on IA for (resource, month).

        Emits manifest v2 columns (``file_type``, ``sha256``,
        ``file_size_bytes``, ``sort_key``, ``row_group_size``,
        ``bloom_filter_columns``, ``uf_sigla``) alongside the existing
        ones. Legacy columns keep their names so the web loader at
        ``web/src/lib/ia-manifest.ts`` keeps validating rows without
        changes.

        Uses the strict module-level reader — a transient IA read failure
        must abort the publish rather than overwrite the live manifest with
        only the new row.
        """
        spec = get_resource(resource)
        table_name = spec.canonical_tables[0].table_name
        # Drift-B wiring: matched ArtifactSpec drives the file_type
        # column on the manifest row instead of a hardcoded literal.
        artifact = require_artifact(spec, "monthly_canonical")

        # Build the new row before acquiring the lock — sha256/size computation
        # is pure local I/O and doesn't need to be serialized.
        month_str = partition_label(spec, start_date)
        parquet_filename = f"{table_name}-{month_str}.parquet"
        parquet_url = f"https://archive.org/download/{item_id}/{parquet_filename}"
        zip_basename = raw_zip_filename(resource, month_str)

        parquet_local = uploaded_files.get(parquet_filename)
        if parquet_local and Path(parquet_local).exists():
            parquet_sha256 = _sha256(Path(parquet_local))
            parquet_size = Path(parquet_local).stat().st_size
        else:
            parquet_sha256 = ""
            parquet_size = 0

        new_row = {
            "data_particao": month_str,
            "table_name": resource,
            "row_count": q_stats.get("valid", 0) if q_stats else 0,
            "quarantine_count": q_stats.get("quarantine", 0) if q_stats else 0,
            "ia_item_id": item_id,
            "raw_zip_url": f"https://archive.org/download/{item_id}/{zip_basename}",
            # Only write parquet_url when we confirmed the file was in the upload set.
            # An empty sha256 means the Parquet was never generated; writing the URL
            # anyway produces a manifest entry that points to a non-existent file and
            # causes the consolidator to 404 and the sync to skip the month forever.
            "parquet_url": parquet_url if parquet_sha256 else "",
            "quarantine_url": f"https://archive.org/download/{item_id}/quarentena-{month_str}.csv"
            if q_stats and q_stats.get("quarantine", 0) > 0
            else "",
            "uploaded_at": datetime.now().isoformat(),
            # Manifest v2 columns (all optional in the web schema):
            "file_type": artifact.file_type,
            "uf_sigla": "",
            "sort_key": _CONTRATOS_SORT_KEY,
            "row_group_size": PARQUET_ROW_GROUP_SIZE,
            "bloom_filter_columns": _CONTRATOS_BLOOM_FILTER_COLUMNS,
            "sha256": parquet_sha256,
            "file_size_bytes": parquet_size,
        }

        with _manifest_lock:
            manifest = read_manifest_from_ia()
            # Deduplicate only against the canonical row for this (resource,
            # partition). Manifest v2 allows multiple rows per partition
            # (monthly_uf shards, supplemental dailies, other resources);
            # blanket removal would silently drop their hash/size metadata.
            # Empty/missing file_type is treated as canonical for v1
            # backward compat (matches web's isCanonicalRow).
            manifest = [
                r
                for r in manifest
                if not (
                    r["data_particao"] == month_str
                    and r["table_name"] == resource
                    and r.get("file_type", "") in ("", "monthly_canonical")
                )
            ]
            manifest.append(new_row)
            write_manifest_to_ia(manifest, access_key, secret_key)
