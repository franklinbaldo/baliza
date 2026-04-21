"""Annual consolidation of daily PNCP Parquet packages into single-file annual archives.

Strategy:
- Past years (frozen): built once ~60 days after year end, never touched again.
- Current year (hot): rebuilt weekly, replacing the existing IA file each time.

Reading is done via DuckDB httpfs directly from IA URLs (no local download).
"""

from __future__ import annotations

import datetime
import hashlib
import tempfile
from pathlib import Path

import duckdb
import internetarchive as ia
from rich.console import Console

from .ia_uploader import read_manifest_from_ia, register_monthly_uf_shards
from .utils import DUCKDB_PARQUET_COPY_OPTIONS

CONSOLIDATED_IA_ITEM = "baliza-pncp-consolidated"
MANIFEST_IA_ITEM = "baliza-pncp-manifest"
DIMENSIONS_IA_ITEM = "baliza-pncp-dimensions"
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


def _parse_iso_mtime(value: str) -> datetime.datetime | None:
    """Parse an ISO 8601 uploaded_at string. Returns None when unparseable."""
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _current_year_is_fresh(manifest: list[dict], year: int) -> bool:
    """True when the current year's consolidated shards are at-or-after every monthly canonical upload.

    Uses the manifest as a single source of truth: consolidation writes
    ``monthly_uf`` shard rows after a successful IA upload, so the newest
    shard timestamp bounds the last successful consolidation. If a monthly
    canonical upload is newer, current-year consolidation is stale.
    """
    year_str = str(year)
    canonical_mtimes: list[datetime.datetime] = []
    shard_mtimes: list[datetime.datetime] = []
    for row in manifest:
        if row.get("table_name") != "contratos":
            continue
        part = row.get("data_particao") or ""
        if not part.startswith(year_str):
            continue
        parsed = _parse_iso_mtime(row.get("uploaded_at") or "")
        if parsed is None:
            # Unparseable timestamp — fail closed: treat as needing rebuild.
            return False
        file_type = row.get("file_type") or ""
        if file_type in ("", "monthly_canonical"):
            canonical_mtimes.append(parsed)
        elif file_type == "monthly_uf":
            shard_mtimes.append(parsed)
    if not canonical_mtimes:
        return True  # nothing to consolidate yet
    if not shard_mtimes:
        return False  # canonical months exist but no shards → needs first build
    return max(shard_mtimes) >= max(canonical_mtimes)


class IAConsolidator:
    """Reads daily Parquet files from IA (via manifest) and builds annual consolidated files."""

    def __init__(self) -> None:
        pass

    def _read_manifest(self) -> list[dict]:
        """Fetch manifest.csv from IA.

        Returns ``[]`` only when the manifest is genuinely absent (HTTP 404 —
        fresh IA item). Any other failure (network error, non-200, parse
        error) raises ``ManifestReadError`` so the freshness gate in
        ``_current_year_is_fresh`` cannot silently treat a transient outage
        as "no data to consolidate."
        """
        return read_manifest_from_ia()

    def _get_daily_urls_for_year(
        self, year: int, manifest: list[dict] | None = None
    ) -> list[str]:
        """Read the manifest.csv on IA and get all daily contratos file URLs for the year.

        Propagates ``ManifestReadError`` on transient failures so we never
        silently consolidate from a partial view of the manifest.
        """
        rows = manifest if manifest is not None else self._read_manifest()
        urls = []
        year_str = str(year)
        for row in rows:
            # Exclude v2 shard / supplemental rows — their contracts already
            # live in the canonical monthly files and re-ingesting them here
            # would duplicate. Empty/missing file_type is v1 canonical.
            file_type = row.get("file_type", "")
            if (
                (row.get("data_particao") or "").startswith(year_str)
                and row.get("table_name") == "contratos"
                and file_type in ("", "monthly_canonical")
            ):
                url = row.get("parquet_url") or row.get("file_url")
                if url:
                    urls.append(url)
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
        manifest: list[dict] | None = None,
    ) -> bool:
        """Build and upload annual consolidated Parquet for the given year.

        ``manifest`` is optional: when supplied, the current-year freshness
        gate uses it in place of refetching manifest.csv.
        """
        frozen = _is_frozen(year)
        filename = _consolidated_file_name(year)

        if frozen and not force:
            if self._check_consolidated_exists_on_ia(year):
                console.print(
                    f"[dim]Skipping {year}: frozen year, consolidated file already on IA.[/dim]"
                )
                return False

        # Fetch the manifest lazily: the freshness gate (current year only)
        # and the URL listing both need it, but fetching here keeps a single
        # consistent view for both and avoids a redundant HTTP round-trip.
        # When the caller already supplied one, reuse it verbatim.
        shared_manifest = manifest

        # Current-year freshness gate: skip if shards are already at-or-after
        # the newest monthly canonical upload. Past years bypass this check —
        # `_is_frozen` + `_check_consolidated_exists_on_ia` already cover them.
        if not force and year == datetime.date.today().year:
            if shared_manifest is None:
                shared_manifest = self._read_manifest()
            if _current_year_is_fresh(shared_manifest, year):
                console.print(
                    f"[dim]Skipping {year}: consolidated shards are up to date.[/dim]"
                )
                return False

        console.print(f"[cyan]Consolidating {year}...[/cyan]")

        # 1. Get daily file URLs from manifest.csv (the helper fetches on its
        #    own when no manifest is threaded through).
        daily_urls = self._get_daily_urls_for_year(year, manifest=shared_manifest)
        if not daily_urls:
            console.print(f"[yellow]No daily files found in manifest for {year}.[/yellow]")
            return False

        console.print(f"  Found {len(daily_urls)} daily files.")
        is_current_year = year == datetime.date.today().year

        # 2. Use DuckDB httpfs to stream+sort and write one consolidated Parquet
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
                        ORDER BY cnpj_orgao, data_publicacao DESC, numero_controle_pncp
                    ) TO '{output_path}'
                    ({DUCKDB_PARQUET_COPY_OPTIONS})
                """)

                size_mb = output_path.stat().st_size / 1_048_576
                console.print(
                    f"  Written {filename} ({size_mb:.1f} MB). Uploading to IA..."
                )

                files_to_upload = {filename: str(output_path)}

                # 2a. Per-UF shards (current year only; past years stay single-file).
                shards: list[dict] = []
                if is_current_year:
                    shards = self._build_per_uf_shards(
                        con, output_path, year, Path(tmpdir)
                    )
                    for s in shards:
                        files_to_upload[s["filename"]] = str(s["path"])
                    console.print(f"  Built {len(shards)} per-UF shards.")

                # 3. Upload to baliza-pncp-consolidated/
                ia.upload(
                    CONSOLIDATED_IA_ITEM,
                    files=files_to_upload,
                    access_key=ia_access_key,
                    secret_key=ia_secret_key,
                    metadata={
                        "title": "Baliza PNCP Consolidated Data",
                        "mediatype": "data",
                        "collection": "opensource_media",
                    },
                    retries=3,
                )

                # 3a. Register shard rows in the manifest so the web resolver
                # can discover them. Without this, uf-filtered archive queries
                # never pick up the narrower shards.
                if shards:
                    shard_rows = [
                        {
                            "uf_sigla": s["uf_sigla"],
                            "parquet_url": f"https://archive.org/download/{CONSOLIDATED_IA_ITEM}/{s['filename']}",
                            "sha256": s["sha256"],
                            "file_size_bytes": s["file_size_bytes"],
                            "row_count": s["row_count"],
                            "ia_item_id": CONSOLIDATED_IA_ITEM,
                        }
                        for s in shards
                    ]
                    register_monthly_uf_shards(
                        year=year,
                        table_name="contratos",
                        shards=shard_rows,
                        access_key=ia_access_key,
                        secret_key=ia_secret_key,
                    )

                # 4. Rebuild cumulative dimension files (current year only —
                # captures latest rollups consumers need for detail pages).
                if is_current_year:
                    self._rebuild_dimensions(
                        con, output_path, Path(tmpdir), ia_access_key, ia_secret_key
                    )

        console.print(f"[green]✓ {year} consolidated uploaded ({size_mb:.1f} MB).[/green]")
        return True

    @staticmethod
    def _build_per_uf_shards(
        con: duckdb.DuckDBPyConnection,
        source_path: Path,
        year: int,
        tmp_root: Path,
    ) -> list[dict]:
        """Emit one contratos-YYYY-uf=XX.parquet per UF present in the file.

        Flat filenames (not Hive directories) so IA preserves them verbatim
        and the Journey 6 URL regex still matches the canonical file.
        Each shard is bloom-filtered + sorted like the canonical.

        Returns per-shard metadata (uf, path, row_count, sha256, file_size_bytes)
        so callers can register shard rows in the manifest.
        """
        shard_dir = tmp_root / "shards"
        shard_dir.mkdir(exist_ok=True)

        ufs = [
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT uf_sigla FROM read_parquet('{source_path}') "
                f"WHERE uf_sigla IS NOT NULL ORDER BY uf_sigla"
            ).fetchall()
        ]

        shards: list[dict] = []
        for uf in ufs:
            shard_name = f"contratos-{year}-uf={uf}.parquet"
            shard_path = shard_dir / shard_name
            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{source_path}')
                    WHERE uf_sigla = ?
                    ORDER BY cnpj_orgao, data_publicacao DESC, numero_controle_pncp
                ) TO '{shard_path}'
                ({DUCKDB_PARQUET_COPY_OPTIONS})
                """,
                [uf],
            )
            count_row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{shard_path}')"
            ).fetchone()
            row_count = count_row[0] if count_row else 0
            sha256 = hashlib.sha256(shard_path.read_bytes()).hexdigest()
            shards.append(
                {
                    "uf_sigla": uf,
                    "path": shard_path,
                    "filename": shard_name,
                    "row_count": row_count,
                    "sha256": sha256,
                    "file_size_bytes": shard_path.stat().st_size,
                }
            )
        return shards

    @staticmethod
    def _rebuild_dimensions(
        con: duckdb.DuckDBPyConnection,
        source_path: Path,
        tmp_root: Path,
        ia_access_key: str,
        ia_secret_key: str,
    ) -> None:
        """Build cumulative dim-{orgaos,fornecedores,unidades}-latest.parquet.

        Aggregated from the current-year consolidated file. These replace
        per-day dimension files as the primary source for supplier/agency
        detail pages; per-day files stay in their monthly items as
        supplemental for audit / diff use cases (Journey 7).
        """
        dim_dir = tmp_root / "dims"
        dim_dir.mkdir(exist_ok=True)

        specs = [
            (
                "dim-orgaos-latest.parquet",
                f"""
                SELECT
                    cnpj_orgao                      AS cnpj,
                    MAX(razao_social_orgao)         AS razao_social,
                    MAX(poder_id)                   AS poder_id,
                    MAX(esfera_id)                  AS esfera_id,
                    COUNT(*)                        AS contratos_total,
                    SUM(valor_inicial)              AS valor_total,
                    MIN(data_publicacao)            AS primeiro_contrato_at,
                    MAX(data_publicacao)            AS ultimo_contrato_at
                FROM read_parquet('{source_path}')
                GROUP BY cnpj_orgao
                ORDER BY cnpj
                """,
            ),
            (
                "dim-fornecedores-latest.parquet",
                f"""
                SELECT
                    ni_fornecedor,
                    MAX(tipo_pessoa)                    AS tipo_pessoa,
                    MAX(nome_razao_social_fornecedor)   AS nome_razao_social,
                    MAX(codigo_pais_fornecedor)         AS codigo_pais,
                    COUNT(*)                            AS contratos_total,
                    SUM(valor_inicial)                  AS valor_total,
                    MIN(data_publicacao)                AS primeiro_contrato_at,
                    MAX(data_publicacao)                AS ultimo_contrato_at
                FROM read_parquet('{source_path}')
                WHERE ni_fornecedor IS NOT NULL
                GROUP BY ni_fornecedor
                ORDER BY ni_fornecedor
                """,
            ),
            (
                "dim-unidades-latest.parquet",
                f"""
                SELECT
                    codigo_unidade,
                    cnpj_orgao,
                    MAX(nome_unidade)   AS nome_unidade,
                    MAX(uf_sigla)       AS uf_sigla,
                    MAX(municipio_nome) AS municipio_nome,
                    MAX(codigo_ibge)    AS codigo_ibge,
                    COUNT(*)            AS contratos_total
                FROM read_parquet('{source_path}')
                WHERE codigo_unidade IS NOT NULL
                GROUP BY codigo_unidade, cnpj_orgao
                ORDER BY codigo_unidade
                """,
            ),
        ]

        files_to_upload: dict[str, str] = {}
        for filename, sql in specs:
            dim_path = dim_dir / filename
            con.execute(
                f"COPY ({sql}) TO '{dim_path}' ({DUCKDB_PARQUET_COPY_OPTIONS})"
            )
            files_to_upload[filename] = str(dim_path)

        ia.upload(
            DIMENSIONS_IA_ITEM,
            files=files_to_upload,
            access_key=ia_access_key,
            secret_key=ia_secret_key,
            metadata={
                "title": "Baliza PNCP Cumulative Dimensions",
                "mediatype": "data",
                "collection": "opensource_media",
            },
            retries=3,
        )
        console.print(
            f"  Rebuilt {len(files_to_upload)} cumulative dimension files."
        )

    def consolidate_all(
        self,
        start_year: int,
        ia_access_key: str,
        ia_secret_key: str,
        force: bool = False,
        manifest: list[dict] | None = None,
    ) -> dict[int, bool]:
        """Consolidate from start_year through the current year.

        Reads the manifest once up-front (unless ``manifest`` is supplied)
        so the current-year freshness gate doesn't re-fetch per year.
        """
        current_year = datetime.date.today().year
        results: dict[int, bool] = {}
        shared_manifest = manifest if manifest is not None else self._read_manifest()
        for year in range(start_year, current_year + 1):
            results[year] = self.consolidate_year(
                year,
                ia_access_key,
                ia_secret_key,
                force=force,
                manifest=shared_manifest,
            )
        return results
