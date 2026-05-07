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

from .artifacts import get_artifact
from .ia_uploader import read_manifest_from_ia, register_monthly_uf_shards
from .resources import CONTRATOS, get_resource
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


def _consolidated_file_name(year: int, *, resource: str = CONTRATOS.name) -> str:
    """Annual consolidated parquet basename — `{canonical_table}-{year}.parquet`.

    Drives off the resource's CanonicalTableSpec so atas / future
    resources get their own file alongside contratos in the same
    `baliza-pncp-consolidated` IA item.
    """
    table_name = get_resource(resource).canonical_tables[0].table_name
    return f"{table_name}-{year}.parquet"


def _requires_httpfs(paths_or_urls: list[str]) -> bool:
    """DuckDB needs httpfs only when reading remote URLs."""
    return any(value.startswith(("http://", "https://", "s3://")) for value in paths_or_urls)


def _parse_iso_mtime(value: str) -> datetime.datetime | None:
    """Parse an ISO 8601 uploaded_at string. Returns None when unparseable.

    Manifest writers historically used ``datetime.now().isoformat()`` (naive),
    while the consolidator's IA-mtime helper produces UTC-aware datetimes
    from unix timestamps. Comparing the two would raise ``TypeError``; treat
    naive manifest timestamps as UTC so freshness math stays well-defined.
    """
    if not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # Naive only: declare the assumed zone. We deliberately use
        # `replace(tzinfo=UTC)` (not `astimezone(UTC)`) because the
        # input has no source offset — `astimezone` on a naive datetime
        # would assume the runtime local zone and silently shift the
        # moment, which is exactly the bug Kilo's roast describes.
        # Aware datetimes (offset already known) skip this branch and
        # round-trip unchanged; their absolute moment is preserved.
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return parsed


def _current_year_is_fresh(
    manifest: list[dict], year: int, *, resource: str = CONTRATOS.name
) -> bool:
    """True when the current year's consolidated shards are at-or-after every monthly canonical upload.

    Uses the manifest as a single source of truth: consolidation writes
    ``monthly_uf`` shard rows after a successful IA upload, so the newest
    shard timestamp bounds the last successful consolidation. If a monthly
    canonical upload is newer, current-year consolidation is stale.

    Resources whose canonical table doesn't carry ``uf_sigla`` (e.g. atas)
    skip the per-UF sharding step entirely — for them, freshness is "the
    annual consolidated row is at-or-after every monthly canonical upload"
    instead of comparing canonical to shards. We still gate via the manifest
    by treating the absence of new canonical uploads as fresh.
    """
    year_str = str(year)
    canonical_mtimes: list[datetime.datetime] = []
    shard_mtimes: list[datetime.datetime] = []
    for row in manifest:
        if row.get("table_name") != resource:
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

    # Resources without UF sharding don't write monthly_uf rows; their
    # freshness gate falls back to "any new monthly canonical means rebuild".
    # Conservatively treat them as stale whenever a canonical upload exists
    # — the consolidator's own no-op short-circuit handles repeated calls.
    if not _resource_has_uf_shards(resource):
        return False

    if not shard_mtimes:
        return False  # canonical months exist but no shards → needs first build
    return max(shard_mtimes) >= max(canonical_mtimes)


def _resource_has_uf_shards(resource: str) -> bool:
    """Whether the consolidator should emit per-UF monthly shards.

    Drift-B wiring: the gate now consults
    ``ArtifactSpec(file_type="monthly_uf")`` membership on the resource
    (the canonical declaration of "this resource publishes a per-UF
    shard"). Falls back to ``CanonicalTableSpec.partition_by_uf`` for
    safety while both fields coexist — a cross-check test pins that
    the two never disagree (``test_artifact_spec.py``).
    """
    spec = get_resource(resource)
    artifact_says_yes = get_artifact(spec, "monthly_uf") is not None
    flag_says_yes = spec.canonical_tables[0].partition_by_uf
    return artifact_says_yes or flag_says_yes


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
        self,
        year: int,
        manifest: list[dict] | None = None,
        *,
        resource: str = CONTRATOS.name,
    ) -> list[str]:
        """Read the manifest.csv on IA and get all daily file URLs for ``resource`` in ``year``.

        Propagates ``ManifestReadError`` on transient failures so we never
        silently consolidate from a partial view of the manifest. Filters
        by ``table_name == resource`` so atas rows don't feed contratos
        consolidation (and vice versa).
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
                and row.get("table_name") == resource
                and file_type in ("", "monthly_canonical")
            ):
                url = row.get("parquet_url") or row.get("file_url")
                if url:
                    urls.append(url)
        return urls

    def _check_consolidated_exists_on_ia(
        self, year: int, *, resource: str = CONTRATOS.name
    ) -> bool:
        """Check if the consolidated annual file already exists in the IA item."""
        filename = _consolidated_file_name(year, resource=resource)
        try:
            item = ia.get_item(CONSOLIDATED_IA_ITEM)
            return any(f.get("name") == filename for f in item.files)
        except Exception:
            return False

    def _consolidated_mtime_on_ia(
        self, year: int, *, resource: str = CONTRATOS.name
    ) -> datetime.datetime | None:
        """Return the upload time of the consolidated annual file on IA, or None.

        IA exposes per-file ``mtime`` as a unix timestamp string. The
        freshness gate for resources without UF shards uses this to
        decide "did the file land after the newest monthly canonical
        upload" without piling extra rows into the manifest.

        Returns None when the file is missing OR present without a
        usable mtime — callers must treat None as "can't decide
        freshness from IA, fall back to a rebuild".
        """
        filename = _consolidated_file_name(year, resource=resource)
        try:
            item = ia.get_item(CONSOLIDATED_IA_ITEM)
        except Exception:
            return None
        for f in item.files:
            if f.get("name") != filename:
                continue
            mtime = f.get("mtime")
            if not mtime:
                return None
            try:
                return datetime.datetime.fromtimestamp(int(mtime), tz=datetime.UTC)
            except (TypeError, ValueError):
                return None
        return None

    def consolidate_year(  # noqa: PLR0912, PLR0913, PLR0915
        self,
        year: int,
        ia_access_key: str,
        ia_secret_key: str,
        force: bool = False,
        manifest: list[dict] | None = None,
        *,
        resource: str = CONTRATOS.name,
    ) -> bool:
        """Build and upload annual consolidated Parquet for ``resource`` in ``year``.

        ``manifest`` is optional: when supplied, the current-year freshness
        gate uses it in place of refetching manifest.csv. ``resource`` routes
        through the canonical table from the registry — atas / future
        resources land their own ``{table_name}-{year}.parquet`` alongside
        contratos in the same ``baliza-pncp-consolidated`` IA item.
        """
        frozen = _is_frozen(year)
        filename = _consolidated_file_name(year, resource=resource)
        spec = get_resource(resource)
        table_spec = spec.canonical_tables[0]
        order_by_sql = table_spec.order_by_sql or ", ".join(
            list(table_spec.sort_columns)
            + ([table_spec.pk] if isinstance(table_spec.pk, str) else list(table_spec.pk))
        )

        if frozen and not force:
            if self._check_consolidated_exists_on_ia(year, resource=resource):
                console.print(
                    f"[dim]Skipping {resource}/{year}: frozen year, consolidated file already on IA.[/dim]"
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
            if _current_year_is_fresh(shared_manifest, year, resource=resource):
                console.print(
                    f"[dim]Skipping {resource}/{year}: consolidated shards are up to date.[/dim]"
                )
                return False
            # Resources without per-UF shards (atas etc.) have no
            # monthly_uf rows for the manifest gate to inspect, so the
            # gate above is conservative — it returns False whenever any
            # canonical month exists. Avoid the resulting daily rebuild
            # storm by also checking the consolidated annual file's IA
            # mtime: if it landed after the newest canonical upload,
            # nothing has changed and the rebuild is wasted work.
            if not _resource_has_uf_shards(resource):
                consolidated_mtime = self._consolidated_mtime_on_ia(year, resource=resource)
                if consolidated_mtime is not None:
                    # Fail-closed on malformed canonical timestamps —
                    # if any row's `uploaded_at` is unparseable we can't
                    # tell whether it's older or newer than
                    # ``consolidated_mtime``, so we must rebuild rather
                    # than risk skipping past genuinely fresh data.
                    # Mirrors the policy at line ~96 in
                    # `_current_year_is_fresh` (Codex review on #557).
                    canonical_mtimes: list[datetime.datetime] = []
                    saw_unparseable = False
                    for row in shared_manifest:
                        if row.get("table_name") != resource:
                            continue
                        if not (row.get("data_particao") or "").startswith(str(year)):
                            continue
                        if row.get("file_type", "") not in ("", "monthly_canonical"):
                            continue
                        parsed = _parse_iso_mtime(row.get("uploaded_at") or "")
                        if parsed is None:
                            saw_unparseable = True
                            break
                        canonical_mtimes.append(parsed)
                    if saw_unparseable:
                        # Force rebuild rather than risk a false-fresh skip.
                        pass
                    elif canonical_mtimes:
                        newest_canonical = max(canonical_mtimes)
                        if consolidated_mtime >= newest_canonical:
                            console.print(
                                f"[dim]Skipping {resource}/{year}: consolidated annual file "
                                f"({consolidated_mtime.isoformat()}) is at-or-after the newest "
                                f"canonical upload ({newest_canonical.isoformat()}).[/dim]"
                            )
                            return False

        console.print(f"[cyan]Consolidating {resource}/{year}...[/cyan]")

        # 1. Get daily file URLs from manifest.csv (the helper fetches on its
        #    own when no manifest is threaded through).
        daily_urls = self._get_daily_urls_for_year(
            year, manifest=shared_manifest, resource=resource
        )
        if not daily_urls:
            console.print(
                f"[yellow]No daily files found in manifest for {resource}/{year}.[/yellow]"
            )
            return False

        console.print(f"  Found {len(daily_urls)} daily files.")
        is_current_year = year == datetime.date.today().year
        wants_uf_shards = _resource_has_uf_shards(resource)

        # 2. Use DuckDB httpfs to stream+sort and write one consolidated Parquet
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / filename
            url_list = ", ".join(f"'{u}'" for u in daily_urls)

            with duckdb.connect(":memory:") as con:
                if _requires_httpfs(daily_urls):
                    self._load_httpfs_extension(con)
                console.print(f"  Merging {len(daily_urls)} files via DuckDB...")
                try:
                    con.execute(f"""
                        COPY (
                            SELECT *
                            FROM read_parquet([{url_list}])
                            ORDER BY {order_by_sql}
                        ) TO '{output_path}'
                        ({DUCKDB_PARQUET_COPY_OPTIONS})
                    """)
                except Exception as e:
                    if "HTTP 404" in str(e):
                        console.print(
                            f"[yellow]⚠ Skipping {year}: one or more source files "
                            f"returned HTTP 404 (file may have been removed from IA). "
                            f"Will retry on a future run.[/yellow]"
                        )
                        return False
                    raise

                size_mb = output_path.stat().st_size / 1_048_576
                console.print(f"  Written {filename} ({size_mb:.1f} MB). Uploading to IA...")

                files_to_upload = {filename: str(output_path)}

                # 2a. Per-UF shards (current year only; past years stay single-file).
                # Gated on the resource's canonical table actually carrying
                # `uf_sigla` — atas etc. don't, so they ship as single annual files.
                shards: list[dict] = []
                if is_current_year and wants_uf_shards:
                    shards = self._build_per_uf_shards(
                        con, output_path, year, Path(tmpdir), resource=resource
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
                        table_name=table_spec.table_name,
                        shards=shard_rows,
                        access_key=ia_access_key,
                        secret_key=ia_secret_key,
                    )

                # 4. Rebuild cumulative dimension files (current year only —
                # captures latest rollups consumers need for detail pages).
                # The aggregations use contratos-only columns
                # (cnpj_orgao + valor_inicial + ni_fornecedor + …); other
                # resources skip this step until per-resource dim shapes
                # are defined.
                if is_current_year and resource == CONTRATOS.name:
                    self._rebuild_dimensions(
                        con, output_path, Path(tmpdir), ia_access_key, ia_secret_key
                    )

        console.print(
            f"[green]✓ {resource}/{year} consolidated uploaded ({size_mb:.1f} MB).[/green]"
        )
        return True

    @staticmethod
    def _load_httpfs_extension(con: duckdb.DuckDBPyConnection) -> None:
        """Load DuckDB httpfs, installing only when not already available."""
        try:
            con.execute("LOAD httpfs;")
        except duckdb.Error:
            con.execute("INSTALL httpfs;")
            con.execute("LOAD httpfs;")

    @staticmethod
    def _build_per_uf_shards(
        con: duckdb.DuckDBPyConnection,
        source_path: Path,
        year: int,
        tmp_root: Path,
        *,
        resource: str = CONTRATOS.name,
    ) -> list[dict]:
        """Emit one ``{table}-YYYY-uf=XX.parquet`` per UF present in the file.

        Flat filenames (not Hive directories) so IA preserves them verbatim
        and the Journey 6 URL regex still matches the canonical file.
        Each shard is bloom-filtered + sorted like the canonical.

        Returns per-shard metadata (uf, path, row_count, sha256, file_size_bytes)
        so callers can register shard rows in the manifest. Caller is
        responsible for gating on ``_resource_has_uf_shards`` — this method
        assumes the canonical schema carries ``uf_sigla``.
        """
        spec = get_resource(resource)
        table_spec = spec.canonical_tables[0]
        table_name = table_spec.table_name
        order_by_sql = table_spec.order_by_sql or ", ".join(
            list(table_spec.sort_columns)
            + ([table_spec.pk] if isinstance(table_spec.pk, str) else list(table_spec.pk))
        )
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
            shard_name = f"{table_name}-{year}-uf={uf}.parquet"
            shard_path = shard_dir / shard_name
            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{source_path}')
                    WHERE uf_sigla = ?
                    ORDER BY {order_by_sql}
                ) TO '{shard_path}'
                ({DUCKDB_PARQUET_COPY_OPTIONS})
                """,
                [uf],
            )
            count_row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{shard_path}')").fetchone()
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
            con.execute(f"COPY ({sql}) TO '{dim_path}' ({DUCKDB_PARQUET_COPY_OPTIONS})")
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
        console.print(f"  Rebuilt {len(files_to_upload)} cumulative dimension files.")

    def consolidate_all(  # noqa: PLR0913
        self,
        start_year: int,
        ia_access_key: str,
        ia_secret_key: str,
        force: bool = False,
        manifest: list[dict] | None = None,
        *,
        resource: str = CONTRATOS.name,
    ) -> dict[int, bool]:
        """Consolidate ``resource`` from ``start_year`` through the current year.

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
                resource=resource,
            )
        return results
