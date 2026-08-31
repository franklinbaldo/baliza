from __future__ import annotations

import json
import subprocess
import threading
import time
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import structlog
from pydantic import ValidationError
from rich.console import Console
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .engine import BalizaEngine
from .partitioning import partition_label
from .resources import CONTRATOS, get_resource
from .utils import validate_url

logger = structlog.get_logger()
console = Console()

# PNCP honours up to 500 items per page for the `contratos` endpoint. Using the
# maximum keeps the CLI extractor from issuing 5x as many requests for the same
# data.
PAGE_SIZE = 500

# Sentinel written once every expected page for a month is on disk. Presence
# alone is the signal; contents are irrelevant. Lives inside the month
# directory so IAUploader.upload_month's existing rmtree cleans it up.
FETCHED_SENTINEL = ".fetched"





def _is_retryable_error(exception: BaseException) -> bool:
    if isinstance(exception, httpx.HTTPStatusError):
        # Retry on 429 and any 5xx (covers 501, 505, proxy 52x, etc.)
        if exception.response.status_code == 429 or exception.response.status_code >= 500:
            return True
        # Explicit 404 block -- don't retry, let it bubble
        if exception.response.status_code == 404:
            return False
    elif isinstance(
        exception, RetryablePayloadError | httpx.RequestError | subprocess.CalledProcessError
    ):
        return True
    return False

def _validate_resource(resource: str):
    """Confirm the resource is registered.

    Charset safety (no path-traversal, no shell metacharacters) is
    enforced at registration time in PNCPResource.__post_init__, so
    any name returned by the registry is safe to interpolate into
    paths, URLs, and DuckDB identifiers. Here we just look it up.
    """
    get_resource(resource)


class RetryablePayloadError(Exception):
    """Raised when PNCP returns a syntactically invalid payload.

    We observe occasional empty/HTML upstream bodies with HTTP 200 while the
    endpoint is under stress. Those payloads are transient in practice and
    should participate in tenacity retries.
    """


class PNCPExtractor:
    """Extracts data from PNCP API and ingests into DuckDB via Ibis."""

    def __init__(
        self,
        engine: BalizaEngine | None = None,
        base_url: str = "https://pncp.gov.br/api/consulta/v1",
        use_curl: bool = False,
    ):
        self.engine = engine
        self.base_url = validate_url(base_url)
        self.use_curl = use_curl
        self._lock = threading.Lock()

        self.headers = {
            "accept": "*/*",
            "User-Agent": "Baliza/1.0 (Data extraction pipeline)",
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)

    @retry(
        retry=retry_if_exception(_is_retryable_error),
        stop=stop_after_attempt(6),
        wait=wait_exponential(multiplier=2, min=4, max=60),
    )
    def fetch_page(
        self, resource: str, start_date: datetime, end_date: datetime, page: int = 1,
        *, extra_params: dict[str, str | int] | None = None,
    ) -> dict[str, Any]:
        """Fetch a single page from the PNCP API with resumption support.

        ``extra_params`` lets callers add resource-specific query
        parameters that aren't part of the date/page contract — e.g.
        publicacoes' modalidade fan-out. The values fold into the cache
        filename so different fan-out points don't collide.
        """
        _validate_resource(resource)
        spec = get_resource(resource)
        cached_data = self._load_cached_page(
            resource=resource, start_date=start_date, page=page, extra_params=extra_params,
        )
        if cached_data is not None:
            return cached_data

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        # Resource registry's spec.fetch.endpoint is the URL path —
        # publicacoes lives at /v1/contratacoes/publicacao while its
        # resource name is just "publicacoes". Using the endpoint
        # instead of the bare resource name makes that work.
        url = f"{self.base_url}/{spec.fetch.endpoint}"
        # Per-resource page size: publicacoes caps at 50 while
        # contratos / atas accept 500. Honoring spec.fetch.max_page_size
        # avoids 400s from over-asking. The legacy global PAGE_SIZE is
        # kept as the fallback for the curl path's URL string template.
        page_size = spec.fetch.max_page_size or PAGE_SIZE
        # Date param names come from the spec — contratos/atas/publicacoes
        # use dataInicial/dataFinal but PCA uses dataInicio/dataFim.
        # Hardcoding either name 400s on the resource that uses the other.
        params: dict[str, str | int] = {
            spec.fetch.date_param_start: start_str,
            spec.fetch.date_param_end: end_str,
            "pagina": page,
            "tamanhoPagina": page_size,
        }
        if extra_params:
            params.update(extra_params)
        logger.info("fetching_page_params", resource=resource, url=url, params=params)

        if self.use_curl:
            data = self._fetch_with_curl(
                resource=resource,
                page=page,
                url=url,
                start_str=start_str,
                end_str=end_str,
                extra_params=extra_params,
                page_size=page_size,
            )
        else:
            data = self._fetch_with_httpx(
                resource=resource,
                page=page,
                url=url,
                params=params,
            )
        self._save_raw(resource, start_date, page, data, extra_params=extra_params)
        return data

    def _param_suffix(self, extra_params: dict[str, str | int] | None) -> str:
        """Stable filename suffix encoding fan-out param values.

        Empty params (the typical case for contratos / atas) returns
        ``""`` so cache paths stay byte-identical with the pre-fan-out
        layout. With params, the suffix is sorted-deterministic so the
        same fan-out point hits the same file every run.
        """
        if not extra_params:
            return ""
        pairs = sorted((k, str(v)) for k, v in extra_params.items())
        return "_" + "_".join(f"{k}{v}" for k, v in pairs)

    def _cache_filename(
        self, resource: str, start_date: datetime, page: int,
        *, extra_params: dict[str, str | int] | None = None,
    ) -> Path:
        # Drift-D wiring: cache directory tracks the resource's
        # partition label (``YYYY-MM`` for monthly, ``YYYY`` for
        # annual) so mirror_month / build_month, which now write/read
        # partition-labeled paths, hit the same files. Fan-out params
        # fold into the filename suffix.
        partition_str = partition_label(get_resource(resource), start_date.date())
        suffix = self._param_suffix(extra_params)
        return Path(f"data/raw/{partition_str}/{resource}{suffix}_p{page}.json")

    def _load_cached_page(
        self, resource: str, start_date: datetime, page: int,
        *, extra_params: dict[str, str | int] | None = None,
    ) -> dict[str, Any] | None:
        """Return cached page when valid; otherwise heal corrupt cache and refetch."""
        filename = self._cache_filename(
            resource=resource, start_date=start_date, page=page, extra_params=extra_params,
        )
        if not (filename.exists() and filename.stat().st_size > 0):
            return None

        try:
            with open(filename) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("corrupt_cache_found", file=str(filename), error=str(e))
            self._unlink_if_exists(filename)
            return None

        if isinstance(data, dict) and ("data" in data or "totalPaginas" in data):
            return data

        logger.warning(
            "corrupt_cache_found",
            file=str(filename),
            error="schema mismatch (no 'data' or 'totalPaginas' key)",
        )
        self._unlink_if_exists(filename)
        return None

    def _unlink_if_exists(self, filename: Path) -> None:
        try:
            filename.unlink()
        except OSError:
            pass

    def _fetch_with_curl(  # noqa: PLR0913
        self, resource: str, page: int, url: str, start_str: str, end_str: str,
        *, extra_params: dict[str, str | int] | None = None,
        page_size: int = PAGE_SIZE,
    ) -> dict[str, Any]:
        spec = get_resource(resource)
        query = (
            f"{spec.fetch.date_param_start}={start_str}"
            f"&{spec.fetch.date_param_end}={end_str}"
            f"&pagina={page}&tamanhoPagina={page_size}"
        )
        if extra_params:
            query += "&" + "&".join(f"{k}={v}" for k, v in extra_params.items())
        try:
            result = subprocess.run(
                [
                    "curl",
                    "-s",
                    "--max-time",
                    "30",
                    "--connect-timeout",
                    "10",
                    "-H",
                    "accept: */*",
                    "-H",
                    f"User-Agent: {self.headers['User-Agent']}",
                    f"{url}?{query}",
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=45,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            raise RetryablePayloadError(f"curl failed for {resource} page {page}: {e}") from e
        return self._parse_json_payload(
            payload=result.stdout.strip(),
            resource=resource,
            page=page,
        )

    def _fetch_with_httpx(
        self, resource: str, page: int, url: str, params: dict[str, str | int]
    ) -> dict[str, Any]:
        # PROTECT AGAINST DOS: Stream response and check size
        max_size = 15 * 1024 * 1024  # 15MB limit
        content = b""
        with self.client.stream("GET", url, params=params) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_bytes():
                content += chunk
                if len(content) > max_size:
                    raise ValueError(f"Response too large: {len(content)} bytes")
        return self._parse_json_payload(payload=content.strip(), resource=resource, page=page)

    def _parse_json_payload(self, payload: str | bytes, resource: str, page: int) -> dict[str, Any]:
        if not payload:
            raise RetryablePayloadError(f"Empty response body for {resource} page {page}")
        try:
            return json.loads(payload)
        except json.JSONDecodeError as e:
            raise RetryablePayloadError(
                f"Invalid JSON response for {resource} page {page}: {e}"
            ) from e

    def _save_raw(
        self, resource: str, start_date: datetime, page: int, data: dict[str, Any],
        *, extra_params: dict[str, str | int] | None = None,
    ):
        """Save raw JSON payload to disk with deterministic name."""
        # Drift-D wiring: see _cache_filename for the rationale.
        partition_str = partition_label(get_resource(resource), start_date.date())
        raw_dir = Path("data/raw") / partition_str
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Deterministic filename for resumability — fan-out params fold
        # into the suffix so different modalidade pages don't collide.
        suffix = self._param_suffix(extra_params)
        filename = raw_dir / f"{resource}{suffix}_p{page}.json"
        with open(filename, "w") as f:
            json.dump(data, f, ensure_ascii=False)

    def probe_range(
        self, resource: str, start_date: datetime, end_date: datetime,
        *, extra_params: dict[str, str | int] | None = None,
    ) -> dict[str, Any]:
        """Fetch page 1 to determine total pages and registry count for a range."""
        data = self.fetch_page(
            resource, start_date, end_date, page=1, extra_params=extra_params,
        )
        return {
            "total_pages": data.get("totalPaginas", 1),
            "total_registries": data.get("totalRegistros", 0),
        }

    def _require_engine(self, operation: str) -> BalizaEngine:
        """Return the shared engine, or fail loudly when it was never wired."""
        if self.engine is None:
            raise RuntimeError(
                f"{operation} requires an engine — construct PNCPExtractor(engine=...)"
            )
        return self.engine

    def _archive_legacy_contratos_table(self) -> None:
        """Rename a pre-flatten main.contratos out of the way on upgrade.

        Older BALIZA versions stored `main.contratos` with the raw Pydantic
        dump (camelCase keys, nested structs). The post-flatten schema uses
        snake_case scalars with PK `numero_controle_pncp`; upserting on top
        of a legacy table raises IbisTypeError because the PK column is
        absent.

        We archive rather than drop: successful sync runs delete
        `data/raw/` in `IAUploader.upload_month`, so the local DuckDB file
        is the only durable copy of rows for months that already shipped.
        The legacy rows are preserved as `main.contratos_legacy_<ts>` for
        the user to inspect / recover; the next `upsert_rows` recreates
        `main.contratos` in the new shape.

        Idempotent under parallel sync workers: DuckDB serializes catalog
        operations across connections, so the rename races cleanly — one
        worker wins, others see the legacy shape gone on their re-check
        and short-circuit. Timestamp-suffixed archive names avoid
        collisions if a user ends up doing multiple upgrade rounds.
        """
        if not self._has_legacy_contratos_shape():
            return

        engine = self._require_engine("_archive_legacy_contratos_table")
        archive_name = f"contratos_legacy_{int(time.time() * 1000)}"
        logger.warning(
            "archiving_legacy_contratos_schema",
            archive_table=f"main.{archive_name}",
        )
        try:
            engine.con.raw_sql(f"ALTER TABLE main.contratos RENAME TO {archive_name}")
        except Exception as e:
            # A parallel worker already did the rename, or `contratos` was
            # dropped/renamed out from under us. Tolerate the race as long
            # as the post-condition (no legacy-shape contratos) holds.
            if self._has_legacy_contratos_shape():
                raise
            logger.info("legacy_archive_raced", error=str(e))

    def _has_legacy_contratos_shape(self) -> bool:
        """Return True iff main.contratos exists AND has the camelCase PK
        without the snake_case PK. Any lookup error is treated as 'no'."""
        try:
            engine = self._require_engine("_has_legacy_contratos_shape")
            tables = engine.con.list_tables(database="main")
            if CONTRATOS.name not in tables:
                return False
            columns = set(engine.con.table(CONTRATOS.name, database="main").schema().names)
        except Exception:
            return False
        return "numeroControlePNCP" in columns and "numero_controle_pncp" not in columns

    def _iter_raw_entries(self, raw_dir: Path, resource_name: str, response_data_key: str) -> Iterator[dict]:  # noqa: E501
        # Match both legacy (`resource_p*.json`) and fan-out
        # (`resource_codigoModalidadeContratacao5_p*.json`) cache
        # filenames. The original glob `resource_p*.json` would have
        # silently skipped every fan-out page, ingesting nothing for
        # publicacoes (Codex P1).
        """Yield parsed JSON entries from the raw data directory."""
        for json_file in sorted(raw_dir.glob(f"{resource_name}*_p*.json")):
            try:
                with open(json_file) as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("corrupt_cache_found", file=str(json_file), error=str(e))
                try:
                    json_file.unlink()
                except OSError:
                    pass
                continue

            entries = data.get(response_data_key) or []
            yield from entries

    def _iter_valid_rows(
        self,
        entries: Iterator[dict],
        spec: Any,
        start_date: datetime,
        stats: dict[str, int],
    ) -> Iterator[dict]:
        """Yield validated and flattened rows from raw entries, updating stats/quarantine."""
        engine = self._require_engine("_iter_valid_rows")
        entity_model = spec.entity_model
        flatten_fn = spec.canonical_tables[0].flatten_fn

        for entry in entries:
            try:
                validated = entity_model.model_validate(entry)
                if flatten_fn:
                    flat = flatten_fn(validated.model_dump())
                    # PCA's flatten emits one row per nested item, so it
                    # returns ``list[dict]``. Contratos/atas/publicacoes
                    # return a single ``dict``. Normalize to a sequence
                    # so the upsert downstream gets a flat list[dict].
                    if isinstance(flat, list):
                        yield from flat
                    else:
                        yield flat
                else:
                    yield validated.model_dump()
                stats["valid"] += 1
            except ValidationError as e:
                stats["quarantine"] += 1
                logger.warning("validation_failed", error=str(e), entry_id=entry.get("id"))
                engine.quarantine_record(spec.name, start_date, str(e), entry)

    def ingest_range(
        self,
        start_date: datetime,
        *,
        resource: str = CONTRATOS.name,
    ) -> dict[str, int]:
        """Validate and ingest all raw JSON files for a specific month/range into the shared engine.

        Args:
            start_date: First day of the month being ingested.
            resource: Registered resource name. Determines the Pydantic
                entity model used for validation, the canonical tables
                to upsert into, and the quarantine bucket. Adding a new
                resource only needs ``entity_model`` set on its
                ``PNCPResource`` and a ``flatten_fn`` on its canonical
                table spec — no extractor changes.
        """
        engine = self._require_engine("ingest_range")

        spec = get_resource(resource)
        if spec.entity_model is None:
            raise RuntimeError(
                f"resource {resource!r} has no entity_model; cannot validate"
            )

        # Drift-D wiring: ingest reads from the partition-labeled
        # directory mirror_month / _save_raw wrote to.
        partition_str = partition_label(spec, start_date.date())
        raw_dir = Path("data/raw") / partition_str

        stats = {"valid": 0, "quarantine": 0}

        if not raw_dir.exists():
            return stats

        # Legacy archive step is contratos-specific by definition (it
        # migrates a pre-snake-case table named 'contratos' that never
        # existed for any other resource).
        if resource == CONTRATOS.name:
            self._archive_legacy_contratos_table()

        # Flattened pipeline using generators
        raw_entries = self._iter_raw_entries(
            raw_dir, resource, spec.fetch.response_data_key
        )
        valid_rows = list(self._iter_valid_rows(raw_entries, spec, start_date, stats))

        # Ingest valid rows into Ibis (shared engine) via UPSERT
        if valid_rows:
            # Direct memory ingestion (Idempotent)
            for table_spec in spec.canonical_tables:
                engine.upsert_rows(
                    valid_rows, table_spec.table_name, schema="main", pk=table_spec.pk
                )

        return stats

    def export_quarantine(self, extraction_date: datetime, output_path: Path) -> bool:
        """Export session quarantine to CSV if not empty."""
        engine = self._require_engine("export_quarantine")
        try:
            q_table = engine.get_table("quarantine", schema="baliza_state")
            # Filter for current date if possible, but in stateless per-day loop,
            # the quarantine table is fresh for this run.
            df = q_table.execute()
            if not df.empty:
                df.to_csv(output_path, index=False)
                return True
        except Exception as e:
            logger.error("quarantine_export_failed", date=extraction_date.isoformat(), error=str(e))
        return False

    def _update_resource_health(
        self, resource: str, extraction_date: datetime, rows_extracted: int
    ):
        """Update resource health state based on extraction results (Circuit Breaker logic)."""
        try:
            engine = self._require_engine("_update_resource_health")
            # Use raw_sql for DDL on Ibis connection
            # First ensure schema exists
            engine.con.raw_sql("CREATE SCHEMA IF NOT EXISTS baliza_state")

            # Ensure table exists
            engine.con.raw_sql("""
                CREATE TABLE IF NOT EXISTS baliza_state.resource_health (
                    resource TEXT PRIMARY KEY,
                    consecutive_empty_days INTEGER DEFAULT 0,
                    last_nonempty_date DATE,
                    status TEXT DEFAULT 'healthy',
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # For queries, we can use the native duckdb connection for simplicity in DDL/DML
            # or use Ibis table expressions. Let's use the native connection for these raw updates.
            native_con = engine.con.con

            # Get current state
            state = native_con.execute(
                "SELECT consecutive_empty_days, last_nonempty_date "
                "FROM baliza_state.resource_health WHERE resource = ?",
                [resource],
            ).fetchone()

            if not state:
                # Initialize
                empty_days = 1 if rows_extracted == 0 else 0
                last_date = None if rows_extracted == 0 else extraction_date.date()
                native_con.execute(
                    "INSERT INTO baliza_state.resource_health "
                    "(resource, consecutive_empty_days, last_nonempty_date) "
                    "VALUES (?, ?, ?)",
                    [resource, empty_days, last_date],
                )
            else:
                curr_empty, curr_last = state
                if rows_extracted == 0:
                    new_empty = curr_empty + 1
                    new_last = curr_last
                else:
                    new_empty = 0
                    new_last = extraction_date.date()

                # Update status based on thresholds
                status = "healthy"
                if new_empty >= 7:
                    status = "stalled"
                elif new_empty >= 3:
                    status = "warning"

                native_con.execute(
                    """UPDATE baliza_state.resource_health 
                       SET consecutive_empty_days = ?, last_nonempty_date = ?,
                           status = ?, updated_at = NOW()
                       WHERE resource = ?""",
                    [new_empty, new_last, status, resource],
                )
        except Exception as e:
            logger.error("health_update_failed", resource=resource, error=str(e))

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        self.close()

    def close(self):
        self.client.close()
