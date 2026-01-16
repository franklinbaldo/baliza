from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import duckdb

try:  # pragma: no cover - optional dependency
    import httpx
except ModuleNotFoundError:  # pragma: no cover - fallback path
    httpx = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from internetarchive import get_session  # type: ignore[import-untyped]
except ModuleNotFoundError:  # pragma: no cover - fallback path
    get_session = None  # type: ignore[assignment]


import typer
from rich import box

# Rich imports for improved CLI UX
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.progress_bar import ProgressBar
from rich.table import Table

from .pipelines.pncp import (
    BACKFILL_PIPELINE_NAME,
    DEFAULT_PIPELINE_NAME,
    _extract_total_paginas,
    default_config_path,
    load_pncp_config,
    run_pncp,
)
from .state import CoverageTracker, GapDetector, StateManager
from .utils import export_parquet
from .utils.dates import humanize_duration, humanize_naturaltime, to_pncp_window

console = Console()

app = typer.Typer(help="Declarative PNCP pipeline runner")

_GAP_ICONS = {
    "incomplete": "🚧",
    "suspect": "🚩",
    "missing": "📥",
    "unprocessed": "🆕",
    "lookback": "🔙",
}

_GAP_COLORS = {
    "incomplete": "yellow",
    "suspect": "red",
    "missing": "blue",
    "unprocessed": "cyan",
    "lookback": "magenta",
}


class _HttpClient(Protocol):
    def get(
        self, url: str, params: dict[str, Any] | None = None
    ) -> Any:  # pragma: no cover - protocol definition
        ...


HttpClientFactory = Callable[..., AbstractContextManager[_HttpClient]]


class _FallbackResponse:
    def __init__(self, *, status_code: int, text: str) -> None:
        self.status_code = status_code
        self._text = text

    def json(self) -> Any:
        if not self._text:
            return {}
        return json.loads(self._text)

    def raise_for_status(self) -> None:
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP request failed with status {self.status_code}")


class _FallbackClient(AbstractContextManager["_FallbackClient"]):
    def __init__(self, *, headers: dict[str, str] | None = None, timeout: int = 30) -> None:
        self.headers = headers or {}
        self.timeout = timeout

    def __enter__(self) -> _FallbackClient:
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # pragma: no cover - no cleanup needed
        return None

    def get(self, url: str, params: dict[str, Any] | None = None) -> _FallbackResponse:
        from urllib import parse, request

        class NoRedirectHandler(request.HTTPRedirectHandler):
            def http_error_302(self, req, fp, code, msg, headers):
                return fp

            http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302

        if not url.startswith(("http://", "https://")):
            raise ValueError("URL scheme must be http or https")

        query = parse.urlencode(params or {}, doseq=True)
        full_url = f"{url}?{query}" if query else url
        req = request.Request(full_url, headers=self.headers)

        opener = request.build_opener(NoRedirectHandler())
        try:
            with opener.open(req, timeout=self.timeout) as response:
                status = int(response.getcode() or 0)

                # Security: Limit response size to prevent DoS via memory exhaustion
                content = bytearray()
                chunk_size = 8192  # 8KB chunks
                max_size = 10 * 1024 * 1024  # 10 MB limit

                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    content.extend(chunk)
                    if len(content) > max_size:
                        raise RuntimeError(f"Response too large (exceeded {max_size} bytes)")

                text = content.decode("utf-8")
        except Exception as exc:  # pragma: no cover - network not exercised in tests
            # Security: Redact query parameters in error logs to prevent secret leakage
            safe_url = parse.urlparse(full_url)._replace(query="").geturl()
            raise RuntimeError(f"HTTP request to {safe_url} failed: {exc}") from exc
        return _FallbackResponse(status_code=status, text=text)


if httpx is not None:

    class _SecureClient(httpx.Client):
        def send(
            self, request: httpx.Request, *, stream: bool = False, **kwargs: Any
        ) -> httpx.Response:
            if request.url.scheme not in ("http", "https"):
                raise ValueError("URL scheme must be http or https")

            try:
                # Force stream=True to inspect/limit content
                response = super().send(request, stream=True, **kwargs)
            except httpx.RequestError as exc:
                safe_url = str(request.url.copy_with(query=None))
                raise RuntimeError(f"HTTP request to {safe_url} failed: {exc}") from exc

            max_size = 10 * 1024 * 1024  # 10 MB

            def _check_size(chunk: bytes, total: int) -> int:
                total += len(chunk)
                if total > max_size:
                    raise RuntimeError(f"Response too large (exceeded {max_size} bytes)")
                return total

            if stream:
                original_iter = response.iter_bytes()

                def limited_iter() -> Iterable[bytes]:
                    total = 0
                    for chunk in original_iter:
                        total = _check_size(chunk, total)
                        yield chunk

                response.stream = limited_iter()
            else:
                try:
                    content = bytearray()
                    total = 0
                    for chunk in response.iter_bytes():
                        total = _check_size(chunk, total)
                        content.extend(chunk)

                    # Manually populate response content
                    response._content = bytes(content)
                    # We must close the underlying stream since we've consumed it
                    # and won't be using the response as a stream.
                    response.close()
                except Exception as exc:
                    response.close()
                    safe_url = str(request.url.copy_with(query=None))
                    raise RuntimeError(f"HTTP request to {safe_url} failed: {exc}") from exc

            return response


def _default_http_client_factory(
    *, headers: dict[str, str] | None = None, timeout: int = 30
) -> AbstractContextManager[_HttpClient]:
    if httpx is not None:
        return _SecureClient(headers=headers or None, timeout=timeout)
    return _FallbackClient(headers=headers or None, timeout=timeout)


_HTTP_CLIENT_FACTORY: HttpClientFactory = _default_http_client_factory


def set_http_client_factory(factory: HttpClientFactory) -> None:
    global _HTTP_CLIENT_FACTORY
    _HTTP_CLIENT_FACTORY = factory


def reset_http_client_factory() -> None:
    set_http_client_factory(_default_http_client_factory)


def _resolve_config_path(config: Path | None) -> Path:
    if config is None:
        return default_config_path()
    return config


def _month_windows(start_month: str, end_month: str) -> Iterable[tuple[datetime, datetime]]:
    """Generate inclusive month windows between two YYYY-MM strings."""

    try:
        start = datetime.strptime(start_month, "%Y-%m").replace(tzinfo=UTC, day=1)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter("start_month must follow YYYY-MM format") from exc

    try:
        end = datetime.strptime(end_month, "%Y-%m").replace(tzinfo=UTC, day=1)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter("end_month must follow YYYY-MM format") from exc

    if start > end:
        raise typer.BadParameter("start_month must be before or equal to end_month")

    current = start
    while current <= end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        yield current, next_month
        current = next_month


def _parse_day(value: str | None, param_name: str) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=None)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter(f"{param_name} must follow YYYY-MM-DD format") from exc


def _pncp_date_param(value: datetime | None) -> str | None:
    if value is None:
        return None
    return to_pncp_window(value)


def _filter_dict_none(values: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


def _parse_optional_date(value: str | None, *, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter(f"{option_name} must follow YYYY-MM-DD format") from exc


@app.command("extract")
def extract(
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the declarative source configuration",
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to the DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Destination dataset name inside DuckDB",
    ),
    lookback_days: int = typer.Option(
        3,
        "--lookback-days",
        "-l",
        min=0,
        help="Number of days to look back from last successful run (for incremental extraction).",
    ),
    start_date: str | None = typer.Option(
        None,
        "--start-date",
        help="Start date for extraction range (YYYY-MM-DD). If not specified, uses intelligent gap detection.",
    ),
    end_date: str | None = typer.Option(
        None,
        "--end-date",
        help="End date for extraction range (YYYY-MM-DD). If not specified, uses today.",
    ),
    resource: str = typer.Option(
        "contratos",
        "--resource",
        "-r",
        help="Resource name to extract (must match configuration)",
    ),
    auto_resume: bool = typer.Option(
        True,
        "--auto-resume/--no-resume",
        help="Automatically resume from incomplete windows",
    ),
    merge_windows: bool = typer.Option(
        True,
        "--merge-windows/--no-merge",
        help="Merge adjacent windows to reduce API calls",
    ),
    max_merge_days: int = typer.Option(
        7,
        "--max-merge-days",
        min=1,
        help="Maximum number of days to merge into a single window",
    ),
) -> None:
    """
    Run the PNCP extraction pipeline with intelligent gap detection and resumability.

    This command automatically identifies which time windows need extraction by:
    1. Checking for incomplete windows from previous failed runs (highest priority)
    2. Looking for suspect windows with data quality issues
    3. Finding never-processed windows
    4. Re-extracting recent data within the lookback period

    The extraction is fully resumable - if interrupted, re-run the same command
    and it will continue from where it left off.
    """
    config_path = _resolve_config_path(config)

    # Initialize state management
    state_manager = StateManager(duckdb, dataset=dataset)
    gap_detector = GapDetector(state_manager)

    try:
        # Determine date range
        if start_date:
            start_dt = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
        else:
            # Use intelligent default: last successful run or 30 days ago
            last_run = state_manager.get_last_successful_run(resource)
            if last_run and last_run.completed_at:
                # Start from completion time of last run
                start_dt = last_run.completed_at.replace(tzinfo=UTC)
            else:
                # First run - go back 30 days
                start_dt = datetime.now(UTC) - timedelta(days=30)

        end_dt = (
            datetime.fromisoformat(end_date).replace(tzinfo=UTC) if end_date else datetime.now(UTC)
        )

        # Analyze coverage
        typer.echo(
            f"Analyzing coverage from {start_dt.date()} to {end_dt.date()} "
            f"(lookback: {lookback_days} days)..."
        )

        gaps = gap_detector.find_gaps(
            resource=resource,
            start_date=start_dt,
            end_date=end_dt,
            lookback_days=lookback_days,
            include_suspect=True,
        )

        if not gaps:
            typer.secho("✓ No gaps found. All windows are up to date.", fg=typer.colors.GREEN)
            return

        # Check for incomplete runs
        incomplete = [g for g in gaps if g.reason == "incomplete"]
        if incomplete and auto_resume:
            typer.echo(
                f"Found {len(incomplete)} incomplete window(s) from previous run. Resuming..."
            )

        # Optionally merge adjacent windows
        if merge_windows:
            original_count = len(gaps)
            gaps = gap_detector.merge_adjacent_windows(gaps, max_merge_days=max_merge_days)
            if len(gaps) < original_count:
                typer.echo(f"Merged {original_count} windows into {len(gaps)} to reduce API calls.")

        # Show summary
        gap_detector.count_gaps_by_reason(gaps)
        typer.echo(f"\nProcessing {len(gaps)} window(s):")

        table = Table(title="Extraction Plan", box=box.ROUNDED)
        table.add_column("Order", justify="right", style="dim")
        table.add_column("Start Date", style="cyan")
        table.add_column("End Date", style="cyan")
        table.add_column("Reason", style="bold")

        limit_rows = 10
        for i, gap in enumerate(gaps[:limit_rows], 1):
            color_tag = _GAP_COLORS.get(gap.reason, "white")
            icon = _GAP_ICONS.get(gap.reason, "")

            table.add_row(
                str(i),
                str(gap.start.date()),
                str(gap.end.date()),
                f"[{color_tag}]{icon} {gap.reason}[/{color_tag}]",
            )

        console.print(table)

        if len(gaps) > limit_rows:
            typer.echo(f"... and {len(gaps) - limit_rows} more windows")

        # Start extraction run
        run_id = state_manager.start_run(resource)
        typer.echo(f"\nStarted extraction run: {run_id}\n")

        results = []
        windows_completed = 0

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                transient=False,
            ) as progress:
                task_id = progress.add_task(
                    description=f"Processing {len(gaps)} windows...", total=len(gaps)
                )

                for i, window in enumerate(gaps, 1):
                    progress.update(
                        task_id,
                        description=f"Processing {window.start.date()} to {window.end.date()} ({window.reason})",
                    )

                    # Run extraction for this window
                    _, run_info = run_pncp(
                        config_path=config_path,
                        dataset=dataset,
                        duckdb_path=duckdb,
                        lookback_days=0,  # Don't apply lookback per-window
                        range_start=window.start,
                        range_end=window.end,
                        pipeline_name=DEFAULT_PIPELINE_NAME,
                    )

                    windows_completed += 1

                    # Update run progress
                    state_manager.update_run_progress(
                        run_id,
                        windows_attempted=i,
                        windows_completed=windows_completed,
                    )

                    results.append(
                        {
                            "window": f"{window.start.date()} to {window.end.date()}",
                            "reason": window.reason,
                            "status": "completed",
                        }
                    )

                    # Mark window as complete
                    periodo = state_manager.tracker.period_label(window.start, window.end)
                    state_manager.tracker.mark_window_status(resource, periodo, "ok")

                    progress.console.print(
                        f"  [green]✓[/green] Completed {window.start.date()} to {window.end.date()}"
                    )

                    progress.advance(task_id)

            # Complete the run
            state_manager.complete_run(run_id, {"windows_completed": windows_completed})

            typer.secho("\n✓ Extraction completed successfully!", fg=typer.colors.GREEN)
            typer.echo(json.dumps({"run_id": run_id, "windows": results}, indent=2, default=str))

        except Exception as e:
            # Fail the run but preserve state
            state_manager.fail_run(run_id, str(e))
            typer.secho(f"\n✗ Extraction failed: {e}", fg=typer.colors.RED, err=True)
            typer.echo(f"\nRun ID: {run_id} (marked as failed)")
            typer.echo(f"Completed {windows_completed}/{len(gaps)} windows before failure.")
            typer.echo("Incomplete windows will be resumed on next run.")
            raise typer.Exit(code=1)

    finally:
        state_manager.close()


@app.command("backfill")
def backfill(
    start_month: str,
    end_month: str,
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the declarative source configuration",
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to the DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Destination dataset name inside DuckDB",
    ),
) -> None:
    """Run stateless monthly backfills between two months (inclusive)."""

    config_path = _resolve_config_path(config)
    windows = list(_month_windows(start_month, end_month))
    results = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        transient=False,
        console=Console(stderr=True),
    ) as progress:
        task_id = progress.add_task(
            description=f"Backfilling {len(windows)} months...", total=len(windows)
        )

        for window_start, window_end in windows:
            progress.update(task_id, description=f"Backfilling {window_start.strftime('%Y-%m')}...")

            _, run_info = run_pncp(
                config_path=config_path,
                dataset=dataset,
                duckdb_path=duckdb,
                lookback_days=0,
                range_start=window_start,
                range_end=window_end,
                pipeline_name=BACKFILL_PIPELINE_NAME,
            )
            results.append(
                {
                    "start": window_start.isoformat(),
                    "end": window_end.isoformat(),
                    "run": run_info.asdict(),
                }
            )
            progress.advance(task_id)

    typer.echo(json.dumps({"windows": results}, indent=2, default=str))


@app.command("verify")
def verify(
    resource: str = typer.Option(
        ..., "--resource", "-r", help="Resource name declared in the PNCP configuration"
    ),
    desde: str | None = typer.Option(
        None,
        "--desde",
        help="Optional start date filter (YYYY-MM-DD) for window evaluation",
    ),
    ate: str | None = typer.Option(
        None,
        "--ate",
        help="Optional end date filter (YYYY-MM-DD, inclusive) for window evaluation",
    ),
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to the declarative source configuration",
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to the DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Destination dataset name inside DuckDB",
    ),
    sequencia: bool = typer.Option(
        False,
        "--sequencia",
        help="Enable sequence and hash audits during verification",
    ),
) -> None:
    """Inspect the coverage tracker and detect missing or suspect windows."""

    inicio_filtro = _parse_day(desde, "--desde")
    fim_filtro = _parse_day(ate, "--ate")
    fim_exclusivo = fim_filtro + timedelta(days=1) if fim_filtro else None

    config_path = _resolve_config_path(config)
    tracker = CoverageTracker(duckdb, dataset=dataset)
    try:
        config_data = load_pncp_config(config_path)
        resources_cfg = config_data.get("resources", [])
        resource_cfg = next(
            (r for r in resources_cfg if isinstance(r, dict) and r.get("name") == resource),
            None,
        )
        if resource_cfg is None:
            raise typer.BadParameter(f"resource '{resource}' not found in configuration")

        endpoint_cfg = resource_cfg.get("endpoint", {}) or {}
        table_name = resource_cfg.get("table_name") or resource
        client_cfg = config_data.get("client", {}) or {}

        headers_cfg = client_cfg.get("headers")
        if not isinstance(headers_cfg, dict):
            headers_cfg = {}
        headers = _filter_dict_none(headers_cfg or {})
        timeout = client_cfg.get("timeout", 30)
        base_url = client_cfg.get("base_url", "") or ""
        endpoint_path = endpoint_cfg.get("path", "")
        params_cfg = endpoint_cfg.get("params")
        if not isinstance(params_cfg, dict):
            params_cfg = {}
        default_params = _filter_dict_none(dict(params_cfg))

        coverage = tracker.fetch_pages_by_window(resource, start=inicio_filtro, end=fim_exclusivo)
        coverage_keys = set(coverage.keys())
        status_map = {row["periodo"]: row for row in tracker.fetch_window_statuses(resource)}
        pending_pages: dict[str, list[int]] = {}
        hash_alerts: list[dict[str, Any]] = []

        with _HTTP_CLIENT_FACTORY(headers=headers or None, timeout=timeout) as client:
            if endpoint_path.startswith("http"):
                endpoint_url = endpoint_path
            else:
                endpoint_url = f"{base_url.rstrip('/')}{endpoint_path}"

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=Console(stderr=True),
                transient=True,
            ) as progress:
                task_id = progress.add_task(
                    description=f"Verifying {len(coverage)} windows...",
                    total=len(coverage),
                )

                for periodo, entry in coverage.items():
                    progress.update(task_id, description=f"Verifying {periodo}...")
                    params = dict(default_params)
                    params["pagina"] = 1
                    inicio = entry.get("janela_inicio")
                    fim = entry.get("janela_fim")
                    if inicio:
                        params["dataInicial"] = _pncp_date_param(inicio)
                    if fim:
                        params["dataFinal"] = _pncp_date_param(fim)
                    params = _filter_dict_none(params)

                    response = client.get(endpoint_url, params=params)
                    if response.status_code == 204:
                        payload: dict[str, Any] = {"data": [], "totalPaginas": 0}
                    else:
                        response.raise_for_status()
                        payload = response.json()
                    recorded_pages = set(entry["recorded_pages"])
                    fallback_total = entry["max_total"] or (
                        max(recorded_pages) if recorded_pages else 0
                    )
                    atual_total = _extract_total_paginas(payload, fallback_total)
                    atual_total = max(atual_total, entry["max_total"], fallback_total)
                    faltantes = [
                        pagina
                        for pagina in range(1, (atual_total or 0) + 1)
                        if pagina not in recorded_pages
                    ]
                    if faltantes:
                        pending_pages[periodo] = faltantes
                    existing_status = status_map.get(periodo)

                    if faltantes:
                        motivo = f"paginas faltantes: {faltantes}"
                        tracker.mark_window_status(resource, periodo, "incompleto", motivo)
                    elif (
                        existing_status
                        and existing_status.get("status") == "suspeito"
                        and not sequencia
                    ):
                        # Preserve previous suspicion when sequence audits are disabled
                        pass
                    else:
                        tracker.mark_window_status(resource, periodo, "ok")

                    if sequencia and atual_total > 0:
                        sample_page = min(atual_total, max(1, atual_total // 2))
                        stored_page = entry["pages"].get(sample_page)
                        if stored_page and stored_page.hash_ids:
                            sample_params = dict(params)
                            sample_params["pagina"] = sample_page
                            sample_response = client.get(endpoint_url, params=sample_params)
                            if sample_response.status_code == 204:
                                sample_payload: dict[str, Any] = {"data": []}
                            else:
                                sample_response.raise_for_status()
                                sample_payload = sample_response.json()
                            registros = (
                                sample_payload.get("data")
                                or sample_payload.get("items")
                                or sample_payload.get("results")
                                or []
                            )
                            try:
                                try:
                                    algoritmo, stored_digest = CoverageTracker.parse_hash_value(
                                        stored_page.hash_ids
                                    )
                                except ValueError:
                                    algoritmo = None
                                    stored_digest = stored_page.hash_ids
                                sample_digest = tracker.hash_registros(
                                    registros,
                                    algorithm=algoritmo,
                                    include_algorithm=algoritmo is None,
                                )
                            except RuntimeError as exc:
                                typer.echo(str(exc), err=True)
                                raise typer.Exit(code=1) from exc
                            if sample_digest and sample_digest != stored_digest:
                                motivo = f"hash divergente na pagina {sample_page}"
                                tracker.mark_window_status(resource, periodo, "suspeito", motivo)
                                hash_alerts.append({"periodo": periodo, "pagina": sample_page})

                    progress.advance(task_id)

        candidatos = tracker.derive_window_candidates(
            table_name,
            start=inicio_filtro,
            end=fim_exclusivo,
        )
        for inicio, fim in candidatos:
            periodo = tracker.period_label(inicio, fim)
            if periodo not in coverage_keys:
                tracker.mark_window_status(
                    resource,
                    periodo,
                    "nao_processado",
                    "janela ausente no manifesto",
                )
                pending_pages.setdefault(periodo, [])

        status_rows = tracker.fetch_window_statuses(resource)
        lacunas = [
            row for row in status_rows if row.get("status") in {"incompleto", "nao_processado"}
        ]
        suspeitas = [row for row in status_rows if row.get("status") == "suspeito"]
        resumo = tracker.summarize_windows(
            resource,
            table_name,
            start=inicio_filtro,
            end=fim_exclusivo,
        )

        resultado = {
            "resource": resource,
            "windows": resumo["windows"],
            "lacunas": lacunas,
            "paginas_pendentes": pending_pages,
            "suspeitas": suspeitas,
        }
        if resumo["missing"]:
            resultado["janelas_nao_manifestadas"] = resumo["missing"]
        if hash_alerts:
            resultado["hash_alertas"] = hash_alerts

        typer.echo(json.dumps(resultado, indent=2, default=str))

    finally:
        tracker.close()


@app.command("export")
def export(
    duckdb_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to the DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset (schema) inside DuckDB to read from",
    ),
    table: str = typer.Option(
        ...,
        "--table",
        "-t",
        help="Table name inside the dataset to export",
    ),
    out_dir: Path = typer.Option(
        Path("data"),
        "--out",
        "-o",
        help="Directory where partitioned Parquet files will be written",
    ),
    date_col: str = typer.Option(
        "dataAtualizacao",
        "--date-col",
        help="Primary date column used for filtering and partitioning",
    ),
    fallback_date_col: list[str] = typer.Option(
        [],
        "--fallback-date-col",
        help="Additional candidate date columns if --date-col is missing",
    ),
    start_date: str | None = typer.Option(
        None,
        "--start-date",
        help="Lower bound (inclusive) for the date filter (YYYY-MM-DD)",
    ),
    end_date: str | None = typer.Option(
        None,
        "--end-date",
        help="Upper bound (inclusive) for the date filter (YYYY-MM-DD)",
    ),
    ia_identifier: str | None = typer.Option(
        None,
        "--ia-identifier",
        help="Identifier for the Internet Archive upload (e.g., baliza-contratos-2024-07)",
    ),
    ia_access_key: str | None = typer.Option(
        None,
        "--ia-access-key",
        envvar="IA_ACCESS_KEY",
        help="Internet Archive access key (or IA_ACCESS_KEY env var)",
    ),
    ia_secret_key: str | None = typer.Option(
        None,
        "--ia-secret-key",
        envvar="IA_SECRET_KEY",
        help="Internet Archive secret key (or IA_SECRET_KEY env var)",
    ),
    ia_metadata_path: Path | None = typer.Option(
        Path("internet-archive-summary.json"),
        "--ia-metadata-path",
        help="Path to save Internet Archive upload metadata",
    ),
) -> None:
    """Export a DuckDB table to partitioned Parquet files."""

    start = _parse_optional_date(start_date, option_name="--start-date")
    finish = _parse_optional_date(end_date, option_name="--end-date")
    if start and finish and start > finish:
        raise typer.BadParameter("--start-date must be before or equal to --end-date")

    fallback_candidates = fallback_date_col or ["dataAtualizacao"]

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=Console(stderr=True),
            transient=True,
        ) as progress:
            progress.add_task(description=f"Exporting table '{table}' to Parquet...", total=None)
            metadata = export_parquet(
                duckdb_path=duckdb_path,
                dataset=dataset,
                table=table,
                out_dir=out_dir,
                date_col=date_col,
                fallback_date_cols=fallback_candidates,
                start_date=start.isoformat() if start else None,
                end_date=finish.isoformat() if finish else None,
            )
    except (duckdb.Error, ValueError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    typer.echo(json.dumps(metadata.asdict(), indent=2, default=str))

    if ia_identifier and (not ia_access_key or not ia_secret_key):
        typer.secho(
            "Internet Archive access and secret keys are required when providing --ia-identifier.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    if ia_identifier:
        # Security validation to prevent path traversal
        import re

        if not re.match(r"^[a-zA-Z0-9_\-]+$", ia_identifier):
            typer.secho(
                "Invalid value for --ia-identifier. Only alphanumeric characters, dashes, and underscores are allowed.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=1)

    if ia_identifier and ia_access_key and ia_secret_key:
        if get_session is None:  # pragma: no cover - optional dependency guard
            typer.secho(
                "InternetArchive library not found. Cannot upload to IA.",
                fg=typer.colors.YELLOW,
                err=True,
            )
            typer.secho(
                "Install with: pip install 'baliza[internet-archive]'",
                fg=typer.colors.YELLOW,
                err=True,
            )
            return

        archive_dir: Path | None = None
        try:
            session = get_session(access_key=ia_access_key, secret_key=ia_secret_key)
            ia_metadata: dict[str, Any] = {
                "title": f"Baliza {metadata.table} Dataset - {ia_identifier}",
                "description": (
                    "Publicly available procurement data extracted by Baliza. "
                    "Source: PNCP API. "
                    f"Extracted from {metadata.start_date} to {metadata.end_date}."
                ),
                "mediatype": "collection",
                "creator": "Baliza Project",
                "subject": ["public procurement", "Brazil", "contracts", "PNCP"],
                "coverage": f"Brazil. Dates: {metadata.start_date} to {metadata.end_date}",
                "created": datetime.now().isoformat(),
            }
            if metadata.rows_exported is not None:
                ia_metadata["lineCount"] = metadata.rows_exported
            if metadata.partition_count is not None:
                ia_metadata["numberOfFiles"] = metadata.partition_count

            archive_dir = Path(f"ia_archive_{ia_identifier}")
            archive_dir.mkdir(parents=True, exist_ok=True)
            shutil.copytree(Path(metadata.output_dir), archive_dir, dirs_exist_ok=True)

            target_item = session.get_item(ia_identifier)

            # Use a spinner on stderr to show progress without polluting stdout
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=Console(stderr=True),
                transient=True,
            ) as progress:
                progress.add_task(
                    description=f"Uploading to Internet Archive (Identifier: {ia_identifier})...",
                    total=None,
                )
                # Use verbose=False to keep stdout clean.
                target_item.upload(str(archive_dir), metadata=ia_metadata, verbose=False)

            upload_metadata = {
                "identifier": ia_identifier,
                "upload_time": datetime.now().isoformat(),
                "ia_metadata": ia_metadata,
                "local_output_dir": metadata.output_dir,
                "local_archive_dir": str(archive_dir),
            }
            metadata_path = ia_metadata_path or Path("internet-archive-summary.json")
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            with metadata_path.open("w", encoding="utf-8") as fh:
                json.dump(upload_metadata, fh, indent=2)
            typer.echo(f"Internet Archive upload metadata saved to: {metadata_path}", err=True)

            typer.echo("Successfully uploaded to Internet Archive.", err=True)
        except Exception as exc:  # pragma: no cover - network dependent
            typer.secho(
                f"Error during Internet Archive upload: {exc}",
                fg=typer.colors.RED,
                err=True,
            )
        finally:
            if archive_dir and archive_dir.exists():
                shutil.rmtree(archive_dir)
                typer.echo("Temporary archive directory cleaned.", err=True)


# State management command group
state_app = typer.Typer(help="Manage and inspect extraction state")
app.add_typer(state_app, name="state")


@state_app.command("show")
def state_show(
    resource: str = typer.Option(..., "--resource", "-r", help="Resource name"),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"), "--duckdb", "-d", help="Path to the DuckDB database file"
    ),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name inside DuckDB"),
) -> None:
    """Show extraction state summary for a resource."""
    manager = StateManager(duckdb, dataset=dataset)

    try:
        statuses = manager.tracker.fetch_window_statuses(resource)

        if not statuses:
            console.print(
                Panel(
                    f"No extraction state found for resource [bold cyan]'{resource}'[/bold cyan].\n\n"
                    "This usually means no extraction has been performed yet.\n"
                    f"To start a new extraction, run:\n[bold green]baliza extract --resource {resource}[/bold green]",
                    title="[yellow]No State Data Found[/yellow]",
                    border_style="yellow",
                    padding=(1, 2),
                )
            )
            return

        counts = {"ok": 0, "incompleto": 0, "suspeito": 0, "nao_processado": 0}
        for status_entry in statuses:
            status = status_entry.get("status", "unknown")
            counts[status] = counts.get(status, 0) + 1

        last_run = manager.get_last_successful_run(resource)

        table = Table(title=f"State Summary for '{resource}'", box=box.ROUNDED)
        table.add_column("Status", style="bold")
        table.add_column("Count", justify="right")
        table.add_column("Percentage", justify="right")
        table.add_column("Distribution", ratio=1)

        total = len(statuses)

        def fmt_pct(value: int) -> str:
            if total == 0:
                return "0.0%"
            return f"{(value / total) * 100:.1f}%"

        def get_bar(value: int, style: str) -> ProgressBar:
            return ProgressBar(
                total=max(total, 1),
                completed=value,
                width=None,
                style="dim",
                complete_style=style,
                finished_style=style,
            )

        table.add_row(
            "[green]Complete windows[/green]",
            str(counts["ok"]),
            fmt_pct(counts["ok"]),
            get_bar(counts["ok"], "green"),
        )
        table.add_row(
            "[yellow]Incomplete windows[/yellow]",
            str(counts["incompleto"]),
            fmt_pct(counts["incompleto"]),
            get_bar(counts["incompleto"], "yellow"),
        )
        table.add_row(
            "[red]Suspect windows[/red]",
            str(counts["suspeito"]),
            fmt_pct(counts["suspeito"]),
            get_bar(counts["suspeito"], "red"),
        )
        table.add_row(
            "[cyan]Unprocessed windows[/cyan]",
            str(counts["nao_processado"]),
            fmt_pct(counts["nao_processado"]),
            get_bar(counts["nao_processado"], "cyan"),
        )
        table.add_row("Total windows", str(total), "100.0%", "", style="bold")

        console.print(table)

        if last_run:
            duration_str = "-"
            if last_run.completed_at and last_run.started_at:
                duration_secs = (last_run.completed_at - last_run.started_at).total_seconds()
                duration_str = humanize_duration(duration_secs)

            grid = Table.grid(padding=(0, 2))
            grid.add_column(style="dim")
            grid.add_column(style="bold")

            grid.add_row("Completed:", humanize_naturaltime(last_run.completed_at))
            grid.add_row("Duration:", duration_str)
            grid.add_row("Windows:", str(last_run.windows_completed))
            grid.add_row("Rows:", f"{last_run.rows_extracted:,}")
            grid.add_row("Run ID:", last_run.run_id)

            console.print(
                Panel(
                    grid,
                    title="[green]Last Successful Run[/green]",
                    title_align="left",
                    border_style="green",
                    padding=(1, 2),
                )
            )

    finally:
        manager.close()


@state_app.command("gaps")
def state_gaps(
    resource: str = typer.Option(..., "--resource", "-r", help="Resource name"),
    start_date: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end_date: str | None = typer.Option(
        None, "--end", help="End date (YYYY-MM-DD), defaults to today"
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"), "--duckdb", "-d", help="Path to the DuckDB database file"
    ),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name inside DuckDB"),
    lookback_days: int = typer.Option(
        0, "--lookback-days", "-l", min=0, help="Lookback days to include"
    ),
    limit: int = typer.Option(
        20, "--limit", "-n", min=1, help="Maximum number of gaps to display"
    ),
) -> None:
    """List gaps in extraction coverage."""
    manager = StateManager(duckdb, dataset=dataset)
    detector = GapDetector(manager)

    try:
        start_dt = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
        end_dt = (
            datetime.fromisoformat(end_date).replace(tzinfo=UTC) if end_date else datetime.now(UTC)
        )

        gaps = detector.find_gaps(resource, start_dt, end_dt, lookback_days=lookback_days)

        if not gaps:
            typer.secho("✓ No gaps found!", fg=typer.colors.GREEN)
            return

        counts = detector.count_gaps_by_reason(gaps)
        typer.echo(f"Found {len(gaps)} gap(s):")
        for reason, count in counts.items():
            icon = _GAP_ICONS.get(reason, "")
            color = _GAP_COLORS.get(reason, "white")
            console.print(f"  • {count} {icon} [{color}]{reason}[/{color}]")

        typer.echo("\nDetails:")

        table = Table(title="Coverage Gaps", box=box.ROUNDED)
        table.add_column("Start Date", style="cyan")
        table.add_column("End Date", style="cyan")
        table.add_column("Reason", style="bold")

        for gap in gaps[:limit]:
            color_tag = _GAP_COLORS.get(gap.reason, "white")
            icon = _GAP_ICONS.get(gap.reason, "")

            table.add_row(
                str(gap.start.date()),
                str(gap.end.date()),
                f"[{color_tag}]{icon} {gap.reason}[/{color_tag}]",
            )

        console.print(table)

        if len(gaps) > limit:
            remaining = len(gaps) - limit
            console.print(
                f"... and {remaining} more gap(s) hidden. Use --limit to see more.",
                style="dim",
            )
    finally:
        manager.close()


@state_app.command("history")
def state_history(
    resource: str = typer.Option(..., "--resource", "-r", help="Resource name"),
    limit: int = typer.Option(10, "--limit", "-n", min=1, help="Number of runs to show"),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"), "--duckdb", "-d", help="Path to the DuckDB database file"
    ),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name inside DuckDB"),
) -> None:
    """Show extraction run history for a resource."""
    manager = StateManager(duckdb, dataset=dataset)

    try:
        history = manager.get_run_history(resource, limit=limit)

        if not history:
            console.print(
                Panel(
                    f"No run history found for resource [bold cyan]'{resource}'[/bold cyan].\n\n"
                    f"To start a new extraction, run:\n[bold green]baliza extract --resource {resource}[/bold green]",
                    title="[yellow]No History Found[/yellow]",
                    border_style="yellow",
                    padding=(1, 2),
                )
            )
            return

        table = Table(
            title=f"Run history for '{resource}' (last {len(history)} runs)", box=box.ROUNDED
        )

        table.add_column("Run ID", style="cyan", no_wrap=True)
        table.add_column("Status", style="bold")
        table.add_column("Started", style="dim")
        table.add_column("Duration")
        table.add_column("Windows", justify="right")
        table.add_column("Rows", justify="right")
        table.add_column("Error", style="red")

        for run in history:
            status_style = {
                "completed": "green",
                "failed": "red",
                "running": "yellow",
            }.get(run.status, "white")

            duration = "-"
            if run.completed_at:
                duration_secs = (run.completed_at - run.started_at).total_seconds()
                duration = humanize_duration(duration_secs)
            elif run.status == "running":
                duration_secs = (datetime.now(UTC) - run.started_at).total_seconds()
                duration = f"[yellow]{humanize_duration(duration_secs)}[/yellow]"

            table.add_row(
                run.run_id,
                f"[{status_style}]{run.status}[/{status_style}]",
                humanize_naturaltime(run.started_at),
                duration,
                str(run.windows_completed),
                f"{run.rows_extracted:,}",
                run.error_message or "-",
            )

        console.print(table)
    finally:
        manager.close()


if __name__ == "__main__":
    app()
