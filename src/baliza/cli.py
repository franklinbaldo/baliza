from __future__ import annotations

import json

from datetime import date, datetime, timedelta, timezone

from pathlib import Path
from contextlib import AbstractContextManager
from typing import Any, Callable, Dict, Iterable, Optional, Protocol, Tuple

import duckdb
import shutil

try:  # pragma: no cover - optional dependency
    import httpx
except ModuleNotFoundError:  # pragma: no cover - fallback path
    httpx = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from internetarchive import get_session  # type: ignore[import-untyped]
except ModuleNotFoundError:  # pragma: no cover - fallback path
    get_session = None  # type: ignore[assignment]


import typer

from .pipelines.pncp import (
    BACKFILL_PIPELINE_NAME,
    DEFAULT_PIPELINE_NAME,
    _extract_total_paginas,
    default_config_path,
    load_pncp_config,
    run_pncp,
)

from .state import CoverageTracker


from .utils import export_parquet
from .utils.dates import to_pncp_window

app = typer.Typer(help="Declarative PNCP pipeline runner")


class _HttpClient(Protocol):
    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:  # pragma: no cover - protocol definition
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
    def __init__(self, *, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> None:
        self.headers = headers or {}
        self.timeout = timeout

    def __enter__(self) -> "_FallbackClient":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # pragma: no cover - no cleanup needed
        return None

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> _FallbackResponse:
        from urllib import parse, request

        query = parse.urlencode(params or {}, doseq=True)
        full_url = f"{url}?{query}" if query else url
        req = request.Request(full_url, headers=self.headers)
        try:
            with request.urlopen(req, timeout=self.timeout) as response:  # type: ignore[attr-defined]
                status = int(response.getcode() or 0)
                text = response.read().decode("utf-8")
        except Exception as exc:  # pragma: no cover - network not exercised in tests
            raise RuntimeError(f"HTTP request to {full_url} failed: {exc}") from exc
        return _FallbackResponse(status_code=status, text=text)


def _default_http_client_factory(
    *, headers: Optional[Dict[str, str]] = None, timeout: int = 30
) -> AbstractContextManager[_HttpClient]:
    if httpx is not None:
        return httpx.Client(headers=headers or None, timeout=timeout)
    return _FallbackClient(headers=headers or None, timeout=timeout)


_HTTP_CLIENT_FACTORY: HttpClientFactory = _default_http_client_factory


def set_http_client_factory(factory: HttpClientFactory) -> None:
    global _HTTP_CLIENT_FACTORY
    _HTTP_CLIENT_FACTORY = factory


def reset_http_client_factory() -> None:
    set_http_client_factory(_default_http_client_factory)


def _resolve_config_path(config: Optional[Path]) -> Path:
    if config is None:
        return default_config_path()
    return config


def _month_windows(
    start_month: str, end_month: str
) -> Iterable[Tuple[datetime, datetime]]:
    """Generate inclusive month windows between two YYYY-MM strings."""

    try:
        start = datetime.strptime(start_month, "%Y-%m").replace(
            tzinfo=timezone.utc, day=1
        )
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter("start_month must follow YYYY-MM format") from exc

    try:
        end = datetime.strptime(end_month, "%Y-%m").replace(tzinfo=timezone.utc, day=1)
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


def _parse_day(value: Optional[str], param_name: str) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=None)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter(f"{param_name} must follow YYYY-MM-DD format") from exc


def _pncp_date_param(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return to_pncp_window(value)


def _filter_dict_none(values: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


def _parse_optional_date(value: Optional[str], *, option_name: str) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - handled by Typer
        raise typer.BadParameter(
            f"{option_name} must follow YYYY-MM-DD format"
        ) from exc


@app.command("extract")
def extract(
    config: Optional[Path] = typer.Option(
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
        help="Number of days to subtract from the last cursor when building the request window.",
    ),
) -> None:
    """Run the PNCP extraction pipeline once using dlt."""
    config_path = _resolve_config_path(config)
    _, run_info = run_pncp(
        config_path=config_path,
        dataset=dataset,
        duckdb_path=duckdb,
        lookback_days=lookback_days,
        pipeline_name=DEFAULT_PIPELINE_NAME,
    )
    typer.echo(json.dumps(run_info.asdict(), indent=2, default=str))


@app.command("backfill")
def backfill(
    start_month: str,
    end_month: str,
    config: Optional[Path] = typer.Option(
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

    for window_start, window_end in windows:
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

    typer.echo(json.dumps({"windows": results}, indent=2, default=str))


@app.command("verify")
def verify(
    resource: str = typer.Option(
        ..., "--resource", "-r", help="Resource name declared in the PNCP configuration"
    ),
    desde: Optional[str] = typer.Option(
        None,
        "--desde",
        help="Optional start date filter (YYYY-MM-DD) for window evaluation",
    ),
    ate: Optional[str] = typer.Option(
        None,
        "--ate",
        help="Optional end date filter (YYYY-MM-DD, inclusive) for window evaluation",
    ),
    config: Optional[Path] = typer.Option(
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
            (
                r
                for r in resources_cfg
                if isinstance(r, dict) and r.get("name") == resource
            ),
            None,
        )
        if resource_cfg is None:
            raise typer.BadParameter(
                f"resource '{resource}' not found in configuration"
            )

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

        coverage = tracker.fetch_pages_by_window(
            resource, start=inicio_filtro, end=fim_exclusivo
        )
        coverage_keys = set(coverage.keys())
        status_map = {
            row["periodo"]: row for row in tracker.fetch_window_statuses(resource)
        }
        pending_pages: Dict[str, list[int]] = {}
        hash_alerts: list[Dict[str, Any]] = []

        with _HTTP_CLIENT_FACTORY(headers=headers or None, timeout=timeout) as client:
            if endpoint_path.startswith("http"):
                endpoint_url = endpoint_path
            else:
                endpoint_url = f"{base_url.rstrip('/')}{endpoint_path}"
            for periodo, entry in coverage.items():
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
                    payload: Dict[str, Any] = {"data": [], "totalPaginas": 0}
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
                else:
                    if (
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
                            sample_payload: Dict[str, Any] = {"data": []}
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
                            tracker.mark_window_status(
                                resource, periodo, "suspeito", motivo
                            )
                            hash_alerts.append(
                                {"periodo": periodo, "pagina": sample_page}
                            )

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
            row
            for row in status_rows
            if row.get("status") in {"incompleto", "nao_processado"}
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
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        help="Lower bound (inclusive) for the date filter (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Upper bound (inclusive) for the date filter (YYYY-MM-DD)",
    ),
    ia_identifier: Optional[str] = typer.Option(
        None,
        "--ia-identifier",
        help="Identifier for the Internet Archive upload (e.g., baliza-contratos-2024-07)",
    ),
    ia_access_key: Optional[str] = typer.Option(
        None,
        "--ia-access-key",
        envvar="IA_ACCESS_KEY",
        help="Internet Archive access key (or IA_ACCESS_KEY env var)",
    ),
    ia_secret_key: Optional[str] = typer.Option(
        None,
        "--ia-secret-key",
        envvar="IA_SECRET_KEY",
        help="Internet Archive secret key (or IA_SECRET_KEY env var)",
    ),
    ia_metadata_path: Optional[Path] = typer.Option(
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
        raise typer.Exit(code=1) from exc

    typer.echo(json.dumps(metadata.asdict(), indent=2, default=str))

    if ia_identifier and (not ia_access_key or not ia_secret_key):
        typer.secho(
            "Internet Archive access and secret keys are required when providing --ia-identifier.",
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
            )
            return

        typer.echo(
            f"\nUploading '{metadata.output_dir}/...' to Internet Archive (Identifier: {ia_identifier})..."
        )

        archive_dir: Optional[Path] = None
        try:
            session = get_session(access_key=ia_access_key, secret_key=ia_secret_key)
            ia_metadata: Dict[str, Any] = {
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
            target_item.upload(str(archive_dir), metadata=ia_metadata, verbose=True)

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
            typer.echo(f"Internet Archive upload metadata saved to: {metadata_path}")

            typer.echo("Successfully uploaded to Internet Archive.")
        except Exception as exc:  # pragma: no cover - network dependent
            typer.secho(
                f"Error during Internet Archive upload: {exc}",
                fg=typer.colors.RED,
                err=True,
            )
        finally:
            if archive_dir and archive_dir.exists():
                shutil.rmtree(archive_dir)
                typer.echo("Temporary archive directory cleaned.")


if __name__ == "__main__":
    app()
