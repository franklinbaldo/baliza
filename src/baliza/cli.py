from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import httpx
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

app = typer.Typer(help="Declarative PNCP pipeline runner")


def _resolve_config_path(config: Optional[Path]) -> Path:
    if config is None:
        return default_config_path()
    return config


def _month_windows(start_month: str, end_month: str) -> Iterable[Tuple[datetime, datetime]]:
    """Generate inclusive month windows between two YYYY-MM strings."""

    try:
        start = datetime.strptime(start_month, "%Y-%m").replace(tzinfo=timezone.utc, day=1)
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


def _isoformat_utc(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _filter_dict_none(values: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in values.items() if v is not None}


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
        pending_pages: Dict[str, list[int]] = {}
        hash_alerts: list[Dict[str, Any]] = []

        with httpx.Client(headers=headers or None, timeout=timeout) as client:
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
                    params["dataInicial"] = _isoformat_utc(inicio)
                if fim:
                    params["dataFinal"] = _isoformat_utc(fim)
                params = _filter_dict_none(params)

                response = client.get(endpoint_url, params=params)
                response.raise_for_status()
                payload = response.json()
                recorded_pages = set(entry["recorded_pages"])
                fallback_total = entry["max_total"] or (max(recorded_pages) if recorded_pages else 0)
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
                    if existing_status and existing_status.get("status") == "suspeito" and not sequencia:
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
                        sample_response.raise_for_status()
                        sample_payload = sample_response.json()
                        registros = (
                            sample_payload.get("data")
                            or sample_payload.get("items")
                            or sample_payload.get("results")
                            or []
                        )
                        sample_hash = tracker.hash_registros(registros)
                        if sample_hash and sample_hash != stored_page.hash_ids:
                            motivo = f"hash divergente na pagina {sample_page}"
                            tracker.mark_window_status(resource, periodo, "suspeito", motivo)
                            hash_alerts.append({"periodo": periodo, "pagina": sample_page})

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


if __name__ == "__main__":
    app()
