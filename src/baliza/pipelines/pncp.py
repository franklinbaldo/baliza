from __future__ import annotations

from copy import deepcopy
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Optional, Union, cast

import dlt
import yaml  # type: ignore[import-untyped]
from dlt.extract.resource import DltResource
from dlt.pipeline import Pipeline
from dlt.sources import DltSource
from dlt.sources.rest_api import rest_api_source

from baliza.state import CoverageTracker

ConfigPath = Union[str, Path, None]

DEFAULT_PIPELINE_NAME = "baliza_pncp"
BACKFILL_PIPELINE_NAME = "baliza_pncp_backfill"
SECONDS_PER_DAY = 24 * 60 * 60
MAX_PAGE_SIZE = 500


def default_config_path() -> Path:
    """Return the packaged default declarative configuration path."""
    return Path(__file__).resolve().parent.parent / "config" / "pncp.yml"


def _import_from_string(dotted_path: str) -> Any:
    """Import a dotted path as a Python object."""
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Invalid import path '{dotted_path}'")
    module = import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as exc:  # pragma: no cover - defensive guard
        raise ValueError(
            f"Object '{attr}' not found in module '{module_path}'"
        ) from exc


def _resolve_callable_entries(node: Any) -> None:
    """Convert any declarative callable references into live callables."""
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if key == "convert" and isinstance(value, str):
                node[key] = _import_from_string(value)
            else:
                _resolve_callable_entries(value)
    elif isinstance(node, list):
        for item in node:
            _resolve_callable_entries(item)


def _resolve_config_path(config_path: ConfigPath) -> Path:
    """Resolve configuration path, falling back to the packaged default."""
    if config_path is None:
        return default_config_path()

    path = Path(config_path)
    if path.exists():
        return path

    if not path.is_absolute():
        candidate = default_config_path().parent / path
        if candidate.exists():
            return candidate

    raise FileNotFoundError(f"Configuration file not found: {config_path}")


def load_pncp_config(config_path: ConfigPath = None) -> Dict[str, Any]:
    """Load and post-process the PNCP REST configuration."""
    path = _resolve_config_path(config_path)
    with path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    _resolve_callable_entries(config)
    return config


def _apply_incremental_overrides(
    config: Dict[str, Any],
    *,
    lookback_days: Optional[int] = None,
    range_start: Any = None,
    range_end: Any = None,
) -> Dict[str, Any]:
    """Apply runtime overrides for incremental configuration."""

    adjusted = deepcopy(config)
    resources = adjusted.get("resources", [])
    for resource in resources:
        endpoint = resource.get("endpoint") if isinstance(resource, dict) else None
        incremental = (
            endpoint.get("incremental") if isinstance(endpoint, dict) else None
        )
        if not isinstance(incremental, dict):
            continue

        params = endpoint.get("params") if isinstance(endpoint, dict) else None
        if isinstance(params, dict):
            _clamp_page_params(params)

        if lookback_days is not None:
            if lookback_days < 0:
                raise ValueError("lookback_days must be zero or a positive integer")
            if lookback_days == 0:
                incremental.pop("lag", None)
            else:
                incremental["lag"] = lookback_days * SECONDS_PER_DAY

        if range_start is not None:
            incremental["initial_value"] = range_start

        if range_end is not None:
            incremental["end_value"] = range_end
        elif range_start is not None:
            incremental.pop("end_value", None)

    return adjusted


def _normalize_request_params(raw_params: Any) -> Dict[str, Any]:
    if isinstance(raw_params, dict):
        return dict(raw_params)
    if isinstance(raw_params, list):
        return {key: value for key, value in raw_params}
    return {}


def _extract_window_bounds(params: Dict[str, Any]) -> tuple[Any, Any]:
    start = (
        params.get("dataInicial")
        or params.get("dataInicial[]")
        or params.get("dataInicial1")
    )
    end = (
        params.get("dataFinal") or params.get("dataFinal[]") or params.get("dataFinal1")
    )
    return start, end


def _extract_total_paginas(payload: Dict[str, Any], fallback: int) -> int:
    for key in (
        "totalPaginas",
        "total_paginas",
        "totalPages",
        "total_paginas_observado",
    ):
        if payload.get(key) is not None:
            try:
                return int(payload[key])
            except (TypeError, ValueError):  # pragma: no cover - defensive guard
                continue
    return fallback


def _extract_pagina(payload: Dict[str, Any], params: Dict[str, Any]) -> int:
    for key in ("paginaAtual", "numeroPagina", "pagina", "page"):
        if payload.get(key) is not None:
            try:
                return int(payload[key])
            except (TypeError, ValueError):  # pragma: no cover - defensive guard
                continue
    try:
        return int(params.get("pagina", 1))
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return 1


def _attach_coverage_tracker(source: DltSource, tracker: CoverageTracker) -> None:
    for resource in source.resources:
        resource_obj = cast(DltResource, resource)

        if resource_obj._pipe.has_parent:
            continue

        original_gen = resource_obj._pipe.gen
        resource_name = resource_obj.name

        def _capture_pages(
            *args: Any,
            _orig_gen: Any = original_gen,
            _resource_name: str = resource_name,
            **kwargs: Any,
        ) -> Any:
            for page in _orig_gen(*args, **kwargs):
                if hasattr(page, "response") and hasattr(page, "request"):
                    try:
                        payload = page.response.json()  # type: ignore[attr-defined]
                    except Exception:  # pragma: no cover - defensive
                        payload = {}
                    params = _normalize_request_params(
                        getattr(page.request, "params", {})
                    )
                    start, end = _extract_window_bounds(params)
                    pagina = _extract_pagina(payload, params)
                    try:
                        page_length = len(page)
                    except TypeError:  # pragma: no cover - defensive guard
                        page_length = 0
                    total_paginas = _extract_total_paginas(payload, page_length)
                    tracker.record_page(
                        _resource_name,
                        start,
                        end,
                        pagina,
                        total_paginas,
                        page,
                    )
                yield page

        resource_obj._pipe.replace_gen(_capture_pages)


def pncp_source(
    config_path: ConfigPath = None,
    *,
    lookback_days: Optional[int] = None,
    range_start: Any = None,
    range_end: Any = None,
    tracker: CoverageTracker | None = None,
) -> DltSource:
    """Create the PNCP dlt source from declarative configuration."""
    config = load_pncp_config(config_path)
    adjusted = _apply_incremental_overrides(
        config,
        lookback_days=lookback_days,
        range_start=range_start,
        range_end=range_end,
    )
    source = rest_api_source(config=cast(Any, adjusted))
    if tracker is not None:
        _attach_coverage_tracker(source, tracker)
    return source


def run_pncp(
    config_path: ConfigPath = None,
    dataset: str = "baliza_raw",
    duckdb_path: Union[str, Path] = "baliza.duckdb",
    *,
    lookback_days: Optional[int] = None,
    range_start: Any = None,
    range_end: Any = None,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
) -> tuple[Pipeline, Any]:
    """Execute the PNCP pipeline using the DuckDB destination."""
    tracker = CoverageTracker(duckdb_path, dataset=dataset)
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(duckdb_path)),
        dataset_name=dataset,
    )
    source = pncp_source(
        config_path,
        lookback_days=lookback_days,
        range_start=range_start,
        range_end=range_end,
        tracker=tracker,
    )
    try:
        run_info = pipeline.run(source)
    finally:
        tracker.close()
    return pipeline, run_info


def _clamp_page_params(params: Dict[str, Any], *, limit: int = MAX_PAGE_SIZE) -> None:
    if not isinstance(params, dict):
        return

    if params.get("pagina") is not None:
        try:
            pagina = int(params["pagina"])
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            pagina = 1
        params["pagina"] = max(1, pagina)

    page_size = params.get("tamanhoPagina")
    if page_size is None:
        return

    try:
        size_int = int(page_size)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return

    clamped = max(1, min(size_int, limit))
    params["tamanhoPagina"] = clamped
