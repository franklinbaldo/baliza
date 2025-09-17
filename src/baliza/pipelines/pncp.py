from __future__ import annotations

from copy import deepcopy
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, Optional, Union

import dlt
import yaml
from dlt.pipeline import Pipeline
from dlt.sources import DltSource
from dlt.sources.rest_api import rest_api_source

ConfigPath = Union[str, Path, None]

DEFAULT_PIPELINE_NAME = "baliza_pncp"
BACKFILL_PIPELINE_NAME = "baliza_pncp_backfill"
SECONDS_PER_DAY = 24 * 60 * 60


def default_config_path() -> Path:
    """Return the packaged default declarative configuration path."""
    return Path(__file__).resolve().parent.parent / "config" / "pncp.yml"


def _import_from_string(dotted_path: str) -> Any:
    """Import a dotted path as a Python object."""
    module_path, _, attr = dotted_path.rpartition('.')
    if not module_path:
        raise ValueError(f"Invalid import path '{dotted_path}'")
    module = import_module(module_path)
    try:
        return getattr(module, attr)
    except AttributeError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Object '{attr}' not found in module '{module_path}'") from exc


def _resolve_callable_entries(node: Any) -> None:
    """Convert any declarative callable references into live callables."""
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if key == 'convert' and isinstance(value, str):
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
    with path.open('r', encoding='utf-8') as fh:
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
        incremental = endpoint.get("incremental") if isinstance(endpoint, dict) else None
        if not isinstance(incremental, dict):
            continue

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


def pncp_source(
    config_path: ConfigPath = None,
    *,
    lookback_days: Optional[int] = None,
    range_start: Any = None,
    range_end: Any = None,
) -> DltSource:
    """Create the PNCP dlt source from declarative configuration."""
    config = load_pncp_config(config_path)
    adjusted = _apply_incremental_overrides(
        config,
        lookback_days=lookback_days,
        range_start=range_start,
        range_end=range_end,
    )
    return rest_api_source(config=adjusted)


def run_pncp(
    config_path: ConfigPath = None,
    dataset: str = 'baliza_raw',
    duckdb_path: Union[str, Path] = 'baliza.duckdb',
    *,
    lookback_days: Optional[int] = None,
    range_start: Any = None,
    range_end: Any = None,
    pipeline_name: str = DEFAULT_PIPELINE_NAME,
) -> tuple[Pipeline, dlt.Run]:
    """Execute the PNCP pipeline using the DuckDB destination."""
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
    )
    run_info = pipeline.run(source)
    return pipeline, run_info
