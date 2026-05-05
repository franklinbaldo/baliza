from __future__ import annotations

from datetime import date

from .resources import CONTRATOS

RESOURCE_CONTRATOS = CONTRATOS.name

# PNCP's contratos endpoint has no data before 2021-09-06. Treat this as
# immutable product-domain knowledge so backfills never waste runs probing
# months that cannot contain source records.
PNCP_RESOURCE_DATA_STARTS: dict[str, date] = {
    RESOURCE_CONTRATOS: date(2021, 9, 6),
}


def _known_data_start(resource: str) -> date:
    """Return the first known source-data date for a PNCP resource."""
    try:
        return PNCP_RESOURCE_DATA_STARTS[resource]
    except KeyError as e:
        raise ValueError(f"No known PNCP data start configured for resource: {resource}") from e


def known_data_start_month(resource: str) -> date:
    """Return the first monthly partition that can contain PNCP source data."""
    start = _known_data_start(resource)
    return start.replace(day=1)


def clamp_to_known_data_start_month(resource: str, start: date) -> date:
    """Clamp a requested start date to the first possible source-data month.

    If no data-start is configured for the resource, returns the month-normalised
    start unchanged so callers never crash on resources not yet in the registry.
    """
    start_month = start.replace(day=1)
    floor = PNCP_RESOURCE_DATA_STARTS.get(resource)
    if floor is None:
        return start_month
    return max(start_month, floor.replace(day=1))


def month_predates_known_data(resource: str, month: date) -> bool:
    """Return True when a monthly partition cannot contain source data."""
    return month.replace(day=1) < known_data_start_month(resource)
