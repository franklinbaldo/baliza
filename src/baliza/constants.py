from __future__ import annotations

from datetime import date

# PNCP's contratos endpoint has no data before 2021-09-06. Treat this as
# immutable product-domain knowledge so backfills never waste runs probing
# months that cannot contain source records.
PNCP_RESOURCE_DATA_STARTS: dict[str, date] = {
    "contratos": date(2021, 9, 6),
}


def known_data_start(resource: str) -> date:
    """Return the first known source-data date for a PNCP resource."""
    return PNCP_RESOURCE_DATA_STARTS[resource]


def known_data_start_month(resource: str) -> date:
    """Return the first monthly partition that can contain PNCP source data."""
    start = known_data_start(resource)
    return start.replace(day=1)


def clamp_to_known_data_start_month(resource: str, start: date) -> date:
    """Clamp a requested start date to the first possible source-data month."""
    start_month = start.replace(day=1)
    return max(start_month, known_data_start_month(resource))


def month_predates_known_data(resource: str, month: date) -> bool:
    """Return True when a monthly partition cannot contain source data."""
    return month.replace(day=1) < known_data_start_month(resource)
