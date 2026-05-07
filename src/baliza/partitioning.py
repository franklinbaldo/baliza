"""Partition iteration based on a resource's ``partition_strategy``.

The mirror and builder pipelines historically assumed monthly
partitions everywhere (``YYYY-MM`` labels, ``relativedelta(months=1)``
steps). PCA — the next resource queued for promotion — is naturally
annual, so the iteration shape needs to come from the resource itself
instead of being baked into ``_pending_mirror_months`` /
``_pending_build_months``.

This module is the typed entry point. It does *not* rewire the mirror
or builder yet (that lands with PCA promotion); it gives those callers
a single helper to consume, and pins the shapes monthly/annual must
satisfy with unit tests so PCA promotion can wire through without
inventing the abstraction at the same time.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date

from .resources import PNCPResource


@dataclass(frozen=True)
class PartitionPeriod:
    """A single partition window for a resource.

    ``start`` is inclusive, ``end`` is inclusive, ``label`` is the
    string the manifest's ``data_particao`` column carries for that
    window (``YYYY-MM`` for monthly, ``YYYY`` for annual).
    """
    start: date
    end: date
    label: str


def _next_month(d: date) -> date:
    return d.replace(year=d.year + 1, month=1, day=1) if d.month == 12 \
        else d.replace(month=d.month + 1, day=1)


def _last_day_of_month(d: date) -> date:
    nxt = _next_month(d.replace(day=1))
    return nxt.replace(day=1) - _ONE_DAY


_ONE_DAY = (date(2000, 1, 2) - date(2000, 1, 1))


def iter_partitions(
    resource: PNCPResource, start: date, end: date,
) -> Iterator[PartitionPeriod]:
    """Yield every partition window between ``start`` and ``end``, inclusive.

    Strategy comes from ``resource.raw_dataset.partition_strategy``.
    Monthly resources yield one period per calendar month; annual
    resources yield one period per calendar year. Unknown strategies
    raise so a misconfigured resource can't silently fall back to
    monthly.
    """
    strategy = resource.raw_dataset.partition_strategy
    if strategy == "monthly":
        cursor = start.replace(day=1)
        last = end.replace(day=1)
        while cursor <= last:
            yield PartitionPeriod(
                start=cursor,
                end=_last_day_of_month(cursor),
                label=cursor.strftime("%Y-%m"),
            )
            cursor = _next_month(cursor)
    elif strategy == "annual":
        cursor = date(start.year, 1, 1)
        while cursor.year <= end.year:
            yield PartitionPeriod(
                start=cursor,
                end=date(cursor.year, 12, 31),
                label=str(cursor.year),
            )
            cursor = date(cursor.year + 1, 1, 1)
    else:
        raise ValueError(
            f"unsupported partition_strategy {strategy!r} for "
            f"resource {resource.resource_name!r}"
        )


def partition_label(resource: PNCPResource, anchor: date) -> str:
    """The ``data_particao`` label for the partition containing ``anchor``."""
    strategy = resource.raw_dataset.partition_strategy
    if strategy == "monthly":
        return anchor.strftime("%Y-%m")
    if strategy == "annual":
        return str(anchor.year)
    raise ValueError(
        f"unsupported partition_strategy {strategy!r} for "
        f"resource {resource.resource_name!r}"
    )
