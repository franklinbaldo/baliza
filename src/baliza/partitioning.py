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
from datetime import date, timedelta

from .resources import PNCPResource
from .resources.specs import PartitionStrategy

_ONE_DAY = timedelta(days=1)


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


def _unsupported_strategy(strategy: object, resource: PNCPResource) -> ValueError:
    return ValueError(
        f"unsupported partition_strategy {strategy!r} for "
        f"resource {resource.resource_name!r}"
    )


def _next_month(d: date) -> date:
    return d.replace(year=d.year + 1, month=1, day=1) if d.month == 12 \
        else d.replace(month=d.month + 1, day=1)


def _last_day_of_month(d: date) -> date:
    nxt = _next_month(d.replace(day=1))
    return nxt.replace(day=1) - _ONE_DAY


def iter_partitions(
    resource: PNCPResource, start: date, end: date,
) -> Iterator[PartitionPeriod]:
    """Yield every partition window between ``start`` and ``end``, inclusive.

    Strategy comes from ``resource.raw_dataset.partition_strategy``.
    Monthly resources yield one period per calendar month; annual
    resources yield one period per calendar year. Unknown strategies
    raise so a misconfigured resource can't silently fall back to
    monthly. A reversed range (``start > end``) raises too — both
    branches normalize boundaries, so a swap can land inside the same
    partition and silently yield one period, masking a caller bug.
    """
    if start > end:
        raise ValueError(
            f"start {start.isoformat()} is after end {end.isoformat()}"
        )
    strategy = resource.raw_dataset.partition_strategy
    if strategy == PartitionStrategy.MONTHLY:
        cursor = start.replace(day=1)
        last = end.replace(day=1)
        while cursor <= last:
            yield PartitionPeriod(
                start=cursor,
                end=_last_day_of_month(cursor),
                label=cursor.strftime("%Y-%m"),
            )
            cursor = _next_month(cursor)
    elif strategy == PartitionStrategy.ANNUAL:
        cursor = date(start.year, 1, 1)
        while cursor.year <= end.year:
            yield PartitionPeriod(
                start=cursor,
                end=date(cursor.year, 12, 31),
                label=str(cursor.year),
            )
            cursor = date(cursor.year + 1, 1, 1)
    else:
        raise _unsupported_strategy(strategy, resource)


def partition_label(resource: PNCPResource, anchor: date) -> str:
    """The ``data_particao`` label for the partition containing ``anchor``."""
    strategy = resource.raw_dataset.partition_strategy
    if strategy == PartitionStrategy.MONTHLY:
        return anchor.strftime("%Y-%m")
    if strategy == PartitionStrategy.ANNUAL:
        return str(anchor.year)
    raise _unsupported_strategy(strategy, resource)


def partition_for(resource: PNCPResource, anchor: date) -> PartitionPeriod:
    """The single ``PartitionPeriod`` containing ``anchor``.

    Convenience for per-partition workers (``mirror_month`` /
    ``build_month``) that previously computed ``end_of_month`` inline
    via ``start_of_month + relativedelta(months=1) - 1day``. With this
    helper they pick up annual support automatically.
    """
    start = current_partition_start(resource, anchor)
    # iter_partitions yields exactly one period when start == end of
    # the same partition, regardless of strategy.
    return next(iter_partitions(resource, start, start))


def parse_partition_label(resource: PNCPResource, label: str) -> date | None:
    """Inverse of ``partition_label``: parse a ``data_particao`` value
    back into the partition's start date.

    Returns ``None`` when the label can't be parsed under the
    resource's strategy — callers use this to skip rows whose
    ``data_particao`` is corrupt or written under a different
    partitioning scheme. A misconfigured resource raises (matches
    iter_partitions / partition_label).
    """
    strategy = resource.raw_dataset.partition_strategy
    try:
        if strategy == PartitionStrategy.MONTHLY:
            if len(label) == 7 and label[4] == "-":
                return date(int(label[:4]), int(label[5:7]), 1)
            return None
        if strategy == PartitionStrategy.ANNUAL:
            return date(int(label), 1, 1) if len(label) == 4 else None
    except (ValueError, IndexError):
        return None
    raise _unsupported_strategy(strategy, resource)


def current_partition_start(resource: PNCPResource, anchor: date | None = None) -> date:
    """Start date of the partition containing ``anchor`` (default today).

    Replaces the ``today.replace(day=1)`` pattern that hardcoded
    monthly partitioning across mirror/builder. Annual resources get
    January 1 of the anchor's year.
    """
    if anchor is None:
        anchor = date.today()
    strategy = resource.raw_dataset.partition_strategy
    if strategy == PartitionStrategy.MONTHLY:
        return anchor.replace(day=1)
    if strategy == PartitionStrategy.ANNUAL:
        return date(anchor.year, 1, 1)
    raise _unsupported_strategy(strategy, resource)


def previous_partition_start(resource: PNCPResource, anchor: date | None = None) -> date:
    """Start date of the partition immediately before the one
    containing ``anchor`` (default today). Used by mirror/builder to
    bound the "complete" range — the current partition is always
    incomplete (still receiving updates) and is iterated separately.
    """
    if anchor is None:
        anchor = date.today()
    current = current_partition_start(resource, anchor)
    return _previous_partition_start_from(resource, current)


def _previous_partition_start_from(resource: PNCPResource, current_start: date) -> date:
    strategy = resource.raw_dataset.partition_strategy
    if strategy == PartitionStrategy.MONTHLY:
        return (current_start - _ONE_DAY).replace(day=1)
    if strategy == PartitionStrategy.ANNUAL:
        return date(current_start.year - 1, 1, 1)
    raise _unsupported_strategy(strategy, resource)


def clamp_to_data_start(resource: PNCPResource, start: date) -> date:
    """Partition-aware floor for backfill start dates.

    Mirrors the legacy ``clamp_to_known_data_start_month`` (still in
    constants.py for callers outside the partition iteration) but
    handles annual resources by clamping to January 1 of the
    floor's year. Resources without a ``data_start`` get the
    partition-normalized start unchanged.
    """
    floor = resource.data_start
    if floor is None:
        return current_partition_start(resource, start)
    floor_partition = current_partition_start(resource, floor)
    requested_partition = current_partition_start(resource, start)
    return max(floor_partition, requested_partition)
