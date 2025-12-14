"""Gap detection for extraction coverage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

from .manager import StateManager


@dataclass(slots=True)
class Window:
    """Represents a time window that needs extraction."""

    start: datetime
    end: datetime
    reason: str  # 'missing', 'incomplete', 'suspect', 'lookback'
    priority: int  # 1=highest (incomplete), 2=suspect, 3=lowest (missing/lookback)

    def __repr__(self) -> str:
        return f"Window({self.start.date()} to {self.end.date()}, {self.reason}, priority={self.priority})"


class GapDetector:
    """Detects gaps in extraction coverage and prioritizes windows for extraction."""

    def __init__(self, state_manager: StateManager):
        """
        Initialize GapDetector with a StateManager.

        Args:
            state_manager: StateManager instance for accessing coverage data
        """
        self.state_manager = state_manager
        self.tracker = state_manager.tracker

    def find_gaps(
        self,
        resource: str,
        start_date: datetime,
        end_date: datetime,
        *,
        lookback_days: int = 0,
        include_suspect: bool = True,
    ) -> List[Window]:
        """
        Find all windows that need extraction within a date range.

        Windows are prioritized:
        1. Incomplete windows (partial extraction, priority=1)
        2. Suspect windows (hash mismatch/anomaly, priority=2)
        3. Missing windows (never extracted, priority=3)
        4. Lookback windows (within lookback period, priority=3)

        Args:
            resource: Name of the resource to check
            start_date: Start of the date range
            end_date: End of the date range (inclusive)
            lookback_days: Number of days to look back from last successful run
            include_suspect: Whether to include suspect windows for re-extraction

        Returns:
            List of Window objects sorted by priority (highest first), then by date
        """
        # Normalize to UTC naive datetimes at start of day
        start_date = self._normalize_date(start_date)
        end_date = self._normalize_date(end_date)

        # Apply lookback if configured
        if lookback_days > 0:
            last_run = self.state_manager.get_last_successful_run(resource)
            if last_run and last_run.completed_at:
                lookback_start = self._normalize_date(
                    last_run.completed_at - timedelta(days=lookback_days)
                )
                start_date = min(start_date, lookback_start)

        # Get window statuses from coverage tracker
        statuses = self.tracker.fetch_window_statuses(resource)
        status_map = {s["periodo"]: s for s in statuses}

        # Generate all possible daily windows in range
        all_windows = self._generate_daily_windows(start_date, end_date)

        gaps: List[Window] = []

        for window_start, window_end in all_windows:
            periodo = self.tracker.period_label(window_start, window_end)
            status_entry = status_map.get(periodo)

            if status_entry is None:
                # Never processed
                gaps.append(
                    Window(
                        start=window_start,
                        end=window_end,
                        reason="missing",
                        priority=3,
                    )
                )
            elif status_entry["status"] == "incompleto":
                # Partial extraction (highest priority)
                gaps.append(
                    Window(
                        start=window_start,
                        end=window_end,
                        reason="incomplete",
                        priority=1,
                    )
                )
            elif status_entry["status"] == "nao_processado":
                # Marked as unprocessed
                gaps.append(
                    Window(
                        start=window_start,
                        end=window_end,
                        reason="unprocessed",
                        priority=3,
                    )
                )
            elif status_entry["status"] == "suspeito" and include_suspect:
                # Hash mismatch or sequence anomaly
                gaps.append(
                    Window(
                        start=window_start,
                        end=window_end,
                        reason="suspect",
                        priority=2,
                    )
                )
            elif status_entry["status"] == "ok":
                # Check if within lookback period (re-extract recent data)
                if lookback_days > 0:
                    now = self._normalize_date(datetime.now(timezone.utc))
                    lookback_threshold = now - timedelta(days=lookback_days)
                    if window_start >= lookback_threshold:
                        gaps.append(
                            Window(
                                start=window_start,
                                end=window_end,
                                reason="lookback",
                                priority=3,
                            )
                        )

        # Sort by priority (ascending, so 1=first), then by date (ascending, so oldest first)
        gaps.sort(key=lambda w: (w.priority, w.start))

        return gaps

    def merge_adjacent_windows(
        self, windows: List[Window], max_merge_days: int = 7
    ) -> List[Window]:
        """
        Merge adjacent windows to reduce the number of API requests.

        Only merges windows that:
        - Have the same priority
        - Have the same reason
        - Are consecutive (no gap between them)
        - Don't exceed max_merge_days when combined

        Args:
            windows: List of Window objects to merge
            max_merge_days: Maximum number of days in a merged window

        Returns:
            List of merged Window objects
        """
        if not windows:
            return []

        merged: List[Window] = []
        current = windows[0]

        for next_window in windows[1:]:
            # Calculate duration if we merge
            merged_duration_days = (next_window.end - current.start).days

            # Check if we can merge
            can_merge = (
                current.priority == next_window.priority
                and current.reason == next_window.reason
                and (next_window.start - current.end).total_seconds() == 0  # Adjacent
                and merged_duration_days <= max_merge_days
            )

            if can_merge:
                # Extend current window to include next window
                current = Window(
                    start=current.start,
                    end=next_window.end,
                    reason=current.reason,
                    priority=current.priority,
                )
            else:
                # Can't merge, save current and move to next
                merged.append(current)
                current = next_window

        # Don't forget the last window
        merged.append(current)
        return merged

    def count_gaps_by_reason(self, gaps: List[Window]) -> dict[str, int]:
        """
        Count windows grouped by reason.

        Args:
            gaps: List of Window objects

        Returns:
            Dictionary mapping reason to count
        """
        counts: dict[str, int] = {}
        for gap in gaps:
            counts[gap.reason] = counts.get(gap.reason, 0) + 1
        return counts

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_date(dt: datetime) -> datetime:
        """
        Normalize a datetime to naive UTC at start of day.

        Args:
            dt: Datetime to normalize

        Returns:
            Naive datetime at 00:00:00
        """
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def _generate_daily_windows(
        start: datetime, end: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Generate daily windows between start and end dates (inclusive).

        Args:
            start: Start date (will be normalized to start of day)
            end: End date (will be normalized to start of day)

        Returns:
            List of (window_start, window_end) tuples representing 24-hour periods
        """
        windows: List[Tuple[datetime, datetime]] = []
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_normalized = end.replace(hour=0, minute=0, second=0, microsecond=0)

        while current <= end_normalized:
            window_start = current
            window_end = current + timedelta(days=1)
            windows.append((window_start, window_end))
            current = window_end

        return windows
