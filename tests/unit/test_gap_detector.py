"""Unit tests for GapDetector."""

from datetime import UTC, date, datetime, timedelta

from baliza.state import GapDetector, StateManager, Window


def test_gap_detector_finds_missing_windows(tmp_path):
    """Test that GapDetector identifies never-extracted windows."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 3, tzinfo=UTC)

    gaps = detector.find_gaps("contratos", start, end, lookback_days=0)

    # Should find 3 days (Jan 1, 2, 3)
    assert len(gaps) == 3
    assert all(g.reason == "missing" for g in gaps)
    assert gaps[0].start.date() == date(2024, 1, 1)
    assert gaps[1].start.date() == date(2024, 1, 2)
    assert gaps[2].start.date() == date(2024, 1, 3)

    manager.close()


def test_gap_detector_respects_completed_windows(tmp_path):
    """Test that completed windows are not included in gaps."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    tracker = manager.tracker

    # Mark Jan 1 as complete
    jan1_start = datetime(2024, 1, 1)
    jan1_end = datetime(2024, 1, 2)
    periodo = tracker.period_label(jan1_start, jan1_end)
    tracker.mark_window_status("contratos", periodo, "ok")

    detector = GapDetector(manager)
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 3, tzinfo=UTC),
        lookback_days=0,
    )

    # Should only find Jan 2 and 3 (Jan 1 is complete)
    assert len(gaps) == 2
    assert gaps[0].start.date() == date(2024, 1, 2)
    assert gaps[1].start.date() == date(2024, 1, 3)

    manager.close()


def test_gap_detector_prioritizes_incomplete_windows(tmp_path):
    """Test that incomplete windows have highest priority."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    tracker = manager.tracker

    # Mark Jan 1 as incomplete
    jan1_start = datetime(2024, 1, 1)
    jan1_end = datetime(2024, 1, 2)
    jan1_periodo = tracker.period_label(jan1_start, jan1_end)
    tracker.mark_window_status("contratos", jan1_periodo, "incompleto")

    # Jan 2 is not processed (missing)
    # Jan 3 is suspect
    jan3_start = datetime(2024, 1, 3)
    jan3_end = datetime(2024, 1, 4)
    jan3_periodo = tracker.period_label(jan3_start, jan3_end)
    tracker.mark_window_status("contratos", jan3_periodo, "suspeito")

    detector = GapDetector(manager)
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 3, tzinfo=UTC),
        lookback_days=0,
    )

    # Should have 3 gaps with correct priorities
    assert len(gaps) == 3

    # Incomplete window should come first (priority=1)
    assert gaps[0].reason == "incomplete"
    assert gaps[0].priority == 1
    assert gaps[0].start.date() == date(2024, 1, 1)

    # Suspect window should be second (priority=2)
    assert gaps[1].reason == "suspect"
    assert gaps[1].priority == 2
    assert gaps[1].start.date() == date(2024, 1, 3)

    # Missing window should be last (priority=3)
    assert gaps[2].reason == "missing"
    assert gaps[2].priority == 3
    assert gaps[2].start.date() == date(2024, 1, 2)

    manager.close()


def test_gap_detector_handles_lookback(tmp_path):
    """Test that lookback extends the date range backward from last successful run."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Create a successful run completed 10 days ago
    run_id = manager.start_run("contratos")
    ten_days_ago = datetime.now(UTC) - timedelta(days=10)

    # Mark one window as complete (10 days ago)
    periodo = manager.tracker.period_label(ten_days_ago, ten_days_ago + timedelta(days=1))
    manager.tracker.mark_window_status("contratos", periodo, "ok")

    # Set the run completion time to 10 days ago
    manager.tracker.conn.execute(
        "UPDATE baliza_state.extraction_runs SET completed_at = ?, status = 'completed' WHERE run_id = ?",
        [ten_days_ago, run_id],
    )
    manager.tracker.conn.commit()

    detector = GapDetector(manager)

    # Request only today, but with 3-day lookback from last run
    # This should extend back to 13 days ago (10 days + 3 day lookback)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    gaps = detector.find_gaps(
        "contratos",
        today,
        today,
        lookback_days=3,
    )

    # Should have gaps from 13 days ago to today (all missing except the one marked 'ok')
    # That's at least today + some historical days
    assert len(gaps) > 1  # At least today + some lookback days

    # Verify that lookback extended the range
    earliest_gap = min(gaps, key=lambda g: g.start)
    # Should have extended back at least a few days before "today"
    assert earliest_gap.start < today

    manager.close()


def test_merge_adjacent_windows(tmp_path):
    """Test that adjacent windows are merged efficiently."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    # Create 7 consecutive missing days
    windows = [
        Window(
            start=datetime(2024, 1, i),
            end=datetime(2024, 1, i) + timedelta(days=1),
            reason="missing",
            priority=3,
        )
        for i in range(1, 8)
    ]

    merged = detector.merge_adjacent_windows(windows, max_merge_days=7)

    # Should merge into 1 window
    assert len(merged) == 1
    assert merged[0].start.date() == date(2024, 1, 1)
    assert merged[0].end.date() == date(2024, 1, 8)

    manager.close()


def test_merge_respects_max_merge_days(tmp_path):
    """Test that merge doesn't exceed max_merge_days."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    # Create 10 consecutive days
    windows = [
        Window(
            start=datetime(2024, 1, i),
            end=datetime(2024, 1, i) + timedelta(days=1),
            reason="missing",
            priority=3,
        )
        for i in range(1, 11)
    ]

    # Merge with max_merge_days=5
    merged = detector.merge_adjacent_windows(windows, max_merge_days=5)

    # Should create 2 windows (5 days each)
    assert len(merged) == 2
    assert (merged[0].end - merged[0].start).days == 5
    assert (merged[1].end - merged[1].start).days == 5

    manager.close()


def test_merge_respects_priority_boundaries(tmp_path):
    """Test that merge doesn't cross priority boundaries."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    # Create windows with different priorities
    windows = [
        Window(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
            reason="incomplete",
            priority=1,
        ),
        Window(
            start=datetime(2024, 1, 2),
            end=datetime(2024, 1, 3),
            reason="missing",
            priority=3,
        ),
        Window(
            start=datetime(2024, 1, 3),
            end=datetime(2024, 1, 4),
            reason="missing",
            priority=3,
        ),
    ]

    merged = detector.merge_adjacent_windows(windows, max_merge_days=7)

    # Should NOT merge across priority boundaries
    assert len(merged) == 2
    assert merged[0].priority == 1
    assert merged[0].reason == "incomplete"
    assert merged[1].priority == 3
    assert merged[1].reason == "missing"
    assert (merged[1].end - merged[1].start).days == 2  # Two missing days merged

    manager.close()


def test_merge_handles_non_adjacent_windows(tmp_path):
    """Test that merge doesn't merge windows with gaps."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    # Create windows with a gap
    windows = [
        Window(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
            reason="missing",
            priority=3,
        ),
        Window(
            start=datetime(2024, 1, 3),  # Gap: Jan 2
            end=datetime(2024, 1, 4),
            reason="missing",
            priority=3,
        ),
    ]

    merged = detector.merge_adjacent_windows(windows, max_merge_days=7)

    # Should NOT merge (not adjacent)
    assert len(merged) == 2

    manager.close()


def test_count_gaps_by_reason(tmp_path):
    """Test that count_gaps_by_reason correctly counts windows."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    gaps = [
        Window(datetime(2024, 1, 1), datetime(2024, 1, 2), "missing", 3),
        Window(datetime(2024, 1, 2), datetime(2024, 1, 3), "missing", 3),
        Window(datetime(2024, 1, 3), datetime(2024, 1, 4), "incomplete", 1),
        Window(datetime(2024, 1, 4), datetime(2024, 1, 5), "suspect", 2),
        Window(datetime(2024, 1, 5), datetime(2024, 1, 6), "suspect", 2),
    ]

    counts = detector.count_gaps_by_reason(gaps)

    assert counts["missing"] == 2
    assert counts["incomplete"] == 1
    assert counts["suspect"] == 2

    manager.close()


def test_gap_detector_excludes_suspect_when_requested(tmp_path):
    """Test that suspect windows can be excluded from gaps."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    tracker = manager.tracker

    # Mark Jan 1 as suspect
    jan1_periodo = tracker.period_label(datetime(2024, 1, 1), datetime(2024, 1, 2))
    tracker.mark_window_status("contratos", jan1_periodo, "suspeito")

    detector = GapDetector(manager)

    # Find gaps without suspect windows
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 2, tzinfo=UTC),
        lookback_days=0,
        include_suspect=False,
    )

    # Should only have Jan 2 (missing), not Jan 1 (suspect)
    assert len(gaps) == 1
    assert gaps[0].start.date() == date(2024, 1, 2)
    assert gaps[0].reason == "missing"

    manager.close()


def test_gap_detector_with_empty_range(tmp_path):
    """Test that GapDetector handles empty date ranges gracefully."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    # Same start and end date
    start = datetime(2024, 1, 1, tzinfo=UTC)
    gaps = detector.find_gaps("contratos", start, start, lookback_days=0)

    # Should have exactly 1 window (the single day)
    assert len(gaps) == 1
    assert gaps[0].start.date() == date(2024, 1, 1)

    manager.close()


def test_window_repr(tmp_path):
    """Test that Window __repr__ is readable."""
    window = Window(
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 2),
        reason="missing",
        priority=3,
    )

    repr_str = repr(window)
    assert "2024-01-01" in repr_str
    assert "2024-01-02" in repr_str
    assert "missing" in repr_str
    assert "priority=3" in repr_str
