"""End-to-end test for resumable extraction functionality."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from baliza.state import StateManager, GapDetector


def test_extraction_tracks_state_across_runs(tmp_path):
    """
    Test that extraction state is preserved across multiple runs.

    Simulates a scenario where:
    1. First run processes some windows successfully
    2. State is saved
    3. Second run can see what was completed
    """
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Simulate first run
    run_id_1 = manager.start_run("contratos")

    # Mark some windows as complete
    for i in range(3):
        window_start = datetime(2024, 1, 1 + i)
        window_end = window_start + timedelta(days=1)
        periodo = manager.tracker.period_label(window_start, window_end)
        manager.tracker.mark_window_status("contratos", periodo, "ok")

    # Complete first run
    manager.complete_run(run_id_1, {"windows_completed": 3})
    manager.close()

    # Simulate second run (new manager instance = restart)
    manager2 = StateManager(db_path)
    detector = GapDetector(manager2)

    # Check that previous windows are not in gaps
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 5, tzinfo=timezone.utc),
        lookback_days=0,
    )

    # Should only have Jan 4 and 5 (Jan 1-3 were completed)
    assert len(gaps) == 2
    assert all(g.reason == "missing" for g in gaps)
    assert gaps[0].start.date().day == 4
    assert gaps[1].start.date().day == 5

    # Verify last run is accessible
    last_run = manager2.get_last_successful_run("contratos")
    assert last_run is not None
    assert last_run.run_id == run_id_1
    assert last_run.windows_completed == 3

    manager2.close()


def test_incomplete_windows_have_highest_priority(tmp_path):
    """
    Test that incomplete windows from failed runs are prioritized.

    Simulates:
    1. Extraction starts
    2. Some windows complete, one is marked incomplete
    3. Run fails
    4. Next run should prioritize the incomplete window
    """
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Simulate failed run
    run_id = manager.start_run("contratos")

    # Mark Jan 1 as complete
    jan1_start = datetime(2024, 1, 1)
    jan1_end = datetime(2024, 1, 2)
    jan1_periodo = manager.tracker.period_label(jan1_start, jan1_end)
    manager.tracker.mark_window_status("contratos", jan1_periodo, "ok")

    # Mark Jan 2 as incomplete (extraction failed mid-way)
    jan2_start = datetime(2024, 1, 2)
    jan2_end = datetime(2024, 1, 3)
    jan2_periodo = manager.tracker.period_label(jan2_start, jan2_end)
    manager.tracker.mark_window_status("contratos", jan2_periodo, "incompleto")

    # Fail the run
    manager.fail_run(run_id, "Connection timeout")

    # Next run: check gaps
    detector = GapDetector(manager)
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 3, tzinfo=timezone.utc),
        lookback_days=0,
    )

    # Should have Jan 2 (incomplete, priority=1) and Jan 3 (missing, priority=3)
    assert len(gaps) == 2

    # Incomplete window should be first
    assert gaps[0].reason == "incomplete"
    assert gaps[0].priority == 1
    assert gaps[0].start.date().day == 2

    # Missing window should be second
    assert gaps[1].reason == "missing"
    assert gaps[1].priority == 3
    assert gaps[1].start.date().day == 3

    manager.close()


def test_gap_detector_with_real_date_progression(tmp_path):
    """
    Test gap detection with realistic date progression.

    Simulates daily extraction over a week with some failures.
    """
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Day 1: Extract Jan 1-3 successfully
    run1 = manager.start_run("contratos")
    for day in range(1, 4):
        start = datetime(2024, 1, day)
        end = start + timedelta(days=1)
        periodo = manager.tracker.period_label(start, end)
        manager.tracker.mark_window_status("contratos", periodo, "ok")
    manager.complete_run(run1, {"windows_completed": 3})

    # Day 2: Try to extract Jan 4-6, but fail at Jan 5
    run2 = manager.start_run("contratos")
    # Jan 4 succeeds
    jan4_periodo = manager.tracker.period_label(
        datetime(2024, 1, 4), datetime(2024, 1, 5)
    )
    manager.tracker.mark_window_status("contratos", jan4_periodo, "ok")
    # Jan 5 fails (marked incomplete)
    jan5_periodo = manager.tracker.period_label(
        datetime(2024, 1, 5), datetime(2024, 1, 6)
    )
    manager.tracker.mark_window_status("contratos", jan5_periodo, "incompleto")
    manager.fail_run(run2, "API timeout")

    # Day 3: Resume extraction for Jan 5-7
    detector = GapDetector(manager)
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 7, tzinfo=timezone.utc),
        lookback_days=0,
    )

    # Should have:
    # - Jan 5 (incomplete, priority=1)
    # - Jan 6, 7 (missing, priority=3)
    assert len(gaps) == 3
    assert gaps[0].reason == "incomplete"  # Jan 5 first
    assert gaps[0].start.date().day == 5
    assert gaps[1].reason == "missing"  # Jan 6
    assert gaps[2].reason == "missing"  # Jan 7

    # Simulate completing the remaining windows
    run3 = manager.start_run("contratos")
    for gap in gaps:
        periodo = manager.tracker.period_label(gap.start, gap.end)
        manager.tracker.mark_window_status("contratos", periodo, "ok")
    manager.complete_run(run3, {"windows_completed": 3})

    # Now all windows should be complete
    final_gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 7, tzinfo=timezone.utc),
        lookback_days=0,
    )
    assert len(final_gaps) == 0  # No gaps!

    manager.close()


def test_run_history_tracks_successes_and_failures(tmp_path):
    """Test that run history correctly tracks both successful and failed runs."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Create mix of successful and failed runs
    run1 = manager.start_run("contratos")
    manager.complete_run(run1, {"windows_completed": 5})

    run2 = manager.start_run("contratos")
    manager.fail_run(run2, "Network error")

    run3 = manager.start_run("contratos")
    manager.complete_run(run3, {"windows_completed": 3})

    # Get history
    history = manager.get_run_history("contratos", limit=10)

    assert len(history) == 3

    # Newest first
    assert history[0].run_id == run3
    assert history[0].status == "completed"
    assert history[0].windows_completed == 3

    assert history[1].run_id == run2
    assert history[1].status == "failed"
    assert history[1].error_message == "Network error"

    assert history[2].run_id == run1
    assert history[2].status == "completed"
    assert history[2].windows_completed == 5

    # Get last successful run (should skip the failed one)
    last_successful = manager.get_last_successful_run("contratos")
    assert last_successful.run_id == run3

    manager.close()


def test_window_merging_reduces_api_calls(tmp_path):
    """Test that adjacent missing windows are merged to reduce API calls."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)
    detector = GapDetector(manager)

    # All windows from Jan 1-7 are missing
    gaps = detector.find_gaps(
        "contratos",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 7, tzinfo=timezone.utc),
        lookback_days=0,
    )

    # Should have 7 individual daily windows
    assert len(gaps) == 7

    # Merge with max_merge_days=7
    merged = detector.merge_adjacent_windows(gaps, max_merge_days=7)

    # Should merge into 1 window
    assert len(merged) == 1
    assert merged[0].start.date().day == 1
    assert merged[0].end.date().day == 8  # Exclusive end
    assert (merged[0].end - merged[0].start).days == 7

    # Merge with max_merge_days=3
    merged_small = detector.merge_adjacent_windows(gaps, max_merge_days=3)

    # Should merge into 3 windows (3 days + 3 days + 1 day)
    assert len(merged_small) == 3

    manager.close()
