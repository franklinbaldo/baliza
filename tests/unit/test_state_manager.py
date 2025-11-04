"""Unit tests for StateManager."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from baliza.state import StateManager, ExtractionRun


def test_state_manager_initialization(tmp_path):
    """Test that StateManager initializes correctly."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    assert manager.duckdb_path == db_path
    assert manager.dataset == "baliza_raw"
    assert manager.tracker is not None

    # Verify table was created
    result = manager.tracker.conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='extraction_runs'
    """).fetchone()

    # Note: In DuckDB the table is in baliza_state schema
    # Let's check it exists
    manager.tracker.conn.execute("SELECT * FROM baliza_state.extraction_runs LIMIT 0")

    manager.close()


def test_start_run_creates_run_id(tmp_path):
    """Test that start_run creates a unique run ID."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    run_id = manager.start_run("contratos")

    # Run ID should have format: YYYYMMDD-HHMMSS-<uuid>
    assert run_id.startswith(f"{datetime.now():%Y%m%d}")
    assert len(run_id.split("-")) == 3  # YYYYMMDD-HHMMSS-uuid

    # Verify it was inserted
    run = manager.get_run(run_id)
    assert run is not None
    assert run.run_id == run_id
    assert run.resource == "contratos"
    assert run.status == "running"

    manager.close()


def test_complete_run_updates_status(tmp_path):
    """Test that complete_run marks run as completed."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    run_id = manager.start_run("contratos")

    # Complete the run
    stats = {
        "windows_completed": 3,
        "pages_fetched": 45,
        "rows_extracted": 22500,
    }
    manager.complete_run(run_id, stats)

    # Verify status updated
    run = manager.get_run(run_id)
    assert run is not None
    assert run.status == "completed"
    assert run.completed_at is not None
    assert run.windows_completed == 3
    assert run.pages_fetched == 45
    assert run.rows_extracted == 22500

    manager.close()


def test_fail_run_records_error(tmp_path):
    """Test that fail_run marks run as failed with error message."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    run_id = manager.start_run("contratos")

    # Fail the run
    error_msg = "Connection timeout after 30 seconds"
    manager.fail_run(run_id, error_msg)

    # Verify status and error recorded
    run = manager.get_run(run_id)
    assert run is not None
    assert run.status == "failed"
    assert run.completed_at is not None
    assert run.error_message == error_msg

    manager.close()


def test_get_last_successful_run(tmp_path):
    """Test that get_last_successful_run returns the most recent completed run."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Create multiple runs
    run1 = manager.start_run("contratos")
    manager.complete_run(run1, {"windows_completed": 1})

    run2 = manager.start_run("contratos")
    manager.complete_run(run2, {"windows_completed": 2})

    run3 = manager.start_run("contratos")
    manager.fail_run(run3, "error")  # This one failed

    # Get last successful run
    last_run = manager.get_last_successful_run("contratos")
    assert last_run is not None
    assert last_run.run_id == run2
    assert last_run.windows_completed == 2

    manager.close()


def test_get_last_successful_run_returns_none_when_no_runs(tmp_path):
    """Test that get_last_successful_run returns None when no runs exist."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    last_run = manager.get_last_successful_run("contratos")
    assert last_run is None

    manager.close()


def test_get_last_run_returns_any_status(tmp_path):
    """Test that get_last_run returns most recent run regardless of status."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    run1 = manager.start_run("contratos")
    manager.complete_run(run1, {"windows_completed": 1})

    run2 = manager.start_run("contratos")
    manager.fail_run(run2, "error")

    # Get last run (should be the failed one)
    last_run = manager.get_last_run("contratos")
    assert last_run is not None
    assert last_run.run_id == run2
    assert last_run.status == "failed"

    manager.close()


def test_get_run_history(tmp_path):
    """Test that get_run_history returns recent runs."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Create several runs
    run_ids = []
    for i in range(5):
        run_id = manager.start_run("contratos")
        manager.complete_run(run_id, {"windows_completed": i})
        run_ids.append(run_id)

    # Get history (limit to 3)
    history = manager.get_run_history("contratos", limit=3)
    assert len(history) == 3

    # Should be in reverse chronological order (newest first)
    assert history[0].run_id == run_ids[4]
    assert history[1].run_id == run_ids[3]
    assert history[2].run_id == run_ids[2]

    manager.close()


def test_update_run_progress(tmp_path):
    """Test that update_run_progress updates metrics."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    run_id = manager.start_run("contratos")

    # Update progress
    manager.update_run_progress(
        run_id,
        windows_attempted=5,
        windows_completed=3,
        pages_fetched=45,
    )

    # Verify updates
    run = manager.get_run(run_id)
    assert run is not None
    assert run.windows_attempted == 5
    assert run.windows_completed == 3
    assert run.pages_fetched == 45

    manager.close()


def test_get_running_runs(tmp_path):
    """Test that get_running_runs returns only runs with status='running'."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Create multiple runs with different statuses
    run1 = manager.start_run("contratos")
    # Leave run1 as 'running'

    run2 = manager.start_run("contratos")
    manager.complete_run(run2, {})

    run3 = manager.start_run("licitacoes")
    # Leave run3 as 'running'

    # Get all running runs
    running = manager.get_running_runs()
    assert len(running) == 2
    run_ids = {r.run_id for r in running}
    assert run1 in run_ids
    assert run3 in run_ids
    assert run2 not in run_ids

    # Get running runs for specific resource
    running_contratos = manager.get_running_runs(resource="contratos")
    assert len(running_contratos) == 1
    assert running_contratos[0].run_id == run1

    manager.close()


def test_state_survives_manager_restart(tmp_path):
    """Test that state persists after manager is recreated."""
    db_path = tmp_path / "test.duckdb"

    # First manager instance
    manager1 = StateManager(db_path)
    run_id = manager1.start_run("contratos")
    manager1.complete_run(run_id, {"windows_completed": 5})
    manager1.close()

    # Second manager instance (simulates restart)
    manager2 = StateManager(db_path)
    last_run = manager2.get_last_successful_run("contratos")
    assert last_run is not None
    assert last_run.run_id == run_id
    assert last_run.windows_completed == 5

    manager2.close()


def test_extraction_run_asdict(tmp_path):
    """Test that ExtractionRun.asdict() returns correct dictionary."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    run_id = manager.start_run("contratos")
    manager.complete_run(run_id, {
        "windows_completed": 3,
        "pages_fetched": 45,
        "rows_extracted": 22500,
    })

    run = manager.get_run(run_id)
    assert run is not None

    run_dict = run.asdict()
    assert run_dict["run_id"] == run_id
    assert run_dict["resource"] == "contratos"
    assert run_dict["status"] == "completed"
    assert run_dict["windows_completed"] == 3
    assert isinstance(run_dict["started_at"], str)  # ISO format
    assert isinstance(run_dict["completed_at"], str)  # ISO format

    manager.close()


def test_multiple_resources_independent(tmp_path):
    """Test that state for different resources is independent."""
    db_path = tmp_path / "test.duckdb"
    manager = StateManager(db_path)

    # Create runs for different resources
    run1 = manager.start_run("contratos")
    manager.complete_run(run1, {"windows_completed": 5})

    run2 = manager.start_run("licitacoes")
    manager.complete_run(run2, {"windows_completed": 10})

    # Verify they're independent
    last_contratos = manager.get_last_successful_run("contratos")
    last_licitacoes = manager.get_last_successful_run("licitacoes")

    assert last_contratos is not None
    assert last_licitacoes is not None
    assert last_contratos.run_id == run1
    assert last_licitacoes.run_id == run2
    assert last_contratos.windows_completed == 5
    assert last_licitacoes.windows_completed == 10

    manager.close()
