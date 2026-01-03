from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from baliza.state import CoverageTracker


@pytest.fixture()
def temp_tracker(tmp_path: Path) -> CoverageTracker:
    db_path = tmp_path / "buffering_test.duckdb"
    tracker = CoverageTracker(db_path, dataset="baliza_raw")
    yield tracker
    tracker.close()

def test_buffering_defers_writes(temp_tracker: CoverageTracker) -> None:
    # 1. Record a page
    records = [{"numeroControlePNCP": "X-1"}]
    temp_tracker.record_page(
        "test_resource",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=1,
        registros=records,
    )

    # 2. Check DB immediately - should be empty (since buffer limit > 1)
    rows = temp_tracker.conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 0, "Writes should be buffered, not immediate"

    # 3. Check buffer
    assert len(temp_tracker._buffer) == 1

    # 4. Flush
    temp_tracker.flush()

    # 5. Check DB after flush
    rows = temp_tracker.conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "test_resource"

    # 6. Check buffer empty
    assert len(temp_tracker._buffer) == 0

def test_buffer_limit_triggers_flush(temp_tracker: CoverageTracker) -> None:
    # Set small limit for testing
    temp_tracker._batch_limit = 5

    records = [{"numeroControlePNCP": "X-1"}]

    # Add 4 items (limit 5)
    for i in range(4):
        temp_tracker.record_page(
            "test_resource",
            "2024-01-01T00:00:00Z",
            "2024-01-02T00:00:00Z",
            pagina=i,
            total_paginas=10,
            registros=records,
        )

    # Should still be buffered
    assert len(temp_tracker._buffer) == 4
    rows = temp_tracker.conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 0

    # Add 5th item -> trigger flush
    temp_tracker.record_page(
        "test_resource",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=5,
        total_paginas=10,
        registros=records,
    )

    # Should be flushed now
    assert len(temp_tracker._buffer) == 0
    rows = temp_tracker.conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 5

def test_read_triggers_flush(temp_tracker: CoverageTracker) -> None:
    records = [{"numeroControlePNCP": "X-1"}]
    temp_tracker.record_page(
        "test_resource",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=1,
        registros=records,
    )

    # Verify not yet in DB
    rows = temp_tracker.conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 0

    # Call a read method
    result = temp_tracker.fetch_pages_by_window("test_resource")

    # Should have triggered flush
    assert len(temp_tracker._buffer) == 0
    rows = temp_tracker.conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 1

    # Result should include the page
    assert result
    period_key = temp_tracker._period_key(
        datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 2, 0, 0)
    )
    assert period_key in result

def test_close_triggers_flush(tmp_path: Path) -> None:
    db_path = tmp_path / "close_test.duckdb"
    tracker = CoverageTracker(db_path)

    records = [{"numeroControlePNCP": "X-1"}]
    tracker.record_page(
        "test_resource",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=1,
        registros=records,
    )

    tracker.close()

    # Reopen to verify persistence
    conn = duckdb.connect(str(db_path))
    rows = conn.execute("SELECT * FROM baliza_state.cobertura").fetchall()
    conn.close()

    assert len(rows) == 1

def test_buffer_deduplicates_in_batch(temp_tracker: CoverageTracker) -> None:
    """Ensure multiple updates to the same page within one batch are safe (Last-Write-Wins)."""

    # Record version 1
    records_v1 = [{"numeroControlePNCP": "V1"}]
    temp_tracker.record_page(
        "test_resource",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=1,
        registros=records_v1,
    )

    # Buffer should have 1 item
    assert len(temp_tracker._buffer) == 1

    # Record version 2 (same key: resource, window, page)
    records_v2 = [{"numeroControlePNCP": "V2"}]
    temp_tracker.record_page(
        "test_resource",
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        pagina=1,
        total_paginas=1,
        registros=records_v2,
    )

    # Buffer should STILL have 1 item (replaced)
    assert len(temp_tracker._buffer) == 1

    # Flush
    temp_tracker.flush()

    # DB should have 1 row
    rows = temp_tracker.conn.execute("SELECT hash_ids FROM baliza_state.cobertura").fetchall()
    assert len(rows) == 1

    # Verify content corresponds to V2
    hash_v2 = temp_tracker.hash_registros(records_v2)
    assert rows[0][0] == hash_v2
