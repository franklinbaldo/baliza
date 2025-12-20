from datetime import datetime

import duckdb
import pytest

from baliza.state.coverage import CoverageTracker


def test_derive_window_candidates_sql_injection(tmp_path):
    """Verify that CoverageTracker.derive_window_candidates is safe against SQL injection."""

    db_path = tmp_path / "test_security.duckdb"
    tracker = CoverageTracker(str(db_path), dataset="baliza_raw")

    # Setup: Create a table with some data
    tracker.conn.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
    tracker.conn.execute("CREATE TABLE baliza_raw.test_table (id INTEGER, data TIMESTAMP)")
    tracker.conn.execute("INSERT INTO baliza_raw.test_table VALUES (1, '2023-01-01 10:00:00')")

    # Malicious input that tries to inject SQL
    # This payload attempts to union with another select to return arbitrary data
    injection = "data) AS dia FROM baliza_raw.test_table UNION SELECT CAST('1999-01-01' AS TIMESTAMP) AS dia --"

    # Expectation: The injection should be treated as a column name and fail because it doesn't exist.
    # It should raise a duckdb.BinderException (or similar) complaining about the column not found.
    # Note: duckdb.BinderException might be exposed as duckdb.Error or similar.

    with pytest.raises((duckdb.BinderException, duckdb.Error)) as excinfo:
        tracker.derive_window_candidates("test_table", date_field=injection)

    # Verify that the error message relates to the column not being found,
    # confirming it was treated as an identifier.
    error_msg = str(excinfo.value)
    assert "not found" in error_msg or "Referenced column" in error_msg
    assert injection in error_msg

    tracker.close()


def test_derive_window_candidates_valid_identifier_quoted(tmp_path):
    """Verify that valid identifiers are correctly quoted and work."""

    db_path = tmp_path / "test_valid.duckdb"
    tracker = CoverageTracker(str(db_path), dataset="baliza_raw")

    tracker.conn.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
    # Table with a weird column name that requires quoting
    tracker.conn.execute(
        'CREATE TABLE baliza_raw.test_table (id INTEGER, "weird-column" TIMESTAMP)'
    )
    tracker.conn.execute("INSERT INTO baliza_raw.test_table VALUES (1, '2023-01-01 10:00:00')")

    # Should work fine
    candidates = tracker.derive_window_candidates("test_table", date_field="weird-column")

    assert len(candidates) == 1
    start, end = candidates[0]
    assert start == datetime(2023, 1, 1, tzinfo=None)  # naive utc as per implementation

    tracker.close()
