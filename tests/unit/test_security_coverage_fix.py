
import pytest
from baliza.state.coverage import CoverageTracker
import duckdb

def test_derive_window_candidates_security(tmp_path):
    tracker = CoverageTracker(tmp_path / "test.duckdb")

    # Setup
    tracker.conn.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
    tracker.conn.execute("CREATE TABLE baliza_raw.test_table (id INTEGER, dataPublicacaoPncp TIMESTAMP)")
    tracker.conn.execute("INSERT INTO baliza_raw.test_table VALUES (1, '2023-01-01 10:00:00')")

    # Attempt Injection
    payload = "dataPublicacaoPncp) THIS_IS_INJECTED"

    # Should raise BinderError (column not found) because it quotes the whole string
    # If vulnerable, it would raise ParserException (syntax error) or execute unexpected logic
    with pytest.raises(duckdb.BinderException) as excinfo:
        tracker.derive_window_candidates("test_table", date_field=payload)

    assert 'Referenced column "dataPublicacaoPncp) THIS_IS_INJECTED" not found' in str(excinfo.value)

    tracker.close()

def test_derive_window_candidates_works_normal(tmp_path):
    tracker = CoverageTracker(tmp_path / "test.duckdb")
    tracker.conn.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
    tracker.conn.execute("CREATE TABLE baliza_raw.test_table (id INTEGER, dataPublicacaoPncp TIMESTAMP)")
    tracker.conn.execute("INSERT INTO baliza_raw.test_table VALUES (1, '2023-01-01 10:00:00')")

    candidates = tracker.derive_window_candidates("test_table")
    assert len(candidates) == 1
    tracker.close()
