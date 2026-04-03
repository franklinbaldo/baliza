import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import duckdb
import pytest

from baliza.extractor import PNCPExtractor, CheckpointData


@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    return db_path


@pytest.fixture
def fake_page_data():
    return {
        "totalPaginas": 5,
        "data": [
            {
                "numeroControlePNCP": "123",
                "anoCompra": 2023,
                "sequencialCompra": 1,
                "dataPublicacao": "2023-01-01T10:00:00"
            }
        ]
    }


def test_deadline_stops_extraction(temp_db, fake_page_data, monkeypatch):
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    extractor = PNCPExtractor(db_path=temp_db)

    with duckdb.connect(str(temp_db)) as con:
        extractor._ensure_schema(con)

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 1)

    # Deadline is in the past, so it should stop immediately
    deadline = datetime.now() - timedelta(minutes=5)

    with patch("baliza.extractor._fetch_page", return_value=fake_page_data) as mock_fetch:
        result = extractor._extract_range_task(
            start_date=start_date,
            end_date=end_date,
            resource="contratos",
            deadline=deadline
        )

        assert result["pages"] == 1 # Initial value is 1, loop breaks before fetching page 1
        assert result["rows_extracted"] == 0
        mock_fetch.assert_not_called()

        # Verify coverage status is 'deadline'
        with duckdb.connect(str(temp_db)) as con:
            coverage = con.execute(
                "SELECT status FROM baliza_state.coverage WHERE resource = 'contratos'"
            ).fetchone()
            assert coverage is not None
            assert coverage[0] == "deadline"

def test_no_deadline_completes_normally(temp_db, fake_page_data, monkeypatch):
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    extractor = PNCPExtractor(db_path=temp_db)

    with duckdb.connect(str(temp_db)) as con:
        extractor._ensure_schema(con)

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 1)

    empty_page = {"totalPaginas": 1, "data": []}

    with patch("baliza.extractor._fetch_page", side_effect=[fake_page_data, empty_page]) as mock_fetch:
        result = extractor._extract_range_task(
            start_date=start_date,
            end_date=end_date,
            resource="contratos",
            deadline=None
        )

        assert result["pages"] >= 1
        assert result["rows_extracted"] == 1

        with duckdb.connect(str(temp_db)) as con:
            # Verify status is 'complete'
            coverage = con.execute(
                "SELECT status FROM baliza_state.coverage WHERE resource = 'contratos'"
            ).fetchone()
            assert coverage is not None
            assert coverage[0] == "complete"

            # Verify checkpoint is cleared
            checkpoint = con.execute(
                "SELECT * FROM baliza_state.extraction_checkpoint"
            ).fetchone()
            assert checkpoint is None

def test_deadline_preserves_checkpoint(temp_db, fake_page_data, monkeypatch):
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    extractor = PNCPExtractor(db_path=temp_db)

    with duckdb.connect(str(temp_db)) as con:
        extractor._ensure_schema(con)

    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 1)

    # We want it to do one page and then stop by deadline, OR stop immediately but we have a checkpoint beforehand
    # The instruction says: "With expired deadline, verify `_clear_checkpoint` is NOT called (checkpoint row still exists in DuckDB)."
    # Let's seed a checkpoint manually, then run with expired deadline.

    with duckdb.connect(str(temp_db)) as con:
        stats = {"current_page": 2, "total_pages": 5, "rows_extracted": 100}
        extractor._save_checkpoint(con, "contratos", start_date, stats, datetime.now())

        checkpoint_before = con.execute(
            "SELECT current_page FROM baliza_state.extraction_checkpoint"
        ).fetchone()
        assert checkpoint_before[0] == 2

    deadline = datetime.now() - timedelta(minutes=5)

    with patch("baliza.extractor._fetch_page") as mock_fetch:
        extractor._extract_range_task(
            start_date=start_date,
            end_date=end_date,
            resource="contratos",
            deadline=deadline
        )

        with duckdb.connect(str(temp_db)) as con:
            checkpoint_after = con.execute(
                "SELECT current_page FROM baliza_state.extraction_checkpoint"
            ).fetchone()
            # The checkpoint should still exist because it was stopped by deadline
            assert checkpoint_after is not None
            assert checkpoint_after[0] == 2
