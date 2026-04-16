from datetime import date, datetime

import duckdb
import pytest

from baliza.engine import BalizaEngine
from baliza.extractor import PNCPExtractor


@pytest.fixture
def temp_db(tmp_path):
    return tmp_path / "test.duckdb"

@pytest.fixture
def extractor(temp_db, monkeypatch):
    monkeypatch.setenv("BALIZA_ALLOW_PRIVATE_NETWORKS", "1")
    engine = BalizaEngine(db_path=temp_db)
    ext = PNCPExtractor(engine=engine)
    return ext

def test_health_increments_on_empty_day(extractor, temp_db):
    test_date = date(2023, 1, 1)
    test_dt = datetime.combine(test_date, datetime.min.time())
    
    extractor._update_resource_health("contratos", test_dt, rows_extracted=0)

    with duckdb.connect(str(temp_db)) as con:
        health = con.execute(
            "SELECT consecutive_empty_days, status FROM baliza_state.resource_health WHERE resource = 'contratos'"
        ).fetchone()

        assert health is not None
        assert health[0] == 1
        assert health[1] == "healthy"

def test_health_resets_on_data(extractor, temp_db):
    test_date_1 = date(2023, 1, 1)
    test_date_2 = date(2023, 1, 2)
    
    test_dt1 = datetime.combine(test_date_1, datetime.min.time())
    test_dt2 = datetime.combine(test_date_2, datetime.min.time())

    # Increment once
    extractor._update_resource_health("contratos", test_dt1, rows_extracted=0)

    with duckdb.connect(str(temp_db)) as con:
        health = con.execute(
            "SELECT consecutive_empty_days FROM baliza_state.resource_health WHERE resource = 'contratos'"
        ).fetchone()
        assert health[0] == 1

    # Reset on data
    extractor._update_resource_health("contratos", test_dt2, rows_extracted=100)

    with duckdb.connect(str(temp_db)) as con:
        health_after = con.execute(
            "SELECT consecutive_empty_days, status, last_nonempty_date FROM baliza_state.resource_health WHERE resource = 'contratos'"
        ).fetchone()

        assert health_after[0] == 0
        assert health_after[1] == "healthy"
        assert health_after[2] == test_date_2

def test_warning_at_3_days(extractor, temp_db):
    for i in range(3):
        test_date = date(2023, 1, 1 + i)
        test_dt = datetime.combine(test_date, datetime.min.time())
        extractor._update_resource_health("contratos", test_dt, rows_extracted=0)

    with duckdb.connect(str(temp_db)) as con:
        health = con.execute(
            "SELECT consecutive_empty_days, status FROM baliza_state.resource_health WHERE resource = 'contratos'"
        ).fetchone()

        assert health[0] == 3
        assert health[1] == "warning"

def test_stalled_at_7_days(extractor, temp_db):
    for i in range(7):
        test_date = date(2023, 1, 1 + i)
        test_dt = datetime.combine(test_date, datetime.min.time())
        extractor._update_resource_health("contratos", test_dt, rows_extracted=0)

    with duckdb.connect(str(temp_db)) as con:
        health = con.execute(
            "SELECT consecutive_empty_days, status FROM baliza_state.resource_health WHERE resource = 'contratos'"
        ).fetchone()

        assert health[0] == 7
        assert health[1] == "stalled"

def test_last_nonempty_date_preserved(extractor, temp_db):
    date_with_data = date(2023, 1, 1)
    empty_date_1 = date(2023, 1, 2)
    empty_date_2 = date(2023, 1, 3)
    
    dt_with_data = datetime.combine(date_with_data, datetime.min.time())
    dt_empty1 = datetime.combine(empty_date_1, datetime.min.time())
    dt_empty2 = datetime.combine(empty_date_2, datetime.min.time())

    # Initial run with data
    extractor._update_resource_health("contratos", dt_with_data, rows_extracted=50)

    # Followed by empties
    extractor._update_resource_health("contratos", dt_empty1, rows_extracted=0)
    extractor._update_resource_health("contratos", dt_empty2, rows_extracted=0)

    with duckdb.connect(str(temp_db)) as con:
        health = con.execute(
            "SELECT consecutive_empty_days, last_nonempty_date FROM baliza_state.resource_health WHERE resource = 'contratos'"
        ).fetchone()

        assert health[0] == 2
        assert health[1] == date_with_data
