from datetime import datetime

import duckdb
import pytest

from baliza.daily_exporter import DailyExporter
from baliza.extractor import PNCPExtractor


@pytest.fixture
def temp_db(tmp_path):
    return tmp_path / "test.duckdb"

@pytest.fixture
def extractor(temp_db):
    return PNCPExtractor(db_path=temp_db)

def populate_db(extractor):
    # Insert some dummy data directly using the normalized schema
    with duckdb.connect(str(extractor.db_path)) as con:
        extractor._ensure_schema(con)

        def insert_row(id, date_str):
            con.execute(f"""
                INSERT INTO {extractor.dataset}.contratos (
                    numero_controle_pncp,
                    data_publicacao,
                    cnpj_orgao,
                    codigo_unidade,
                    valor_inicial
                ) VALUES (
                    ?, CAST(? AS TIMESTAMP), '12345678000199', '001', 100.00
                )
            """, [id, date_str])

        # Date 1: 2023-01-01
        insert_row("1", "2023-01-01 10:00:00")
        insert_row("2", "2023-01-01 23:59:59")

        # Date 2: 2023-01-02
        insert_row("3", "2023-01-02 00:00:00")

        # Date 3: 2023-01-03
        insert_row("4", "2023-01-03 12:00:00")

def test_cleanup_uploaded(extractor, temp_db):
    populate_db(extractor)

    target_date = datetime(2023, 1, 1)

    # Should delete 2 rows
    deleted = extractor.cleanup_uploaded(target_date)
    assert deleted == 2

    with duckdb.connect(str(temp_db)) as con:
        count = con.execute(f"SELECT COUNT(*) FROM {extractor.dataset}.contratos").fetchone()[0]
        assert count == 2  # 2 rows remaining (dates 2 and 3)

        # Verify specific IDs remaining
        ids = con.execute(
            f"SELECT numero_controle_pncp FROM {extractor.dataset}.contratos ORDER BY numero_controle_pncp"
        ).fetchall()
        assert [r[0] for r in ids] == ["3", "4"]

def test_get_dates_ready_for_export(extractor, temp_db):
    populate_db(extractor)

    # By default stability_days=7. Current date is effectively "now".
    # The populated dates are Jan 2023, so they are definitely ready.
    dates = extractor.get_dates_ready_for_export(stability_days=1)

    assert len(dates) == 3
    assert dates[0] == datetime(2023, 1, 1)
    assert dates[1] == datetime(2023, 1, 2)
    assert dates[2] == datetime(2023, 1, 3)


def test_daily_exporter(temp_db, tmp_path):
    # Setup extractor to create schema and data
    extractor = PNCPExtractor(db_path=temp_db)
    populate_db(extractor)

    exporter = DailyExporter(db_path=temp_db)
    output_dir = tmp_path / "output"

    target_date = datetime(2023, 1, 1).date()
    stats = exporter.export(target_date, output_dir)

    assert stats["tables"]["contratos"]["row_count"] == 2
    assert (output_dir / "2023-01-01" / "contratos.parquet").exists()
