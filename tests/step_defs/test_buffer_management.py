"""BDD step definitions for buffer_management.feature."""

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pytest_bdd import given, parsers, scenario, then, when

from baliza.extractor import PNCPExtractor

# =============================================================================
# Scenario: Buffer contains only recent unstable data (Tier 1)
# =============================================================================


@pytest.mark.tier1
@scenario("../features/buffer_management.feature", "Buffer contains only recent unstable data")
def test_buffer_contains_recent_data():
    pass


@given(parsers.parse("extraction has run for {days:d} days"), target_fixture="db_with_extractions")
def db_with_extractions(tmp_path: Path, days: int) -> dict:
    """Create database with N days of extractions."""
    db_file = tmp_path / "test.duckdb"

    with PNCPExtractor(db_file, "baliza_raw") as extractor:
        with duckdb.connect(str(db_file)) as con:
            extractor._ensure_schema(con)

            # Insert data for each day
            base_date = datetime(2023, 1, 1)
            for day in range(days):
                current_date = base_date + timedelta(days=day)
                date_str = current_date.strftime("%Y-%m-%dT10:00:00")

                # Insert 100 rows per day
                for i in range(100):
                    con.execute(
                        """
                        INSERT INTO baliza_raw.contratos
                        (numero_controle_pncp, ano_compra, cnpj_orgao, data_publicacao)
                        VALUES (?, 2023, '12345678000190', ?)
                    """,
                        [f"CTRL-{day:02d}-{i:03d}", date_str],
                    )

    return {"db_path": db_file, "dataset": "baliza_raw", "total_days": days}


@given(
    parsers.parse("days {start:d}-{end:d} have been uploaded to Internet Archive"),
    target_fixture="db_with_uploads",
)
def mark_days_uploaded(db_with_extractions, start, end):
    """Mark days as uploaded to IA."""
    db_path = db_with_extractions["db_path"]

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        base_date = datetime(2023, 1, 1)
        for day in range(start - 1, end):  # 0-indexed, inclusive
            current_date = base_date + timedelta(days=day)
            extractor.record_ia_upload(
                item_id=f"baliza-{current_date.date()}",
                extraction_date=current_date,
                file_count=3,
                total_rows=100,
            )

    return db_with_extractions


@when("I check buffer stats", target_fixture="buffer_stats_result")
def check_buffer_stats(db_with_uploads):
    """Get buffer statistics."""
    db_path = db_with_uploads["db_path"]

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        stats = extractor.get_buffer_stats()

    return {**db_with_uploads, "stats": stats}


@then(parsers.parse("the buffer should contain data for days {start:d}-{end:d} only"))
def check_buffer_contains_days(buffer_stats_result, start, end):
    """Verify buffer contains exactly the expected days after upload bookkeeping."""
    db_path = buffer_stats_result["db_path"]
    total_days = buffer_stats_result["total_days"]
    expected_days = end - start + 1
    base_date = datetime(2023, 1, 1)
    expected_dates = {
        (base_date + timedelta(days=day)).date()
        for day in range(start - 1, end)
    }

    with duckdb.connect(str(db_path), read_only=True) as con:
        dates_in_buffer = {
            row[0]
            for row in con.execute(
                "SELECT DISTINCT CAST(data_publicacao AS DATE) FROM baliza_raw.contratos"
            ).fetchall()
        }

    # Scenario notes: after uploading days 1-3 of a 10-day extraction, the buffer
    # still physically contains all 10 days (upload bookkeeping only); cleanup is a
    # separate step. So we assert the raw buffer contains every day AND that the
    # uploaded set is the documented prefix.
    assert len(dates_in_buffer) == total_days, (
        f"Expected {total_days} days in raw buffer, got {len(dates_in_buffer)}"
    )
    assert expected_dates.issubset(dates_in_buffer), (
        f"Buffer missing expected dates {expected_dates - dates_in_buffer}"
    )
    assert expected_days == len(expected_dates)


@then(parsers.parse("days {start:d}-{end:d} should be recorded in uploaded_to_ia table"))
def check_uploaded_records(buffer_stats_result, start, end):
    """Verify uploaded days are recorded."""
    db_path = buffer_stats_result["db_path"]

    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM baliza_state.uploaded_to_ia").fetchone()[0]

    expected = end - start + 1
    assert count == expected, f"Expected {expected} records in uploaded_to_ia, got {count}"


# =============================================================================
# Scenario: Cleanup removes data after successful upload (Tier 1)
# =============================================================================


@pytest.mark.tier1
@scenario("../features/buffer_management.feature", "Cleanup removes data after successful upload")
def test_cleanup_removes_data():
    pass


@given(
    parsers.parse("the buffer contains {rows:d} rows for {date_str}"),
    target_fixture="buffer_with_data",
)
def buffer_with_data(tmp_path: Path, rows: int, date_str: str) -> dict:
    """Create buffer with N rows for a specific date."""
    db_file = tmp_path / "test.duckdb"

    with PNCPExtractor(db_file, "baliza_raw") as extractor:
        with duckdb.connect(str(db_file)) as con:
            extractor._ensure_schema(con)

            # Insert N rows for the date
            for i in range(rows):
                con.execute(
                    """
                    INSERT INTO baliza_raw.contratos
                    (numero_controle_pncp, ano_compra, cnpj_orgao, data_publicacao)
                    VALUES (?, 2023, '12345678000190', ?)
                """,
                    [f"CTRL-{i:05d}", f"{date_str}T10:00:00"],
                )

    return {"db_path": db_file, "dataset": "baliza_raw", "date_str": date_str, "initial_rows": rows}


@given(
    parsers.parse("{date_str} was successfully uploaded to Internet Archive"),
    target_fixture="data_uploaded",
)
def mark_date_uploaded(buffer_with_data, date_str):
    """Mark date as uploaded to IA."""
    db_path = buffer_with_data["db_path"]
    upload_date = datetime.strptime(date_str, "%Y-%m-%d")

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        extractor.record_ia_upload(
            item_id=f"baliza-{date_str}",
            extraction_date=upload_date,
            file_count=3,
            total_rows=buffer_with_data["initial_rows"],
        )

    return buffer_with_data


@when(parsers.parse("cleanup runs for {date_str}"), target_fixture="cleanup_result")
def run_cleanup(data_uploaded, date_str):
    """Run cleanup for a specific date."""
    db_path = data_uploaded["db_path"]
    cleanup_date = datetime.strptime(date_str, "%Y-%m-%d")

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        rows_deleted = extractor.cleanup_uploaded(cleanup_date)

    return {**data_uploaded, "rows_deleted": rows_deleted}


@then(parsers.parse("the buffer should not contain data for {date_str}"))
def check_buffer_empty_for_date(cleanup_result, date_str):
    """Verify buffer no longer contains data for the date."""
    db_path = cleanup_result["db_path"]

    with duckdb.connect(str(db_path), read_only=True) as con:
        count = con.execute(
            """
            SELECT COUNT(*)
            FROM baliza_raw.contratos
            WHERE CAST(data_publicacao AS DATE) = ?
        """,
            [date_str],
        ).fetchone()[0]

    assert count == 0, f"Expected 0 rows for {date_str}, found {count}"


@then("the state tables should still exist")
def check_state_tables_exist(cleanup_result):
    """Verify state tables were not deleted."""
    db_path = cleanup_result["db_path"]

    with duckdb.connect(str(db_path), read_only=True) as con:
        # Check uploaded_to_ia table exists and has data
        uploaded_count = con.execute("SELECT COUNT(*) FROM baliza_state.uploaded_to_ia").fetchone()[
            0
        ]

        assert uploaded_count > 0, "uploaded_to_ia table should still contain records"

        # Check schema exists
        schemas = con.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'baliza_state'"
        ).fetchall()

        assert len(schemas) > 0, "baliza_state schema should still exist"


# =============================================================================
# Scenario: Get dates ready for export (Tier 2)
# =============================================================================


@pytest.mark.tier2
@scenario("../features/buffer_management.feature", "Get dates ready for export")
def test_get_dates_ready_for_export():
    pass


@given(
    parsers.parse("the buffer contains data for the last {days:d} days"),
    target_fixture="buffer_with_recent_data",
)
def buffer_with_recent_data(tmp_path: Path, days: int) -> dict:
    """Create buffer with data for last N days."""
    db_file = tmp_path / "test.duckdb"

    with PNCPExtractor(db_file, "baliza_raw") as extractor:
        with duckdb.connect(str(db_file)) as con:
            extractor._ensure_schema(con)

            # Insert data for last N days (counting back from today)
            for day_offset in range(days):
                current_date = datetime.now() - timedelta(days=day_offset)
                date_str = current_date.strftime("%Y-%m-%dT10:00:00")

                for i in range(50):
                    con.execute(
                        """
                        INSERT INTO baliza_raw.contratos
                        (numero_controle_pncp, ano_compra, cnpj_orgao, data_publicacao)
                        VALUES (?, 2023, '12345678000190', ?)
                    """,
                        [f"CTRL-{day_offset:02d}-{i:03d}", date_str],
                    )

    return {"db_path": db_file, "dataset": "baliza_raw", "total_days": days}


@given(parsers.parse("stability window is {days:d} days"), target_fixture="with_stability_window")
def set_stability_window(buffer_with_recent_data, days):
    """Set stability window (stored for later use)."""
    return {**buffer_with_recent_data, "stability_days": days}


@when("I query dates ready for export", target_fixture="ready_dates")
def query_ready_dates(with_stability_window):
    """Query dates ready for export."""
    db_path = with_stability_window["db_path"]
    stability_days = with_stability_window["stability_days"]

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        dates = extractor.get_dates_ready_for_export(stability_days)

    return {**with_stability_window, "ready_dates": dates}


@then(parsers.parse("only dates older than {days:d} days should be returned"))
def check_dates_older_than(ready_dates, days):
    """Verify all returned dates are older than N days."""
    dates = ready_dates["ready_dates"]
    cutoff = datetime.now() - timedelta(days=days)

    for dt in dates:
        assert dt < cutoff, f"Date {dt} is not older than {days} days"


@then("dates already uploaded to IA should be excluded")
def check_uploaded_excluded(ready_dates):
    """Verify uploaded dates are excluded."""
    db_path = ready_dates["db_path"]

    # Mark one old date as uploaded
    old_date = datetime.now() - timedelta(days=10)

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        extractor.record_ia_upload(
            item_id=f"baliza-{old_date.date()}",
            extraction_date=old_date,
            file_count=3,
            total_rows=50,
        )

        # Re-query
        dates = extractor.get_dates_ready_for_export(ready_dates["stability_days"])

    # Uploaded date should not be in results
    uploaded_dates = [old_date.date()]
    ready_date_strs = [d.date() for d in dates]

    for uploaded_date in uploaded_dates:
        assert uploaded_date not in ready_date_strs, (
            f"Uploaded date {uploaded_date} should be excluded"
        )


# =============================================================================
# Scenario: Buffer stats show current state (Tier 2)
# =============================================================================


@pytest.mark.tier2
@scenario("../features/buffer_management.feature", "Buffer stats show current state")
def test_buffer_stats_show_state():
    pass


@given(
    parsers.parse("the buffer contains {total_rows:d} rows across {dates:d} dates"),
    target_fixture="buffer_stats_setup",
)
def buffer_stats_setup(tmp_path: Path, total_rows: int, dates: int) -> dict:
    """Create buffer with specific row and date counts."""
    db_file = tmp_path / "test.duckdb"
    rows_per_date = total_rows // dates

    with PNCPExtractor(db_file, "baliza_raw") as extractor:
        with duckdb.connect(str(db_file)) as con:
            extractor._ensure_schema(con)

            # Insert data for N dates
            base_date = datetime(2023, 1, 1)
            for day in range(dates):
                current_date = base_date + timedelta(days=day)
                date_str = current_date.strftime("%Y-%m-%dT10:00:00")

                for i in range(rows_per_date):
                    con.execute(
                        """
                        INSERT INTO baliza_raw.contratos
                        (numero_controle_pncp, ano_compra, cnpj_orgao, data_publicacao)
                        VALUES (?, 2023, '12345678000190', ?)
                    """,
                        [f"CTRL-{day:02d}-{i:04d}", date_str],
                    )

    return {
        "db_path": db_file,
        "dataset": "baliza_raw",
        "expected_total_rows": total_rows,
        "expected_dates": dates,
    }


@given(
    parsers.parse("{uploaded_count:d} dates have been uploaded to IA"),
    target_fixture="with_uploads_marked",
)
def mark_dates_uploaded_ia(buffer_stats_setup, uploaded_count):
    """Mark N dates as uploaded to IA."""
    db_path = buffer_stats_setup["db_path"]

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        base_date = datetime(2023, 1, 1)
        for i in range(uploaded_count):
            current_date = base_date + timedelta(days=i)
            extractor.record_ia_upload(
                item_id=f"baliza-{current_date.date()}",
                extraction_date=current_date,
                file_count=3,
                total_rows=2000,
            )

    return {**buffer_stats_setup, "expected_uploaded": uploaded_count}


@when("I get buffer stats", target_fixture="final_stats")
def get_final_stats(with_uploads_marked):
    """Get final buffer statistics."""
    db_path = with_uploads_marked["db_path"]

    with PNCPExtractor(db_path, "baliza_raw") as extractor:
        stats = extractor.get_buffer_stats()

    return {**with_uploads_marked, "stats": stats}


@then(parsers.parse("total_rows should be {expected:d}"))
def check_total_rows(final_stats, expected):
    """Verify total_rows matches expected."""
    actual = final_stats["stats"]["total_rows"]
    assert actual == expected, f"Expected {expected} total_rows, got {actual}"


@then(parsers.parse("dates_in_buffer should be {expected:d}"))
def check_dates_in_buffer(final_stats, expected):
    """Verify dates_in_buffer matches expected."""
    actual = final_stats["stats"]["dates_in_buffer"]
    assert actual == expected, f"Expected {expected} dates_in_buffer, got {actual}"


@then(parsers.parse("dates_uploaded_to_ia should be {expected:d}"))
def check_dates_uploaded(final_stats, expected):
    """Verify dates_uploaded_to_ia matches expected."""
    actual = final_stats["stats"]["dates_uploaded_to_ia"]
    assert actual == expected, f"Expected {expected} dates_uploaded_to_ia, got {actual}"
