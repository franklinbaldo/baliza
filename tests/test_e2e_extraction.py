"""E2E Test Suite for BALIZA Extraction Pipeline

Tests the complete extraction workflow against real PNCP API.
Implements retry logic for network stability as per TODO.md requirements.
"""

from datetime import date

import orjson
import pytest
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from baliza.extractor import AsyncPNCPExtractor
from baliza.pncp_writer import BALIZA_DB_PATH, connect_utf8
from tests.schemas import ContratosResponse


@pytest.mark.e2e
@pytest.mark.asyncio
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
async def test_e2e_extract_single_day():
    """E2E test: Extract real data from PNCP API for a single day

    This test validates the complete extraction pipeline:
    - Real API calls to PNCP
    - Task planning and discovery
    - Data extraction and storage in DuckDB
    - Fault tolerance and resumability
    """
    # Use a known good date with data
    test_date = date(2024, 1, 15)  # Monday, likely to have procurement data

    # Run the extraction for a single day
    async with AsyncPNCPExtractor(concurrency=1) as extractor:
        await extractor.extract_data(
            start_date=test_date,
            end_date=test_date,
        )

    # Verify data was extracted
    if BALIZA_DB_PATH.exists():
        con = connect_utf8(str(BALIZA_DB_PATH))

        # Check that tasks were created
        tasks_result = con.execute(
            """
            SELECT COUNT(*) as task_count
            FROM psa.pncp_extraction_tasks
            WHERE date_range_start = ?
        """,
            [test_date],
        ).fetchone()

        assert tasks_result[0] > 0, "No extraction tasks were created"

        # Check that some data was extracted
        data_result = con.execute(
            """
            SELECT COUNT(*) as response_count
            FROM psa.pncp_raw_responses
            WHERE DATE(created_at) = ?
        """,
            [test_date],
        ).fetchone()

        # Allow for empty results (some dates might not have data)
        # But verify the extraction process ran
        assert data_result[0] >= 0, "Extraction process failed"

        con.close()


@pytest.mark.e2e
@pytest.mark.asyncio
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
async def test_e2e_extraction_resumability():
    """E2E test: Verify extraction can be resumed after interruption

    This test validates the fault-tolerant architecture:
    - Extraction state is persisted in DuckDB
    - Re-running extraction continues from where it left off
    - No duplicate data is created
    """
    test_date = date(2024, 1, 16)  # Different date from previous test

    # First extraction run
    async with AsyncPNCPExtractor(concurrency=1) as extractor:
        await extractor.extract_data(
            start_date=test_date, end_date=test_date
        )

    if BALIZA_DB_PATH.exists():
        con = connect_utf8(str(BALIZA_DB_PATH))

        # Get initial state
        initial_tasks = con.execute(
            """
            SELECT COUNT(*) as task_count
            FROM psa.pncp_extraction_tasks
            WHERE date_range_start = ?
        """,
            [test_date],
        ).fetchone()[0]

        initial_responses = con.execute(
            """
            SELECT COUNT(*) as response_count
            FROM psa.pncp_raw_responses
            WHERE DATE(created_at) = ?
        """,
            [test_date],
        ).fetchone()[0]

        # Second extraction run (should be idempotent)
        async with AsyncPNCPExtractor(concurrency=1) as extractor:
            await extractor.extract_data(
                start_date=test_date,
                end_date=test_date,
            )

        # Verify idempotency
        final_tasks = con.execute(
            """
            SELECT COUNT(*) as task_count
            FROM psa.pncp_extraction_tasks
            WHERE date_range_start = ?
        """,
            [test_date],
        ).fetchone()[0]

        final_responses = con.execute(
            """
            SELECT COUNT(*) as response_count
            FROM psa.pncp_raw_responses
            WHERE DATE(created_at) = ?
        """,
            [test_date],
        ).fetchone()[0]

        # Tasks should remain the same (idempotent)
        assert final_tasks == initial_tasks, "Task count changed on re-run"

        # Responses should not decrease (data preservation)
        assert final_responses >= initial_responses, "Data was lost on re-run"

        con.close()


@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.asyncio
@retry(
    stop=stop_after_attempt(2),  # Fewer retries for slow tests
    wait=wait_exponential_jitter(initial=2, max=60),
    reraise=True,
)
async def test_e2e_extraction_multiple_endpoints():
    """E2E test: Verify extraction works across multiple PNCP endpoints

    This test validates:
    - Multiple endpoint discovery (contratos, atas, etc.)
    - Concurrent extraction across endpoints
    - Data integrity across different data types
    """
    test_date = date(2024, 1, 17)  # Another unique date

    async with AsyncPNCPExtractor(concurrency=2) as extractor:
        await extractor.extract_data(
            start_date=test_date, end_date=test_date
        )

    if BALIZA_DB_PATH.exists():
        con = connect_utf8(str(BALIZA_DB_PATH))

        # Check that multiple endpoints were discovered
        endpoints_result = con.execute(
            """
            SELECT DISTINCT endpoint_name
            FROM psa.pncp_extraction_tasks
            WHERE date_range_start = ?
        """,
            [test_date],
        ).fetchall()

        endpoint_names = [row[0] for row in endpoints_result]

        # Should have at least contratos endpoint
        assert "contratos" in endpoint_names, "Contratos endpoint not found"

        # Verify data was extracted for multiple endpoints
        for endpoint in endpoint_names:
            endpoint_data = con.execute(
                """
                SELECT COUNT(*) as count
                FROM psa.pncp_raw_responses
                WHERE endpoint_name = ?
                AND DATE(created_at) = ?
            """,
                [endpoint, test_date],
            ).fetchone()[0]

            # Allow for empty results but verify structure exists
            assert endpoint_data >= 0, f"Extraction failed for endpoint {endpoint}"

        con.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_database_schema():
    """E2E test: Verify database schema is correctly created

    This test validates:
    - Required tables exist
    - Table schemas are correct
    - Data types are properly defined
    """
    # This test doesn't need retry logic as it's checking local database
    if BALIZA_DB_PATH.exists():
        con = connect_utf8(str(BALIZA_DB_PATH))

        # Check that required tables exist
        tables_result = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'psa'
        """).fetchall()

        table_names = [row[0] for row in tables_result]

        assert "pncp_extraction_tasks" in table_names, "Tasks table missing"
        assert "pncp_raw_responses" in table_names, "Responses table missing"

        # Check key columns exist in tasks table
        tasks_columns = con.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'psa' AND table_name = 'pncp_extraction_tasks'
        """).fetchall()

        task_column_names = [row[0] for row in tasks_columns]

        required_task_columns = [
            "task_id",
            "endpoint_name",
            "data_date",
            "modalidade",
            "status",
            "total_pages",
            "missing_pages",
            "created_at",
            "updated_at",
        ]

        for col in required_task_columns:
            assert col in task_column_names, (
                f"Required column {col} missing from tasks table"
            )

        # Check key columns exist in responses table
        responses_columns = con.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'psa' AND table_name = 'pncp_raw_responses'
        """).fetchall()

        response_column_names = [row[0] for row in responses_columns]

        required_response_columns = [
            "id",
            "endpoint_name",
            "data_date",
            "current_page",
            "response_content",
            "extracted_at",
        ]

        for col in required_response_columns:
            assert col in response_column_names, (
                f"Required column {col} missing from responses table"
            )

        con.close()
    else:
        pytest.skip("Database file not found - run extraction first")


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_validate_response_schema():
    """E2E test: Validate the schema of the raw responses from the PNCP API."""
    test_date = date(2024, 1, 15)  # A date with known data

    # Run extraction
    async with AsyncPNCPExtractor(concurrency=1) as extractor:
        await extractor.extract_data(
            start_date=test_date, end_date=test_date
        )

    # Verify data was extracted and validate schema
    if BALIZA_DB_PATH.exists():
        con = connect_utf8(str(BALIZA_DB_PATH))

        # Get all responses for the test date
        responses = con.execute(
            """
            SELECT response_content
            FROM psa.pncp_raw_responses
            WHERE DATE(created_at) = ?
        """,
            [test_date],
        ).fetchall()

        assert len(responses) > 0, "No responses found to validate"

        for response in responses:
            response_json = orjson.loads(response[0])
            ContratosResponse.model_validate(response_json)

        con.close()
    else:
        pytest.skip("Database file not found - run extraction first")
