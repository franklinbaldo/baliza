import asyncio
import uuid
from datetime import date

import duckdb
import pytest

from src.baliza.pncp_extractor import AsyncPNCPExtractor


@pytest.fixture
def extractor():
    """Fixture to provide an instance of AsyncPNCPExtractor with an in-memory DuckDB."""
    extractor = AsyncPNCPExtractor(concurrency=1)
    # Override the database connection to use an in-memory database for testing
    extractor.conn = duckdb.connect(':memory:')
    extractor._init_database()  # Re-initialize the database in memory
    yield extractor
    extractor.conn.close()


@pytest.mark.asyncio
async def test_deduplication_of_response_content(extractor: AsyncPNCPExtractor):
    """
    Tests that identical response content is stored only once in the pncp_response_content table.
    """
    # Sample response data
    response_content = '{"key": "value"}'
    content_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, response_content)

    # Simulate two responses with the same content
    responses = [
        {
            "endpoint_url": "http://example.com/1",
            "endpoint_name": "test_endpoint",
            "request_parameters": {},
            "response_code": 200,
            "response_content": response_content,
            "response_headers": {},
            "data_date": date(2023, 1, 1),
            "run_id": "test_run",
            "total_records": 1,
            "total_pages": 1,
            "current_page": 1,
            "page_size": 1,
        },
        {
            "endpoint_url": "http://example.com/2",
            "endpoint_name": "test_endpoint",
            "request_parameters": {},
            "response_code": 200,
            "response_content": response_content,
            "response_headers": {},
            "data_date": date(2023, 1, 2),
            "run_id": "test_run",
            "total_records": 1,
            "total_pages": 1,
            "current_page": 1,
            "page_size": 1,
        },
    ]

    # Use the batch store method to insert the data
    extractor._batch_store_responses(responses)

    # Check that the content is stored only once
    count = extractor.conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_response_content WHERE response_content_uuid = ?",
        [str(content_uuid)]
    ).fetchone()[0]
    assert count == 1

    # Check that there are two entries in the raw responses table
    count = extractor.conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_content_uuid = ?",
        [str(content_uuid)]
    ).fetchone()[0]
    assert count == 2
