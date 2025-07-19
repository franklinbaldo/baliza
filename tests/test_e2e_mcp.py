"""E2E Test Suite for BALIZA MCP Server

Tests the MCP server functionality against real database.
Validates AI-powered data analysis capabilities.
"""

import json

import pytest
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from baliza.mcp_server import app
from baliza.pncp_writer import BALIZA_DB_PATH


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_server_initialization():
    """E2E test: Verify MCP server can be initialized"""
    # Test that the FastMCP app is properly configured
    assert app is not None, "MCP server app not initialized"

    # Check that the server is a FastMCP instance
    assert app.__class__.__name__ == "FastMCP", "MCP server not a FastMCP instance"


@pytest.mark.e2e
@pytest.mark.asyncio
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
async def test_e2e_mcp_available_datasets():
    """E2E test: Verify MCP server can list available datasets"""
    # Only run if database exists
    if not BALIZA_DB_PATH.exists():
        pytest.skip("Database file not found - run extraction first")

    # Import the logic function directly for testing
    from baliza.mcp_server import _available_datasets_logic

    result = await _available_datasets_logic()

    # Should return a JSON string with dataset information
    assert isinstance(result, str), "Available datasets should return string"

    # Should be valid JSON
    datasets = json.loads(result)
    assert isinstance(datasets, list), "Datasets should be a list"

    # Should have expected dataset structure
    if datasets:  # Only check if datasets exist
        dataset = datasets[0]
        assert "name" in dataset, "Dataset should have name"
        assert "description" in dataset, "Dataset should have description"


@pytest.mark.e2e
@pytest.mark.asyncio
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
async def test_e2e_mcp_dataset_schema():
    """E2E test: Verify MCP server can describe dataset schemas"""
    # Only run if database exists
    if not BALIZA_DB_PATH.exists():
        pytest.skip("Database file not found - run extraction first")

    # Import the logic function directly for testing
    from baliza.mcp_server import _dataset_schema_logic

    # Test with a known table
    try:
        result = await _dataset_schema_logic("pncp_raw_responses")

        # Should return a JSON string with schema information
        assert isinstance(result, str), "Dataset schema should return string"

        # Should be valid JSON
        schema = json.loads(result)
        assert isinstance(schema, dict), "Schema should be a dictionary"

        # Should have expected schema structure
        assert "table_name" in schema, "Schema should have table_name"
        assert "columns" in schema, "Schema should have columns"

        # Columns should be a list
        assert isinstance(schema["columns"], list), "Columns should be a list"

        # Each column should have expected structure
        if schema["columns"]:
            column = schema["columns"][0]
            assert "name" in column, "Column should have name"
            assert "type" in column, "Column should have type"

    except Exception as e:
        # If table doesn't exist, that's also a valid test result
        assert "not found" in str(e).lower() or "does not exist" in str(e).lower(), (
            f"Unexpected error: {e}"
        )


@pytest.mark.e2e
@pytest.mark.asyncio
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=30),
    reraise=True,
)
async def test_e2e_mcp_sql_query_execution():
    """E2E test: Verify MCP server can execute SQL queries safely"""
    # Only run if database exists
    if not BALIZA_DB_PATH.exists():
        pytest.skip("Database file not found - run extraction first")

    # Import the logic function directly for testing
    from baliza.mcp_server import _execute_sql_query_logic

    # Test basic SELECT query
    query = "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'psa'"

    result = await _execute_sql_query_logic(query)

    # Should return a JSON string with query results
    assert isinstance(result, str), "SQL query should return string"

    # Should be valid JSON
    query_result = json.loads(result)
    assert isinstance(query_result, dict), "Query result should be a dictionary"

    # Should have expected result structure
    assert "columns" in query_result, "Query result should have columns"
    assert "rows" in query_result, "Query result should have rows"
    assert "row_count" in query_result, "Query result should have row_count"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_sql_query_security():
    """E2E test: Verify MCP server blocks dangerous SQL operations"""
    # Only run if database exists
    if not BALIZA_DB_PATH.exists():
        pytest.skip("Database file not found - run extraction first")

    # Import the logic function directly for testing
    from baliza.mcp_server import _execute_sql_query_logic

    # Test that dangerous operations are blocked
    dangerous_queries = [
        "DROP TABLE psa.pncp_raw_responses",
        "DELETE FROM psa.pncp_raw_responses",
        "UPDATE psa.pncp_raw_responses SET response_body = 'hacked'",
        "INSERT INTO psa.pncp_raw_responses VALUES ('test')",
        "CREATE TABLE malicious_table (id INT)",
        "ALTER TABLE psa.pncp_raw_responses ADD COLUMN malicious_col TEXT",
    ]

    for query in dangerous_queries:
        with pytest.raises(Exception) as exc_info:
            await _execute_sql_query_logic(query)

        # Should raise an exception for dangerous operations
        error_msg = str(exc_info.value).lower()
        assert any(
            word in error_msg
            for word in ["not allowed", "forbidden", "denied", "select only"]
        ), f"Dangerous query should be blocked: {query}"


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_complex_query():
    """E2E test: Verify MCP server can handle complex analytical queries"""
    # Only run if database exists and has data
    if not BALIZA_DB_PATH.exists():
        pytest.skip("Database file not found - run extraction first")

    # Import the logic function directly for testing
    from baliza.mcp_server import _execute_sql_query_logic

    # Test complex query with JOINs and aggregations
    query = """
    SELECT
        t.endpoint_name,
        COUNT(*) as task_count,
        SUM(CASE WHEN t.status = 'COMPLETE' THEN 1 ELSE 0 END) as completed_tasks,
        AVG(t.total_pages) as avg_pages
    FROM psa.pncp_extraction_tasks t
    GROUP BY t.endpoint_name
    ORDER BY task_count DESC
    LIMIT 10
    """

    try:
        result = await _execute_sql_query_logic(query)

        # Should return valid JSON
        query_result = json.loads(result)
        assert isinstance(query_result, dict), (
            "Complex query should return valid result"
        )

        # Should have expected structure
        assert "columns" in query_result, "Result should have columns"
        assert "rows" in query_result, "Result should have rows"

        # Columns should include the expected fields
        columns = query_result["columns"]
        expected_columns = [
            "endpoint_name",
            "task_count",
            "completed_tasks",
            "avg_pages",
        ]
        for col in expected_columns:
            assert col in columns, f"Missing expected column: {col}"

    except Exception as e:
        # If no data exists, that's also a valid test outcome
        error_msg = str(e).lower()
        if "no such table" in error_msg or "does not exist" in error_msg:
            pytest.skip("Required tables not found - run extraction first")
        else:
            raise


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_mcp_enum_metadata():
    """E2E test: Verify MCP server can access enum metadata"""
    # Test enum metadata functionality
    from baliza.enums import get_all_enum_metadata

    metadata = get_all_enum_metadata()

    # Should return a dictionary
    assert isinstance(metadata, dict), "Enum metadata should be a dictionary"

    # Should have expected structure
    if metadata:  # Only check if metadata exists
        for enum_name, enum_data in metadata.items():
            assert isinstance(enum_name, str), "Enum name should be string"
            assert isinstance(enum_data, dict), "Enum data should be dictionary"
            assert "values" in enum_data, "Enum data should have values"
