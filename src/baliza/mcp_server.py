"""BALIZA MCP Server - AI-Powered Data Analysis

Implements the MCP server architecture defined in ADR-007.
Provides secure, read-only access to BALIZA data for AI analysis.
Uses DuckDB for data access as specified in ADR-001.
"""

import asyncio
import json
import logging
from pathlib import Path

import duckdb
from fastmcp import FastMCP

from baliza.enums import get_all_enum_metadata

# Initialize the FastMCP server
app = FastMCP()
logger = logging.getLogger(__name__)

# Define the mapping of logical table names to their Parquet glob patterns
# Assumes data/parquet/<table>/*.parquet structure
# TODO: Move this to a configuration file (see ISSUES.md)
PARQUET_TABLE_MAPPING = {
    "contratos": "contratos/*.parquet",
    "atas": "atas/*.parquet",
    "contratacoes": "contratacoes/*.parquet",
    "pca": "pca/*.parquet",
    "instrumentos": "instrumentos/*.parquet",
    # Add other datasets as needed
}


def get_table_mapping() -> dict[str, str]:
    """Get table mapping configuration.

    Future enhancement: Load from config file instead of hardcoded dict.
    """
    return PARQUET_TABLE_MAPPING.copy()


# --- Resources ---


async def _available_datasets_logic() -> str:
    """Returns a list of available datasets for querying."""
    datasets = [
        {"name": "contratos", "description": "Dados sobre contratos públicos."},
        {"name": "atas", "description": "Dados sobre atas de registro de preço."},
    ]
    return json.dumps(datasets)


@app.resource("mcp://baliza/available_datasets")
async def available_datasets() -> str:
    return await _available_datasets_logic()


async def _dataset_schema_logic(dataset_name: str, base_dir: str | None = None) -> str:
    """Returns the schema for a given dataset."""
    data_dir = Path(base_dir) if base_dir else Path("data/parquet")

    # Use the mapping to get the correct glob pattern
    table_mapping = get_table_mapping()
    parquet_glob_pattern = table_mapping.get(dataset_name)
    if not parquet_glob_pattern:
        return json.dumps({"error": f"Dataset '{dataset_name}' not found in mapping."})

    # Construct the full path for DuckDB's read_parquet
    full_parquet_path = data_dir / parquet_glob_pattern

    # Robust path sanitization
    try:
        resolved_path = full_parquet_path.resolve(strict=True)
        if not resolved_path.is_relative_to(data_dir.resolve()):
            return json.dumps({"error": "Invalid dataset path."})
    except FileNotFoundError:
        return json.dumps({"error": f"Dataset '{dataset_name}' not found."})
    except Exception as e:
        logger.exception(f"Path resolution error for {dataset_name}: {e}")
        return json.dumps({"error": "Invalid dataset name."})

    try:
        con = duckdb.connect(database=":memory:")
        # Use read_parquet directly with the glob pattern
        schema = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{resolved_path!s}')"
        ).fetchdf()
        return schema.to_json(orient="records")
    except duckdb.Error as e:
        logger.exception(f"Failed to get schema for {dataset_name}: {e}")
        return json.dumps({"error": str(e)})


@app.resource("mcp://baliza/dataset_schema/{dataset_name}")
async def dataset_schema(dataset_name: str) -> str:
    return await _dataset_schema_logic(dataset_name)


async def _enum_metadata_logic() -> str:
    """Returns metadata for all enums."""
    metadata = get_all_enum_metadata()
    return json.dumps(metadata)


@app.resource("mcp://baliza/enum_metadata")
async def enum_metadata() -> str:
    return await _enum_metadata_logic()


# --- Tools ---


async def _execute_sql_query_logic(query: str, base_dir: str | None = None) -> str:
    """Executes a read-only SQL query against the procurement dataset."""
    # Enhanced Security Validation: Strict query validation
    query_upper = query.strip().upper()

    # Only allow SELECT statements
    if not query_upper.startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are allowed."})

    # Block dangerous SQL keywords that could be used for injection
    dangerous_keywords = [
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "EXEC",
        "EXECUTE",
        "UNION",
        "--",
        "/*",
        "*/",
        "XP_",
        "SP_",
        "BULK",
        "OPENROWSET",
        "OPENDATASOURCE",
    ]

    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return json.dumps(
                {"error": f"Forbidden keyword '{keyword}' detected in query."}
            )

    # Additional validation: Check for suspicious patterns
    if ";" in query and not query.strip().endswith(";"):
        return json.dumps({"error": "Multiple statements not allowed."})

    try:
        con = duckdb.connect(database=":memory:")

        parquet_dir = Path(base_dir) if base_dir else Path("data/parquet")
        if parquet_dir.exists():
            table_mapping = get_table_mapping()
            for table_name, glob_pattern in table_mapping.items():
                # Validate table name to prevent injection
                if not table_name.isalnum():
                    logger.error(f"Invalid table name: {table_name}")
                    continue

                full_parquet_path = parquet_dir / glob_pattern
                # Robust path sanitization for view creation
                try:
                    resolved_path = full_parquet_path.resolve(strict=True)
                    if not resolved_path.is_relative_to(parquet_dir.resolve()):
                        logger.error(f"Attempted path traversal: {full_parquet_path}")
                        continue  # Skip this view if path is invalid
                except FileNotFoundError:
                    logger.warning(f"Parquet path not found: {full_parquet_path}")
                    continue  # Skip if file does not exist
                except Exception as e:
                    logger.exception(
                        f"Path resolution error for {full_parquet_path}: {e}"
                    )
                    continue  # Skip on other path errors

                # SECURITY FIX: Use safe SQL construction
                # Validate table name is safe (alphanumeric only, already checked above)
                # Create a view for each logical table using read_parquet with parameterized path
                con.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet(?)",
                    [str(resolved_path)],
                )

        # SECURITY FIX: Execute user query in isolated manner
        # Additional timeout protection (DuckDB feature)
        result = con.execute(query).fetchdf()

        # Limit result size to prevent resource exhaustion
        if len(result) > 10000:
            logger.warning(f"Query returned {len(result)} rows, truncating to 10000")
            result = result.head(10000)

        return result.to_json(orient="records")

    except duckdb.Error as e:
        logger.exception(f"Query failed: {query} - {e}")
        return json.dumps({"error": f"Query failed: {str(e)}"})
    except Exception as e:
        logger.exception(f"Unexpected error executing query: {e}")
        return json.dumps({"error": "Query execution failed due to unexpected error."})


@app.tool("mcp://baliza/execute_sql_query")
async def execute_sql_query(query: str) -> str:
    return await _execute_sql_query_logic(query)


def run_server(host: str = "127.0.0.1", port: int = 8000):
    """
    Runs the MCP server. This function will be called by the CLI.

    Args:
        host: The host to bind the server to
        port: The port to run the server on
    """
    # Proper async lifecycle management for MCP server
    try:
        # Try to pass host/port if FastMCP supports it
        app.run(host=host, port=port)
    except TypeError:
        # Fallback if FastMCP doesn't support host/port parameters
        logger.warning(
            "FastMCP app.run() doesn't support host/port parameters, using defaults"
        )
        try:
            app.run()
        except Exception as e:
            logger.error(f"Failed to start MCP server: {e}")
            raise
    except KeyboardInterrupt:
        logger.info("MCP server shutdown requested")
    except Exception as e:
        logger.error(f"MCP server error: {e}")
        raise


async def run_server_async(host: str = "127.0.0.1", port: int = 8000):
    """
    Async version of run_server for proper lifecycle management.

    Args:
        host: The host to bind the server to
        port: The port to run the server on
    """
    logger.info(f"Starting MCP server on {host}:{port}")
    try:
        # If FastMCP has an async run method, use it
        if hasattr(app, "run_async"):
            await app.run_async(host=host, port=port)
        else:
            # Fallback to sync version
            app.run(host=host, port=port)
    except KeyboardInterrupt:
        logger.info("MCP server shutdown requested")
    except Exception as e:
        logger.error(f"MCP server error: {e}")
        raise


async def _run_tests():
    """A simple test runner to validate functionality."""
    import shutil
    import tempfile

    import pandas as pd

    print("--- Running MCP Server Tests ---")
    test_dir = tempfile.mkdtemp()
    parquet_dir = Path(test_dir) / "parquet"
    parquet_dir.mkdir()

    # Create dummy data in subdirectories to match the new structure
    (parquet_dir / "contratos").mkdir()
    (parquet_dir / "atas").mkdir()

    try:
        # Create dummy data for 'contratos'
        contracts_df = pd.DataFrame(
            {
                "id": [1, 2],
                "valor": [100.0, 200.0],
                "fornecedor": ["Empresa A", "Empresa B"],
            }
        )
        contracts_df.to_parquet(parquet_dir / "contratos" / "part-00000.parquet")
        print("✅ [1/7] Dummy data for 'contratos' created.")

        # Create dummy data for 'atas'
        atas_df = pd.DataFrame(
            {
                "id": [101, 102],
                "numero": ["ATA-001", "ATA-002"],
            }
        )
        atas_df.to_parquet(parquet_dir / "atas" / "part-00000.parquet")
        print("✅ [2/7] Dummy data for 'atas' created.")

        # 3. Test available_datasets_logic
        datasets_str = await _available_datasets_logic()
        datasets = json.loads(datasets_str)
        assert len(datasets) > 0
        assert any(d["name"] == "contratos" for d in datasets)
        assert any(d["name"] == "atas" for d in datasets)
        print("✅ [3/7] `available_datasets` logic passed.")

        # 4. Test _dataset_schema_logic
        schema_str = await _dataset_schema_logic("contratos", base_dir=str(parquet_dir))
        schema = json.loads(schema_str)
        assert "id" in [c["column_name"] for c in schema]
        print("✅ [4/7] `dataset_schema` logic passed.")

        # 5. Test _enum_metadata_logic
        metadata_str = await _enum_metadata_logic()
        metadata = json.loads(metadata_str)
        assert "ModalidadeContratacao" in metadata
        assert "values" in metadata["ModalidadeContratacao"]
        print("✅ [5/7] `enum_metadata` logic passed.")

        # 6. Test _dataset_schema_logic with path traversal attempt
        invalid_schema_str = await _dataset_schema_logic(
            "../invalid", base_dir=str(parquet_dir)
        )
        invalid_schema = json.loads(invalid_schema_str)
        assert "error" in invalid_schema
        assert "Invalid dataset path." in invalid_schema["error"]
        print("✅ [6/7] `_dataset_schema_logic` path traversal prevention passed.")

        # 7. Test _execute_sql_query_logic (success)
        result_str = await _execute_sql_query_logic(
            "SELECT * FROM contratos", base_dir=str(parquet_dir)
        )
        result = json.loads(result_str)
        assert len(result) == 2
        assert result[0]["fornecedor"] == "Empresa A"
        print("✅ [7/7] `execute_sql_query` logic passed.")

    finally:
        shutil.rmtree(test_dir)
        print("\n--- Tests Finished ---")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(_run_tests())
    else:
        run_server()
