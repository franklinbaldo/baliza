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
    # Basic security to prevent path traversal
    if ".." in dataset_name or "/" in dataset_name:
        return json.dumps({"error": "Invalid dataset name."})

    data_dir = Path(base_dir) if base_dir else Path("data/parquet")
    parquet_path = data_dir / f"{dataset_name}.parquet"

    if not parquet_path.exists():
        return json.dumps({"error": f"Dataset '{dataset_name}' not found."})

    try:
        con = duckdb.connect(database=":memory:")
        schema = con.execute(f"DESCRIBE SELECT * FROM '{parquet_path!s}'").fetchdf()
        return schema.to_json(orient="records")
    except duckdb.Error as e:
        logger.error(f"Failed to get schema for {dataset_name}: {e}")
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
    # Security Validation: Only allow SELECT statements
    if not query.strip().upper().startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are allowed."})

    try:
        con = duckdb.connect(database=":memory:")

        parquet_dir = Path(base_dir) if base_dir else Path("data/parquet")
        if parquet_dir.exists():
            for parquet_file in parquet_dir.glob("*.parquet"):
                table_name = parquet_file.stem
                con.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{parquet_file!s}'"
                )

        result = con.execute(query).fetchdf()
        return result.to_json(orient="records")

    except duckdb.Error as e:
        logger.error(f"Query failed: {query} - {e}")
        return json.dumps({"error": f"Query failed: {e!s}"})


@app.tool("mcp://baliza/execute_sql_query")
async def execute_sql_query(query: str) -> str:
    return await _execute_sql_query_logic(query)


def run_server():
    """
    Runs the MCP server. This function will be called by the CLI.
    """
    app.run()


async def _run_tests():
    """A simple test runner to validate functionality."""
    import shutil
    import tempfile

    import pandas as pd

    print("--- Running MCP Server Tests ---")
    test_dir = tempfile.mkdtemp()
    parquet_dir = Path(test_dir) / "parquet"
    parquet_dir.mkdir()

    # Store original Path class
    OriginalPath = Path

    try:
        # Create dummy data
        contracts_df = pd.DataFrame(
            {
                "id": [1, 2],
                "valor": [100.0, 200.0],
                "fornecedor": ["Empresa A", "Empresa B"],
            }
        )
        contracts_df.to_parquet(parquet_dir / "contratos.parquet")
        print("✅ [1/6] Dummy data created.")

        # 2. Test available_datasets_logic
        datasets_str = await _available_datasets_logic()
        datasets = json.loads(datasets_str)
        assert len(datasets) > 0
        print("✅ [2/6] `available_datasets` logic passed.")

        # 3. Test _dataset_schema_logic
        schema_str = await _dataset_schema_logic("contratos", base_dir=str(parquet_dir))
        schema = json.loads(schema_str)
        assert "id" in [c["column_name"] for c in schema]
        print("✅ [3/6] `dataset_schema` logic passed.")

        # 4. Test _enum_metadata_logic
        metadata_str = await _enum_metadata_logic()
        metadata = json.loads(metadata_str)
        assert "ModalidadeContratacao" in metadata
        assert "values" in metadata["ModalidadeContratacao"]
        print("✅ [4/6] `enum_metadata` logic passed.")

        # 5. Test _execute_sql_query_logic (security)
        error_str = await _execute_sql_query_logic(
            "DROP TABLE a;", base_dir=str(parquet_dir)
        )
        error = json.loads(error_str)
        assert "error" in error
        print("✅ [5/6] `execute_sql_query` security passed.")

        # 6. Test _execute_sql_query_logic (success)
        result_str = await _execute_sql_query_logic(
            "SELECT * FROM contratos", base_dir=str(parquet_dir)
        )
        result = json.loads(result_str)
        assert len(result) == 2
        assert result[0]["fornecedor"] == "Empresa A"
        print("✅ [6/6] `execute_sql_query` logic passed.")

    finally:
        shutil.rmtree(test_dir)
        print("\n--- Tests Finished ---")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        asyncio.run(_run_tests())
    else:
        run_server()
