# Analysis of `src/baliza/mcp_server.py`

This file implements the MCP server for AI-powered data analysis.

## Architectural Issues

1.  **Hardcoded Table Mapping:** The `PARQUET_TABLE_MAPPING` is hardcoded. This makes it difficult to add new datasets without modifying the code. It should be loaded from a configuration file.
2.  **Mixing Logic and API:** The file mixes the logic for handling MCP requests with the API endpoint definitions. This violates the Single Responsibility Principle and makes the code harder to test.
3.  **Noisy `logger` variable:** The `logger` variable is created but it is not used in the whole file.

## Code Quality Issues

1.  **Hardcoded Paths:** The `base_dir` in `_dataset_schema_logic` and `_execute_sql_query_logic` is hardcoded to `data/parquet`. This should be configurable.
2.  **Bare `except Exception`:** The `_dataset_schema_logic` and `_execute_sql_query_logic` functions use a bare `except Exception`, which is too broad. It's better to catch more specific exceptions.
3.  **Noisy `if __name__ == "__main__"` block:** The `if __name__ == "__main__"` block contains test code that is not part of a formal test suite. This makes the code harder to test and maintain.
4.  **Redundant `run_server` and `run_server_async` functions:** The `run_server` and `run_server_async` functions are redundant. The `run_server` function should be removed and the `run_server_async` function should be used instead.

## Suggestions for Improvement

*   **Externalize Table Mapping:** Move the `PARQUET_TABLE_MAPPING` to a separate configuration file (e.g., a YAML or JSON file).
*   **Separate Logic and API:** Create a separate `mcp_logic` module to contain the logic for handling MCP requests. The `mcp_server.py` file should then import the logic functions from the `mcp_logic` module.
*   **Remove unused variables:** The `logger` variable is not used anywhere in the file and should be removed.
*   **Make Paths Configurable:** The paths to the data and parquet files should be configurable, for example, through a setting in the `config.py` file.
*   **Specific Exception Handling:** Use more specific exception handling to provide better error messages and make the script more robust.
*   **Move Test Code to a Test Suite:** The test code in the `if __name__ == "__main__"` block should be moved to a separate test file in the `tests` directory.
*   **Remove Redundant Functions:** Remove the `run_server` function and use the `run_server_async` function instead.

Overall, the `mcp_server.py` file is a good starting point, but it needs a significant refactoring to improve its architecture and code quality. The suggestions above will help to make the code more modular, maintainable, and easier to understand.

## Proposed Solutions

*   **Create a `config` module:**
    *   This module will be responsible for loading the configuration from a YAML or JSON file.
    *   It will have a `get_table_mapping` function that returns the table mapping.
    *   It will have a `get_data_dir` function that returns the path to the data directory.
*   **Create a `logic` module:**
    *   This module will contain the logic for handling MCP requests.
    *   It will have functions for getting available datasets, getting dataset schemas, and executing SQL queries.
*   **Refactor `mcp_server.py`:**
    *   The `mcp_server.py` file will be responsible for defining the MCP server and its resources.
    *   It will import the logic functions from the `logic` module.
    *   It will use the `config` module to get the table mapping and data directory.
*   **Move Test Code to a Test Suite:**
    *   Create a `test_mcp_server.py` file in the `tests` directory.
    *   Move the test code from the `if __name__ == "__main__"` block to the `test_mcp_server.py` file.
*   **Remove Redundant Functions:**
    *   Remove the `run_server` function.
*   **Add docstrings and type hints to all functions.**
