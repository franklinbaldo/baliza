# Analysis of `scripts/export_to_parquet.py`

This script exports data from a DuckDB database to Parquet files.

## Issues Found

1.  **Hardcoded Paths:** The `db_path` and `output_dir` are hardcoded. This makes the script less flexible. These should be configurable, for example, via command-line arguments.
2.  **Hardcoded Table Name:** The `table_name` is hardcoded to `raw_pncp`. This should also be configurable.
3.  **Assumes Data from Yesterday:** The script is hardcoded to export data from yesterday (`date.today() - timedelta(days=1)`). This is not flexible and might not be what the user wants. The date should be a parameter.
4.  **SQL Injection Vulnerability:** The SQL query is constructed using an f-string, which can be vulnerable to SQL injection if the input is not properly sanitized. While the inputs in this script are controlled, it's a bad practice. Use parameterized queries instead.
5.  **No Logging:** The script uses `print` for output. It would be better to use a proper logging library for more structured and configurable logging.
6.  **No Error Handling for `os.path.exists`:** The script checks if the database file exists, but it doesn't handle the case where the output directory doesn't exist.
7.  **Bare `except Exception`:** The `except` block catches a bare `Exception`, which is too broad. It should catch more specific exceptions.
8.  **Redundant `print` statement:** The `print` statement inside the `export_new_data_to_parquet` function is redundant, as the function is only called from the `if __name__ == "__main__"` block.

## Suggestions for Improvement

*   **Command-Line Arguments:** Use `argparse` or a similar library to make paths, table names, and dates configurable.
*   **Parameterized Queries:** Use parameterized queries to prevent SQL injection vulnerabilities.
*   **Logging:** Use the `logging` module for better logging.
*   **Error Handling:** Add more specific error handling and ensure the output directory exists.
*   **Code Organization:** The script could be structured into smaller functions for better readability and testability.
*   **Remove redundant code:** Remove the redundant `print` statement.
*   **Add docstrings:** Add docstrings to the functions to explain what they do.
*   **Add type hints:** Add type hints to the function signatures.

## Proposed Solutions

*   **Add a `main` function:**
    *   Use `argparse` to add command-line arguments for the database path, output directory, table name, and target date.
    *   Use a `try...except` block to handle `FileNotFoundError` if the database file does not exist.
    *   Use a logging library to log the progress of the script.
*   **Refactor `export_new_data_to_parquet`:**
    *   Use parameterized queries to prevent SQL injection vulnerabilities.
    *   Add more specific error handling for database and file operations.
    *   Ensure the output directory exists before exporting the data.
    *   Add a docstring and type hints to the function.
