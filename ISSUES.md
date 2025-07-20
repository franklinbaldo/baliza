# Baliza Architectural and Design Issues

This document outlines the architectural and design issues found in the `baliza` project.

## `cli.py`

*   **Mixing of UI and Business Logic:** The `extract`, `transform`, and `load` commands contain a lot of UI-related code (e.g., `rich` progress bars, headers, panels) mixed with the core business logic. This makes the code harder to test and reuse.
*   **Monolithic `run` Command:** The `run` command is a large, monolithic function that orchestrates the entire ETL pipeline. This could be broken down into smaller, more manageable functions.
*   **Lack of Dependency Injection:** The `AsyncPNCPExtractor`, `transformer`, and `loader` are directly imported and used. This makes it difficult to swap out implementations for testing or other purposes.

## `config.py`

*   **Hardcoded Configuration:** The `pncp_endpoints` list is hardcoded in the `Settings` class. This should be moved to a separate configuration file (e.g., YAML or TOML) to allow for easier modification without changing the code.

## `extractor.py`

*   **Complex `extract_data` Method:** The `extract_data` method is very long and complex, handling planning, discovery, execution, and reconciliation. This could be broken down into smaller, more focused methods.
*   **Tight Coupling with `PNCPWriter`:** The `AsyncPNCPExtractor` is tightly coupled with the `PNCPWriter`. This makes it difficult to use a different writer implementation.
*   **Lack of a Clear State Machine:** The task status is managed with strings (`PENDING`, `DISCOVERING`, `FETCHING`, etc.). A more robust state machine implementation would make the logic clearer and less error-prone.

## `loader.py`

*   **Hardcoded Table Names:** The `export_to_parquet` function hardcodes the `gold` schema and table names. This should be made more configurable.
*   **Lack of Error Handling:** The `upload_to_internet_archive` function does not have any error handling for the `upload` call.

## `mcp_server.py`

*   **Hardcoded Table Mapping:** The `PARQUET_TABLE_MAPPING` is hardcoded. This should be made more configurable.
*   **SQL Injection Vulnerability:** The `_execute_sql_query_logic` function is vulnerable to SQL injection attacks. It only checks if the query starts with `SELECT`, but this is not sufficient to prevent malicious queries.

## `pncp_writer.py`

*   **Monolithic `_init_database` Method:** The `_init_database` method is responsible for creating the schema, tables, indexes, and migrating the data. This should be broken down into smaller, more focused methods.
*   **Lack of a Migration Framework:** The database schema is managed manually. Using a migration framework like Alembic would make it easier to manage schema changes over time.

## `task_claimer.py`

*   **Hardcoded dbt Command:** The `create_task_plan` function hardcodes the `dbt` command. This should be made more configurable.

## `transformer.py`

*   **Hardcoded dbt Command:** The `transform` function hardcodes the `dbt` command. This should be made more configurable.
