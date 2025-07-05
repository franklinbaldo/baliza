## Agent Instructions for Baliza Project

Welcome, AI Agent! This document provides guidance for working on the Baliza codebase.

### Project Overview

Baliza is a Python application that downloads daily data from Brazil's National Public Procurement Portal (PNCP) and archives it to the Internet Archive (IA). The goal is to create a reliable, long-term public backup of this important data.

### Key Technologies

*   **Python 3.11+**: The primary programming language.
*   **Typer**: Used for the command-line interface (CLI).
*   **Requests**: For making HTTP requests to the PNCP API.
*   **Tenacity**: For handling retries with exponential backoff.
*   **Zstandard**: For compressing data before upload.
*   **Internetarchive**: Python library for interacting with the Internet Archive.
*   **DuckDB**: Planned for robust state management (currently CSV).
*   **Pytest**: For running tests.
*   **uv**: For environment and package management. Preferred over pip/venv directly.

### Code Structure (Post-Refactor)

The main application code resides in the `src/baliza/` directory:

*   `cli.py`: The main entry point for the Typer CLI application. Orchestrates the overall workflow.
*   `client.py`: Handles all communication with the PNCP API, including fetching data and retry logic.
*   `pipeline.py`: Manages the data harvesting process, including pagination over API results.
*   `storage.py`: Responsible for local file operations (creating JSONL, compressing with Zstandard, calculating checksums) and uploading files to the Internet Archive.
*   `state.py`: Manages the application's state, tracking processed files and their upload status. Initially CSV, will be migrated to DuckDB.
*   `__init__.py`: Makes `baliza` a Python package and defines `__version__`.
*   `main.py`: Kept for backward compatibility, currently a thin wrapper around `cli.py`.

### Development Workflow

1.  **Environment Setup**:
    *   Ensure `uv` is installed.
    *   Use `uv venv` to create/activate the virtual environment.
    *   Use `uv sync` to install/update dependencies from `pyproject.toml` and `uv.lock`.

2.  **Running the Application**:
    *   The primary way to run is via the CLI: `python -m baliza.cli run-baliza --date YYYY-MM-DD`.
    *   Or, once installed: `baliza run-baliza --date YYYY-MM-DD`.
    *   Internet Archive credentials (`IA_KEY`, `IA_SECRET`) must be set as environment variables for uploads to work.

3.  **Running Tests**:
    *   Tests are located in the `tests/` directory.
    *   Run tests using `pytest` from the project root: `uv run pytest`
    *   Aim for high test coverage, especially for new features or critical paths.
    *   Use `pytest-httpserver` for mocking external HTTP services (like PNCP).
    *   Integration tests (e.g., actual IA uploads to a staging area) should be marked appropriately (e.g., `@pytest.mark.integration`) and may require specific setup or credentials.

4.  **Logging**:
    *   The project is moving towards structured JSON logging. Use Python's `logging` module for this.
    *   Avoid `print()` or `typer.echo()` for application logic logging; these are acceptable for direct user feedback in `cli.py` if appropriate.

5.  **State Management**:
    *   The `state/processed_records.csv` file is the current method for tracking processed data.
    *   This will be replaced by a DuckDB database (`state/baliza_state.duckdb`). Schema changes or queries should be handled carefully.

6.  **File Naming**:
    *   Data files uploaded to Internet Archive should follow the convention: `pncp-ctrt-YYYY-MM-DD.jsonl.zst` (for contratações) or `pncp-ctos-YYYY-MM-DD.jsonl.zst` (for contratos, when implemented). `ctrt` and `ctos` are preferred short forms.

7.  **Commits and Pull Requests**:
    *   Follow conventional commit messages if possible (e.g., `feat: ...`, `fix: ...`, `refactor: ...`).
    *   Ensure tests pass before submitting changes.
    *   Update `README.md` and this `AGENTS.md` file if your changes affect setup, architecture, or agent instructions.

### Current Plan & Priorities (as of initial refactoring)

The project is undergoing a significant refactoring based on a diagnostic review. Key priorities include:

1.  **Modularization**: Separating concerns into `client`, `pipeline`, `storage`, `state`, and `cli` modules. (Largely complete in first pass).
2.  **Robust State**: Migrating from CSV to DuckDB for state management.
3.  **Improved Reliability**: Enhancing retry logic, implementing structured logging, and adding alerts.
4.  **Workflow Optimization**: Improving GitHub Actions efficiency and Dockerizing the application.
5.  **Testing**: Expanding test coverage, especially integration tests.

Please refer to the active plan provided by the user or in project management tools for the most current tasks.

### Important Considerations

*   **Rate Limiting**: Be mindful of the PNCP API's rate limits. Implement appropriate delays (e.g., `time.sleep(1)` between paged requests) and robust retry mechanisms.
*   **Idempotency**: Design operations to be idempotent where possible. If a step fails, re-running it should not cause duplicate data or incorrect state, especially concerning IA uploads and state recording.
*   **Error Handling**: Implement comprehensive error handling. Log errors clearly and ensure the application fails gracefully or recovers if possible.
*   **Security**: Handle API keys (like IA_KEY, IA_SECRET) securely. They should be passed via environment variables, not hardcoded. Future work includes exploring OIDC for GitHub Actions.

By following these guidelines, you'll help maintain a high-quality, robust, and maintainable codebase for Baliza. If anything is unclear, please ask for clarification.
