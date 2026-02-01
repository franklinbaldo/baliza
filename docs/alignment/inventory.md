# Baliza Project Inventory

This document provides the canonical commands for building, testing, and running the Baliza project.

## Dependency Management

To install all project dependencies, including those for development and testing, run:

```bash
uv sync --all-extras
```

## Running the CLI

Baliza is a Typer-based CLI. Available commands:

- `extract`: Extract data from PNCP API to DuckDB.
- `verify`: Verify data coverage and detect gaps.
- `export`: Export DuckDB table to Parquet files.
- `export-daily`: Export daily self-contained parquet package.
- `buffer-stats`: Show buffer statistics for monitoring.
- `status`: Show overall extraction status.

Usage:
```bash
uv run baliza [COMMAND] --help
```

## Running Tests

To execute the full test suite, use:

```bash
uv run pytest
```

To run only BDD features:
```bash
uv run pytest tests/step_defs/
```

To run integration tests:
```bash
uv run pytest tests/integration/
```

## Frameworks and Tools

- **Backend**: Python 3.11+
- **CLI**: Typer, Rich
- **Data**: DuckDB, PyArrow
- **HTTP Client**: HTTPX, Tenacity
- **Testing**: Pytest, Pytest-BDD, VCR.py, Pytest-HTTPX, Pytest-Mock
- **Linting**: Ruff
- **Type Checking**: MyPy
- **Package Manager**: uv
