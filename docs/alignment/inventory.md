# Baliza Project Inventory

This document provides the canonical commands for building, testing, and running the Baliza project.

## Frameworks and Tools

- **CLI Framework:** [Typer](https://typer.tiangolo.com/)
- **Data Processing:** [DuckDB](https://duckdb.org/), [PyArrow](https://arrow.apache.org/docs/python/index.html)
- **HTTP Client:** [httpx](https://www.python-httpx.org/)
- **Dependency Management:** [uv](https://github.com/astral-sh/uv)
- **Testing:** [pytest](https://docs.pytest.org/), [pytest-bdd](https://github.com/pytest-dev/pytest-bdd)
- **Linting:** [ruff](https://github.com/astral-sh/ruff)
- **Type Checking:** [mypy](https://mypy.readthedocs.io/)

## Dependency Management

To install all project dependencies, including those for development and testing, run:

```bash
uv sync --all-extras
```

## Running the CLI

To run the Baliza CLI locally:

```bash
uv run baliza --help
```

## Running Tests

To execute the full test suite, use the following command:

```bash
uv run pytest
```

To run only BDD tests:

```bash
uv run pytest tests/step_defs/
```

## Feature Files Location

BDD features are located in `tests/features/*.feature`.
Step definitions are located in `tests/step_defs/*.py`.
