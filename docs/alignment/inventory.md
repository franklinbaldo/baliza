# Baliza Project Inventory

This document provides the canonical commands for building, testing, and running the Baliza project.

## Dependency Management

To install all project dependencies, including those for development and testing, run:

```bash
uv sync --all-extras
```

## Running the CLI

```bash
uv run baliza --help
```

## Running Tests

To execute the full test suite, use the following command:

```bash
uv run pytest
```

To run only Tier 0 tests:

```bash
uv run pytest -m tier0
```

To run BDD tests only:

```bash
uv run pytest tests/step_defs/
```

## Linting and Formatting

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```
