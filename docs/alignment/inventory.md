# BDD & Test Framework Inventory

This document inventories the testing frameworks and tools used in the Baliza project.

## BDD Framework

- **Framework:** [pytest-bdd](https://pytest-bdd.readthedocs.io/)
- **Features Location:** `tests/features/`
- **Step Definitions Location:** `tests/step_defs/`

## Test Runner

- **Framework:** [pytest](https://docs.pytest.org/)
- **Configuration:** `pyproject.toml` (under `[tool.pytest.ini_options]`)

## How to Run Tests

The following command runs the entire test suite, including BDD scenarios:

```bash
uv run pytest
```
