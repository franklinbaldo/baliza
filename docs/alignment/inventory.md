# Baliza Test & BDD Inventory

This document outlines the testing frameworks, configuration, and execution commands for the Baliza project.

## Testing Frameworks

- **Primary Framework:** `pytest` is the core test runner.
- **HTTP Mocking:** `pytest-httpx` and `vcrpy` are used for mocking HTTP requests in tests, ensuring that tests are repeatable and don't rely on the live PNCP API.
- **BDD Framework:** The project **does not** currently use a formal BDD framework like `pytest-bdd` or `behave`. There are no `.feature` files in the repository. The tests are written in Python using `pytest`.

## Test Locations

- **Unit Tests:** `tests/unit/`
- **End-to-End Tests:** `tests/e2e/`
- **Integration Tests:** `tests/integration/`

## How to Run Tests

Tests are executed using `pytest`. The recommended way to run the tests is through the `uv` command, which ensures the tests run in the correct environment with the project's dependencies.

To run all tests:
```bash
uv run pytest
```

To run a specific test file:
```bash
uv run pytest tests/unit/test_dates.py
```
