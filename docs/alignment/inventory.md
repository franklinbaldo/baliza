# BDD & Test Framework Inventory

This document outlines the testing frameworks and procedures used in the Baliza project.

## Frameworks

- **Test Runner:** [pytest](https://docs.pytest.org/)
- **BDD Framework:** [pytest-bdd](https://pytest-bdd.readthedocs.io/)
- **Mocking:** [pytest-mock](https://pytest-mock.readthedocs.io/)
- **HTTP fixtures:** [pytest-vcr](https://pytest-vcr.readthedocs.io/)

## How to Run Tests

The full test suite can be executed using `uv`.

```bash
uv run pytest
```

## File Locations

- **BDD Features:** `tests/features/`
- **Step Definitions:** `tests/step_defs/`
- **Unit Tests:** `tests/unit/`
- **End-to-end Tests:** `tests/e2e/`
- **Test Fixtures:** `tests/fixtures/` and `tests/conftest.py`
- **VCR Cassettes:** `tests/cassettes/`
