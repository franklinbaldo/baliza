# BDD & Test Framework Inventory

This document outlines the testing frameworks, tools, and conventions used in the Baliza project.

## Testing Frameworks

- **Primary Runner:** `pytest` is used as the main test runner.
- **BDD Framework:** `pytest-bdd` is used to enable Behavior-Driven Development with Gherkin `.feature` files.
- **HTTP Mocking:** `pytest-vcr` is used to record and replay HTTP interactions, ensuring tests are fast and reliable.

## File Locations

- **BDD Features:** Gherkin `.feature` files are located in `tests/features/`.
- **Step Definitions:** Python step implementations are located in `tests/step_defs/`.
- **Test Fixtures:** Shared test fixtures are defined in `tests/conftest.py` and `tests/fixtures/`.
- **Recorded HTTP Cassettes:** `pytest-vcr` cassettes are stored in `tests/cassettes/`.
- **Unit Tests:** Unit tests are located in `tests/unit/`.
- **Integration Tests:** Integration tests are located in `tests/integration/`.
- **End-to-End Tests:** E2E tests are located in `tests/e2e/`.

## How to Run Tests

To run the full test suite, use the following command from the project root:

```bash
uv run pytest
```
