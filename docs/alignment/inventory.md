# BDD & Testing Inventory

This document inventories the testing frameworks, commands, and asset locations for the Baliza CLI project.

## 1. Testing Frameworks

The project uses a combination of frameworks built on top of `pytest`:

- **Test Runner:** `pytest` is the primary test runner.
- **BDD Framework:** `pytest-bdd` is used for Behavior-Driven Development, linking `.feature` files to Python test code.
- **HTTP Mocking:**
    - `pytest-httpx` provides fixtures for mocking HTTP requests in tests.
    - `pytest-vcr` is used for recording and replaying real HTTP interactions, ensuring tests are fast and deterministic while based on actual API responses.

## 2. How to Run Tests

To run the full test suite, execute the following command from the repository root:

```bash
uv run pytest
```

This command will discover and run all tests, including the BDD scenarios.

## 3. Asset Locations

- **BDD Feature Files:** Business-readable specifications are located in `tests/features/`. These files use the Gherkin syntax (`.feature`).
- **Step Definitions:** The Python code that implements the BDD steps is located in `tests/step_defs/`. Each `.feature` file has a corresponding `test_*.py` file in this directory.
- **Test Fixtures:** Shared `pytest` fixtures are located in `tests/conftest.py`.
- **Recorded API Responses (Cassettes):** `pytest-vcr` stores recorded HTTP interactions as YAML files in `tests/cassettes/`. These are used to replay API responses without making live network calls.
