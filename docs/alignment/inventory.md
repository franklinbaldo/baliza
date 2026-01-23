# BDD & Test Framework Inventory

This document inventories the tools and conventions used for testing in the Baliza project.

## Frameworks

- **BDD Framework**: [`pytest-bdd`](https://pytest-bdd.readthedocs.io/)
- **Test Runner**: [`pytest`](https://docs.pytest.org/)
- **HTTP Mocking**: [`pytest-httpx`](https://colin-b.github.io/pytest-httpx/)
- **CLI Testing**: [`typer.testing.CliRunner`](https://typer.tiangolo.com/tutorial/testing/)

## How to Run Tests

The test suite can be executed using the `pytest` command. First, ensure all test dependencies are installed.

```bash
# Install dependencies, including test extras
uv sync --all-extras

# Run the full test suite
uv run pytest
```

## File & Folder Locations

- **BDD Features**: All Gherkin `.feature` files are located in `tests/features/`.
- **Step Definitions**: The Python implementation for the BDD steps are in `tests/step_defs/`. Each `.feature` file has a corresponding `test_*.py` file.
- **Test Configuration**: Pytest is configured in `pyproject.toml` under the `[tool.pytest.ini_options]` section.
- **Fixtures & Helpers**: Shared test fixtures and support code are located in `tests/conftest.py`.
