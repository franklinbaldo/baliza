# Baliza Test & BDD Inventory

## Frameworks

- **Testing Framework:** `pytest`
- **BDD Framework:** `pytest-bdd`
- **HTTP Mocking:** `pytest-httpx`

## How to Run Tests

The project uses `uv` for environment management.

```bash
# Install all dependencies, including test dependencies
uv sync --all-extras

# Run the full test suite
uv run pytest
```

## Locations

- **BDD Features:** `tests/features/`
- **Step Definitions:** `tests/step_defs/`
- **Test Fixtures:** `tests/fixtures/` and `tests/conftest.py`
- **Integration Tests:** `tests/integration/`
