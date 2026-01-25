# BDD & Test Framework Inventory

- **Frameworks Used:**
  - `pytest` (core test runner)
  - `pytest-bdd` (for Gherkin feature execution)
  - `pytest-httpx` (for mocking HTTP requests)

- **How to Run Tests:**
  ```bash
  # Install all dependencies (including test extras)
  uv sync --all-extras

  # Run the full test suite
  uv run pytest
  ```

- **Feature File Location:**
  - All BDD `.feature` files are located in `tests/features/`.
