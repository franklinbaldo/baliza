# ADR-005: Adopt a Modern, High-Performance Python Toolchain

**Status:** Accepted

**Context:**
For an I/O-intensive data processing project like BALIZA, performance and developer experience (DX) are crucial. Traditional tools like `pip`, `virtualenv`, and the `black`/`isort`/`flake8` combination work, but more modern alternatives offer significant gains.

**Decision:**
We will adopt a modern, integrated set of tools for Python development:
1.  **`uv`:** For dependency and virtual environment management, replacing `pip` and `venv`.
2.  **`ruff`:** For code linting and formatting, replacing `black`, `isort`, `flake8`, and others.
3.  **`httpx` with HTTP/2 support:** For asynchronous HTTP requests, instead of `requests`.
4.  **`typer` and `rich`:** To create a modern, interactive, and user-friendly Command Line Interface (CLI).

**Consequences:**
*   **Positive:**
    *   **Development Performance:** `uv` is orders of magnitude faster at installing and resolving dependencies, speeding up environment setup and CI pipelines.
    *   **Simplified DX:** `ruff`, as a single, extremely fast tool, simplifies the `pre-commit` configuration and shortens feedback cycles.
    *   **Execution Performance:** `httpx` with HTTP/2 allows for more efficient use of network connections, which is critical for bulk data extraction.
    *   **Usability:** The CLI built with `typer` and `rich` provides a superior user experience with automatic help, validation, and informative progress bars.
*   **Negative:**
    *   **Newer Tools:** `uv` is newer than `pip`. Although stable, it may have less history in niche scenarios.
    *   **Learning Curve:** Developers accustomed to the traditional ecosystem may need a brief adjustment period.