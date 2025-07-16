# TODO - BALIZA Project Improvements

This document outlines a roadmap for improving the code structure, quality, and robustness of the BALIZA data extractor. The current implementation is based on a solid, stateful architecture using a DuckDB control table, but it can be refined for better maintainability, error handling, and user experience.

---

### 1. Code Modularity and Structure

**Problem:** The main script `src/baliza/pncp_extractor.py` is over 800 lines long, making it difficult to navigate and maintain. It mixes responsibilities like database management, API fetching, business logic, and CLI definitions.

**Solution:** Refactor the single file into a structured package.

-   **`src/baliza/config.py`**:
    -   [ ] Move `PNCP_ENDPOINTS` list here.
    -   [ ] Move constants like `PNCP_BASE_URL`, `CONCURRENCY`, `PAGE_SIZE`, `USER_AGENT`.
-   **`src/baliza/database.py`**:
    -   [ ] Create a `DatabaseManager` class to encapsulate all DuckDB interactions.
    -   [ ] Move `connect_utf8`, `_init_database`, `_create_indexes_if_not_exist`, `_migrate_to_zstd_compression`.
    -   [ ] The `DatabaseManager` should handle the connection and cursor lifecycle.
-   **`src/baliza/fetch.py`**:
    -   [ ] Create an `APIClient` class.
    -   [ ] Move `_fetch_with_backpressure` and related HTTP logic into this class.
    -   [ ] The `APIClient` should manage the `httpx.AsyncClient` lifecycle.
-   **`src/baliza/models.py`**:
    -   [ ] Define an `Enum` for task statuses (`TaskStatus.PENDING`, `TaskStatus.FETCHING`, etc.).
    -   [ ] Use dataclasses or Pydantic models to represent a `Task` and an API `Response`.
-   **`src/baliza/extractor.py`**:
    -   [ ] The main `AsyncPNCPExtractor` class will be leaner.
    -   [ ] It will use instances of `DatabaseManager` and `APIClient` to perform its work.
    -   [ ] The core logic of the extraction phases (`_plan_tasks`, `_discover_tasks`, etc.) will remain here, but will call out to the other modules.
-   **`src/baliza/main.py` (or `cli.py`)**:
    -   [ ] Move the `typer` app and command definitions here.

---

### 2. Improve Robustness and Error Handling

**Problem:** The current state machine has potential edge cases. For example, a task can get stuck in the `DISCOVERING` state if the script crashes at the wrong moment.

**Solution:**

-   [ ] **Handle Stuck Tasks:** In the `_discover_tasks` phase, add logic to also select tasks that have been in the `DISCOVERING` state for an unusually long time (e.g., > 1 hour) and retry them.
-   [ ] **Reconciliation for Failures:** The `_reconcile_tasks` phase should also handle tasks that failed during fetching. It should check if any pages were successfully downloaded before the failure and update the `missing_pages` list accordingly, setting the status to `PARTIAL` instead of leaving it as `FAILED`.
-   [ ] **Dead-Letter Queue:** For tasks that repeatedly fail discovery or fetching, change their status to `FAILED` and record the final error. Prevent them from being picked up in subsequent runs unless a `force` flag is used.

---

### 3. Enhance User Interface and Progress Reporting

**Problem:** The current progress report is good, but it only shows the "Fetching" phase. The user doesn't see the progress of the Discovery or Reconciliation phases.

**Solution:** Implement the multi-level progress reporting envisioned in `tabela-tasks.md`.

-   [ ] **Overall Progress Bar:** Add a main progress bar at the top that tracks the number of `COMPLETE` tasks vs. the total number of tasks in the database. `(SELECT COUNT(*) WHERE status = 'COMPLETE') / (SELECT COUNT(*))`
-   [ ] **Phase-Specific Bars:**
    -   Keep the detailed per-endpoint bars for the **Execution** phase.
    -   Add a progress bar for the **Discovery** phase, showing `(Tasks Discovered / Total Pending Tasks)`.
    -   Add a spinner or a progress bar for the **Reconciliation** phase.

---

### 4. Code Quality and Best Practices

**Problem:** The code contains "magic strings" for statuses, hardcoded paths, and some complex functions that could be simplified.

**Solution:**

-   [ ] **Use Enums/Constants:** Replace all string literals for statuses (`'PENDING'`, `'COMPLETE'`, etc.) with the `TaskStatus` Enum defined in `models.py`.
-   [ ] **Configuration Management:** Use a library like `pydantic-settings` to manage configuration (database paths, concurrency) via environment variables or a config file, instead of hardcoding them.
-   [ ] **Simplify Migration:** The `_migrate_to_zstd_compression` logic is complex and runs on every startup. Convert this into a separate, one-time CLI command (e.g., `baliza db migrate`). The main `extract` command should assume the schema is correct.
-   [ ] **Remove Dead Code:** The `_complete_task_and_print` function appears to be unused. Analyze and remove it if confirmed.

---

### 5. Documentation and Cleanup

**Problem:** Some of the markdown documents are now out of sync with the implementation.

**Solution:**

-   [ ] **Update `PROBLEMAS_PAGINAS.MD`**: Add a note at the top of this file explaining that the issue was resolved by implementing the more advanced architecture from `tabela-tasks.md`, and that the document is kept for historical context.
-   [ ] **Archive `tabela-tasks.md`**: Rename it to `docs/archive/architecture-v2.md` and add a note that it describes the implemented architecture.
-   [ ] **Add Code Comments:** Add comments to the key phases in `AsyncPNCPExtractor` explaining *why* each phase is necessary (e.g., "Phase 1: Plan all work to ensure idempotency").
