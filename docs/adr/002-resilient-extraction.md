# ADR-002: Resilient and Task-Driven Extraction Architecture

**Status:** Accepted

**Context:**
Extracting data from the PNCP is a long-running process prone to failures (network errors, API instability, rate limiting). A simple script that iterates through dates and pages is fragile: any interruption would result in losing all progress and require a full restart. We need a system that is:
1.  **Resilient:** Able to survive interruptions and resume from where it left off.
2.  **Idempotent:** Repeated executions should not cause data duplication or unnecessary work.
3.  **Monitorable:** Extraction progress must be clear and easy to track.
4.  **Efficient:** Must use concurrency to maximize download speed.

**Decision:**
We will implement a task-driven extraction architecture managed by a control table (`psa.pncp_extraction_tasks`) in DuckDB. The process will be divided into four distinct phases:
1.  **Planning:** Generates all tasks (a combination of endpoint, date, and modality) and inserts them into the control table with `PENDING` status.
2.  **Discovery:** For each pending task, fetches the first page to get metadata (e.g., `total_pages`) and updates the task status to `FETCHING`.
3.  **Execution:** Concurrently downloads all pages listed as `missing_pages` for all tasks in `FETCHING` or `PARTIAL` states.
4.  **Reconciliation:** After execution, verifies which pages were successfully saved to the database and updates each task's status to `COMPLETE` or `PARTIAL`.

**Consequences:**
*   **Positive:**
    *   **Maximum Fault Tolerance:** The process can be interrupted at any time and resumed without data loss by simply re-running the `extract` command.
    *   **Persistent State:** The control table serves as the single source of truth for progress, making the system transparent and easy to debug.
    *   **Efficiency:** The execution phase can focus exclusively on the bulk download of pages, without complex state logic in the main loop.
    *   **Scalability:** The discovery of procurement modalities was easily integrated by generating distinct tasks for each one.
*   **Negative:**
    *   **Increased Complexity:** The code logic is more complex than a simple monolithic script.
    *   **Database Overhead:** Requires more interactions with DuckDB to manage task states.