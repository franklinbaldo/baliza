Excellent. Based on all our discussions, here is the final and consolidated implementation plan. This document serves as a complete technical guide for refactoring the script, adopting an architecture driven by a task control table.

---

### **Final Implementation Plan: Extraction Driven by a Control Table**

#### 1. Overview and Objective

The objective is to transform the extraction script from a real-time process into a more robust ETL (Extract, Transform, Load) system, managed by a persistent state. We will do this by introducing a **task control table** in DuckDB.

This table will function as a definitive "work plan," listing each combination of *endpoint* and *monthly period* as an individual task. The script will go through distinct phases: planning the work, discovering the details of each task (e.g., total pages), executing the download, and updating the progress.

This architecture will make the process extremely resilient to interruptions, fully resumable (idempotent), and much easier to monitor and debug.

---

#### 2. Proposed Architecture: The Task Control Table

We will create a new table, `psa.pncp_extraction_tasks`, which will be the brain of the operation.

**Table Definition (SQL):**
```sql
CREATE TABLE IF NOT EXISTS psa.pncp_extraction_tasks (
    -- Task Identifiers
    task_id VARCHAR PRIMARY KEY,                  -- Readable primary key, e.g., 'contratos_publicacao_2023-01-01'
    endpoint_name VARCHAR NOT NULL,               -- Name of the API endpoint
    data_date DATE NOT NULL,                      -- Start date of the monthly period for the task

    -- State Machine and Metadata
    status VARCHAR DEFAULT 'PENDING' NOT NULL,    -- State: PENDING, DISCOVERING, FETCHING, PARTIAL, COMPLETE, FAILED
    total_pages INTEGER,                          -- Total number of pages (discovered in Phase 2)
    total_records INTEGER,                        -- Total number of records (discovered in Phase 2)

    -- Progress Tracking
    missing_pages JSON,                           -- JSON list of missing page numbers, e.g., '[2, 5, 8]'

    -- Auditing and Debugging
    last_error TEXT,                              -- Message of the last error for easy diagnosis
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),

    -- Constraints to ensure integrity
    CONSTRAINT unique_task UNIQUE (endpoint_name, data_date)
);
```

**Task States (`status`):**
*   **`PENDING`**: The task has been created, but no work has started.
*   **`DISCOVERING`**: The process is fetching page 1 to obtain metadata.
*   **`FETCHING`**: The task is active, and its missing pages are being downloaded.
*   **`PARTIAL`**: The task was partially processed but interrupted. Useful for knowing which tasks to resume first.
*   **`COMPLETE`**: All pages for this task have been successfully downloaded and saved.
*   **`FAILED`**: An unrecoverable error occurred during the discovery or execution phase.

---

#### 3. Refactored Execution Flow in Phases

The main `extract_data` method will orchestrate the following phases sequentially:

**Phase 0: Initialization**
1.  Connect to DuckDB.
2.  Execute `CREATE TABLE IF NOT EXISTS` to ensure `psa.pncp_extraction_tasks` exists.

**Phase 1: Planning (Generate Tasks)**
*   **Objective:** Populate the control table with all necessary tasks.
*   **Actions:**
    1.  Generate the list of monthly periods (`months_to_process`) from the provided start and end dates.
    2.  Iterate over each `endpoint` and each `month`.
    3.  For each combination, build a `task_id` (e.g., `f"{endpoint['name']}_{month[0].isoformat()}"`).
    4.  Execute a batch `INSERT ... ON CONFLICT DO NOTHING` to add only the tasks that do not yet exist in the control table. This makes the phase idempotent.

**Phase 2: Discovery**
*   **Objective:** Obtain metadata (`total_pages`, `total_records`) for all pending tasks by fetching only page 1 of each.
*   **Actions:**
    1.  Select all tasks with `status = 'PENDING'`.
    2.  For each task, in parallel:
        a.  Update its status to `DISCOVERING`.
        b.  Make a single request to **page 1** of that endpoint/period.
        c.  **On success:**
            i.   Calculate `total_pages` from the `totalRegistros` of the response.
            ii.  Generate the initial list of `missing_pages` (e.g., `list(range(2, total_pages + 1))`).
            iii. Update the task in the table: `status = 'FETCHING'`, fill in `total_pages`, `total_records`, and `missing_pages`.
            iv. Save the page 1 response in the `psa.pncp_raw_responses` table using the `writer_worker`.
        d.  **On failure:**
            i.   Update the task in the table: `status = 'FAILED'`, fill in the `last_error` field.

**Phase 3: Execution (Fetching)**
*   **Objective:** Download all pages listed as "missing" in the work plan.
*   **Actions:**
    1.  Build a global list of **all pages to be downloaded** from all tasks with `status = 'FETCHING'` or `status = 'PARTIAL'`. The SQL query with `unnest` is perfect for this:
        ```sql
        SELECT t.task_id, t.endpoint_name, t.data_date, p.page_number
        FROM psa.pncp_extraction_tasks t,
             unnest(json_extract(t.missing_pages, '$')) AS p(page_number)
        WHERE t.status IN ('FETCHING', 'PARTIAL');
        ```
    2.  Create an `asyncio` task for each row returned by the query above, passing all necessary parameters to the `_fetch_with_backpressure` function.
    3.  Execute all download tasks concurrently, respecting the semaphore.
    4.  Enqueue all responses (success or failure) to the `writer_worker`, which will save them in the `psa.pncp_raw_responses` table.

**Phase 4: Reconciliation (State Update)**
*   **Objective:** Update the control table based on the data that was actually downloaded in Phase 3.
*   **Actions:**
    1.  This phase is executed after the completion of Phase 3 (i.e., when the write queue is empty).
    2.  Create a `_reconcile_tasks()` function.
    3.  Inside it, select all tasks with `status IN ('FETCHING', 'PARTIAL')`.
    4.  For each task:
        a.  Query the `psa.pncp_raw_responses` table to get the list of pages that were successfully downloaded (`response_code = 200`) for that `endpoint_name` and `data_date`.
        b.  Compare the list of downloaded pages with the task's `missing_pages` list.
        c.  Calculate the new list of `missing_pages`.
        d.  **Update the task:**
            i.   If the new `missing_pages` list is empty, change `status = 'COMPLETE'`.
            ii.  If the list has shrunk but is not empty, change `status = 'PARTIAL'` (or keep `FETCHING`).
            iii. Update the `missing_pages` column with the new list.

---

#### 4. Integration with the User Interface (`rich.Progress`)

The new architecture allows for much richer and more accurate visual feedback.

1.  **Overall Progress:** A main progress bar can show the total progress of the work plan.
    *   `total = SELECT COUNT(*) FROM psa.pncp_extraction_tasks;`
    *   `completed = SELECT COUNT(*) FROM psa.pncp_extraction_tasks WHERE status = 'COMPLETE';`

2.  **Phase-by-Phase Progress:** You can have progress bars for each phase:
    *   **Discovery:** A bar showing the processing of `PENDING` tasks.
    *   **Execution:** A bar showing the number of downloaded pages vs. the total number of missing pages at the beginning of the phase.

3.  **Detailed Progress (Optional):** It is even possible to show the individual progress of each active task (`status = 'FETCHING'`), calculating the progress based on the size of the `missing_pages` list.

---

#### 5. Summary of the Advantages of the New Architecture

*   **Total Resilience:** The script can be interrupted at any time and will resume exactly where it left off.
*   **Idempotency:** Repeated executions do not cause data duplication or unnecessary work.
*   **Transparency and Debugging:** The control table provides a clear view of the state of each part of the process, making it easy to identify failures.
*   **Scalability:** The "work units" model (pages) can be easily parallelized or even distributed among multiple processes/machines.
*   **Maintainability:** The clear separation of phases makes the code cleaner, more organized, and easier to understand and modify.
