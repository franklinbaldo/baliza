# Feature Brief: Resumable Extraction Pipeline

## 1. Goal Mapping

- **Master Plan Goal:** 1. Achieve Full Extraction Resumability
- **Epic:** Epic 1: Resumable Extraction Pipeline
- **User Story:** As an operator of the Baliza CLI, I want the data extraction process to be fully resumable, so that I can recover from failures without losing progress or reprocessing data unnecessarily.

## 2. Acceptance Criteria (BDD)

The primary acceptance criteria are defined in the BDD feature file:
[`tests/features/resumable_extraction.feature`](../../tests/features/resumable_extraction.feature)

The implementer is expected to create step definitions for these scenarios and ensure they pass.

## 3. Data Contracts & State Management

The core of this feature is a new state management system. The state will be stored within the DuckDB file (`baliza.duckdb` by default) in a dedicated schema named `baliza_state`.

### `baliza_state.janelas` (Windows) Table Schema

This table tracks the status of each discrete time window (typically one day) that has been processed.

| Column Name      | Data Type | Description                                                                 | Example                               |
| ---------------- | --------- | --------------------------------------------------------------------------- | ------------------------------------- |
| `resource_name`  | VARCHAR   | The name of the resource being extracted (e.g., "contratos"). Primary Key 1. | "contratos"                           |
| `start_date`     | DATE      | The inclusive start date of the window. Primary Key 2.                      | 2024-01-01                            |
| `end_date`       | DATE      | The exclusive end date of the window.                                       | 2024-01-02                            |
| `status`         | VARCHAR   | The status of the window (`completed`, `incomplete`, `failed`).             | "completed"                           |
| `last_updated`   | TIMESTAMP | Timestamp of the last update to this record.                                | 2024-11-04T10:30:00Z                  |
| `commit_hash`    | VARCHAR   | Git commit hash of the code version that processed this window.             | "a1b2c3d"                             |

### `baliza_state.cobertura` (Coverage) Table Schema

This table stores page-level metadata to allow for fine-grained verification.

| Column Name             | Data Type | Description                                                                 | Example                                     |
| ----------------------- | --------- | --------------------------------------------------------------------------- | ------------------------------------------- |
| `resource_name`         | VARCHAR   | The name of the resource.                                                   | "contratos"                                 |
| `start_date`            | DATE      | The start date of the window this page belongs to.                          | 2024-01-01                                  |
| `page_number`           | INTEGER   | The page number retrieved.                                                  | 1                                           |
| `total_pages_observed`  | INTEGER   | The `totalPaginas` value reported by the API in the response for this page. | 15                                          |
| `item_hashes`           | VARCHAR[] | An array of hashes of the primary keys (`numeroControlePNCP`) on this page.   | ["hash1", "hash2", ...]                   |
| `loaded_at`             | TIMESTAMP | Timestamp when this page was loaded.                                        | 2024-11-04T10:31:00Z                  |

## 4. Implementation Checklist

The following components need to be created or modified:

1.  **`src/baliza/state/manager.py` (New File):**
    -   Create a `StateManager` class.
    -   It should initialize the `baliza_state` schema and tables in the DuckDB connection if they don't exist.
    -   Provide methods to:
        -   `get_completed_windows(resource_name)`: Returns a list of `(start, end)` date ranges.
        -   `update_window_status(resource_name, start_date, end_date, status)`: Inserts or updates a window's status.
        -   `merge_contiguous_windows(resource_name)`: A utility to merge adjacent windows with the same status.

2.  **`src/baliza/state/gap_detector.py` (New File):**
    -   Create a `GapDetector` class.
    -   It should take the overall desired date range and the completed windows from the `StateManager`.
    -   Provide a method `calculate_gaps()` that returns a list of date ranges that need to be processed. This should include:
        -   Incomplete/failed windows from previous runs.
        -   Gaps between completed windows.
        -   The new date range defined by the lookback period.

3.  **`src/baliza/cli.py` (Modification):**
    -   Modify the `extract` command.
    -   Instantiate the `StateManager` and `GapDetector`.
    -   Use the `GapDetector` to determine the list of windows to process.
    -   Loop through the calculated gaps, running the `dlt` pipeline for each one.
    -   On successful completion of a window, update its status to `completed` using the `StateManager`.
    -   If a `dlt` run fails, catch the exception and update the window status to `incomplete` or `failed`.

4.  **`src/baliza/pipelines/pncp.py` (Modification):**
    -   Ensure the pipeline can be dynamically called with different `start_date` and `end_date` parameters for each gap.

5.  **`tests/step_defs/test_resumable_extraction.py` (New File):**
    -   Implement the step definitions for `resumable_extraction.feature`.
    -   Use a temporary DuckDB file for test isolation.
    -   Use mocking (`pytest-mock`) and VCR cassettes (`pytest-vcr`) to simulate API success and failure conditions.

## 5. Out of Scope for this Iteration

-   Implementing the `state` CLI subcommands (`show`, `gaps`, `history`). These will be handled in a subsequent feature brief.
-   The `verify` command logic. It will be updated to use the new state tables later.
-   Support for any resource other than `contratos`.

## 6. Risks & Assumptions

-   **Assumption:** The primary key `(resource_name, start_date)` is sufficient to uniquely identify a window in the `janelas` table.
-   **Risk:** Large backfills could generate a very large list of gaps. The implementation should process them iteratively and not hold all data in memory.
-   **Risk:** Concurrent writes to the DuckDB file are not supported. The CLI architecture (single process) mitigates this, but it's a constraint to be aware of.
