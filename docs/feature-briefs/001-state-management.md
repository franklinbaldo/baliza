# Feature Brief: Resumable Extraction & State Management

-   **ID:** 001
-   **Epic:** Resumable Extraction Pipeline
-   **Goal Mapping:** Achieve Full Extraction Resumability
-   **Status:** Ready for Implementation

## 1. User Story

As a data pipeline operator, I want the `baliza extract` command to be fully resumable, so that if the process is interrupted by a network error or crash, I can simply re-run it, and it will automatically continue from where it left off without reprocessing data unnecessarily.

## 2. Acceptance Criteria (BDD-Style)

### Scenario 1: First-time extraction

-   **Given** no previous extraction state exists
-   **When** I run `baliza extract` for a specific date range
-   **Then** the command should process all pages for all days in the range
-   **And** it should create a state record in DuckDB (`baliza_state.extraction_runs`) with a "completed" status
-   **And** it should record all processed time windows in (`baliza_state.cobertura`) as "completed".

### Scenario 2: Extraction is interrupted

-   **Given** an extraction process is running for a 10-day period
-   **And** it is interrupted after successfully completing 5 of the 10 days
-   **Then** the state record for the current run should be marked as "failed" or "incomplete"
-   **And** the coverage information should reflect that 5 days are "completed" and 5 are "not_processed".

### Scenario 3: Resuming an interrupted extraction

-   **Given** a previous extraction run failed, leaving 5 of 10 days incomplete
-   **When** I run `baliza extract` again with the same parameters
-   **Then** the `GapDetector` should identify the 5 "not_processed" days as the highest priority
-   **And** the pipeline should *only* process those 5 remaining days
-   **And** upon successful completion, the original run record should be updated to "completed" (or a new run record is created, and the old one is marked as "resumed")
-   **And** all 10 days in the coverage table should be marked as "completed".

### Scenario 4: New `state` CLI commands

-   **Given** several extraction runs have occurred (some successful, some failed)
-   **When** I run `baliza state show --resource contratos`
-   **Then** I should see a summary of completed, incomplete, and missing time windows.
-   **When** I run `baliza state gaps --resource contratos`
-   **Then** I should see a list of the specific date ranges that are missing or incomplete.
-   **When** I run `baliza state history --resource contratos`
-   **Then** I should see a log of the past extraction runs with their status (completed, failed), duration, and the number of windows processed.

## 3. Data Contracts (Schemas)

The implementation must create and manage the following tables within the DuckDB `baliza_state` schema.

### `baliza_state.extraction_runs`

-   `run_id` (TEXT, PK): Unique identifier for the extraction run (e.g., UUID).
-   `resource_name` (TEXT): The resource being extracted (e.g., "contratos").
-   `status` (TEXT): The current state of the run (e.g., "running", "completed", "failed").
-   `started_at` (TIMESTAMP): When the run was initiated.
-   `completed_at` (TIMESTAMP): When the run finished (successfully or not).
-   `parameters` (JSON): The CLI parameters for the run (e.g., lookback days, date range).

### `baliza_state.cobertura` (Coverage)

-   `window_start` (TIMESTAMP): The start of the time window.
-   `window_end` (TIMESTAMP): The end of the time window.
-   `resource_name` (TEXT): The associated resource.
-   `status` (TEXT): "completed", "incomplete", "not_processed", "suspect".
-   `run_id` (TEXT, FK): The run that processed this window.
-   `last_updated` (TIMESTAMP): The timestamp of the last update to this record.
-   `details` (JSON): Metadata, such as observed page counts, record hashes, etc.

## 4. Observability (Logs/Metrics) Expectations

-   **Structured Logging:** The `extract` command should produce structured logs (e.g., JSON format) indicating:
    -   The start and end of each extraction run.
    -   The number of gaps detected.
    -   Which specific time windows are being processed (e.g., "Processing incomplete window: 2024-10-20 to 2024-10-21").
    -   The outcome of each window (success, failure).
    -   A summary upon completion, including total windows processed and total time taken.

## 5. Out-of-Scope

-   **Automatic healing of "suspect" windows:** The initial implementation should focus on "incomplete" and "not_processed" gaps. Handling "suspect" data (e.g., where page counts change) will be a future feature.
-   **Multi-resource gap detection:** The initial implementation can assume it only needs to manage the `contratos` resource.

## 6. Risk Notes

-   **Schema Evolution:** The state management schemas may need to evolve. The initial implementation should be done in a way that is easy to migrate (e.g., using simple DDL statements that can be versioned).
-   **Concurrency:** The current CLI model is single-process. The design does not need to handle concurrent writes to the state database, which simplifies the logic.
-   **Performance:** For very large backfills (many years), querying the coverage table for gaps could become slow. The queries should be designed with performance in mind (e.g., using appropriate indexes on timestamp columns).
