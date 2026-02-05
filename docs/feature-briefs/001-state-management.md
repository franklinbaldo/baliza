# Feature Brief: State Management CLI

-   **ID:** 001
-   **Epic:** Unified State & Observability
-   **Goal Mapping:** Excellent Developer/Operator Experience
-   **Status:** Ready for Implementation

## 1. User Story

As a data pipeline operator, I want a unified `baliza state` command to inspect the status, coverage, and history of my extractions, so that I can easily identify gaps and monitor the health of the pipeline without running multiple fragmented commands.

## 2. Acceptance Criteria (BDD-Style)

### Scenario 1: `baliza state show`
-   **Given** a DuckDB database with extraction data
-   **When** I run `baliza state show`
-   **Then** I should see a summary of the total contracts extracted
-   **And** the date range of the data in the buffer
-   **And** the number of pending checkpoints
-   **Note:** This consolidates the existing `status` and `buffer-stats` functionality.

### Scenario 2: `baliza state gaps`
-   **Given** a specific date range and resource
-   **When** I run `baliza state gaps --start YYYY-MM-DD --end YYYY-MM-DD`
-   **Then** the command should query the `baliza_state.coverage` table
-   **And** list all dates within the range that do not have a "complete" status
-   **Note:** This consolidates the logic from `verify`.

### Scenario 3: `baliza state history`
-   **Given** previous extraction runs recorded in `baliza_state.runs`
-   **When** I run `baliza state history`
-   **Then** I should see a table with the last N runs, showing:
    -   Run ID
    -   Started At
    -   Duration
    -   Status (completed/failed)
    -   Rows Extracted

## 3. Data Contracts (Schemas)

The implementation must use the existing tables in the `baliza_state` schema:

### `baliza_state.runs` (Already defined in `extractor.py`)
- `run_id` (VARCHAR PRIMARY KEY)
- `resource` (VARCHAR)
- `pipeline_name` (VARCHAR)
- `started_at` (TIMESTAMP)
- `finished_at` (TIMESTAMP)
- `status` (VARCHAR)
- `windows_completed` (INTEGER)
- `windows_failed` (INTEGER)
- `rows_extracted` (INTEGER)
- `error_message` (VARCHAR)

### `baliza_state.coverage` (Already defined in `extractor.py`)
- `resource` (VARCHAR)
- `window_start` (TIMESTAMP)
- `window_end` (TIMESTAMP)
- `status` (VARCHAR)
- `total_paginas` (INTEGER)
- `rows_extracted` (INTEGER)
- `extracted_at` (TIMESTAMP)

## 4. Implementation Notes

- Use the `typer` sub-command pattern: `state = typer.Typer(); app.add_typer(state, name="state")`.
- Use the `rich` library for formatted output (Tables, Panels).
- Maintain backward compatibility by keeping `status` and `verify` as aliases or simple wrappers if needed, but prioritize the new `state` structure.

## 5. Out-of-Scope
- Automatic re-extraction of gaps (this remains in `extract`).
- Advanced filtering of history (start with the last 10-20 runs).

## 6. Risk Notes
- Ensure `PNCPExtractor` properly populates the `runs` table (currently it seems to only define the schema but may not be recording every run yet).
