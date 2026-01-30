# Feature Brief: State Management CLI

**Goal:** Provide a dedicated `state` subcommand group in the Baliza CLI to improve observability of the extraction process.

## User Story

As a data operator,
I want to use `baliza state show`, `baliza state gaps`, and `baliza state history`,
So that I can easily monitor the progress and integrity of my data extraction pipeline.

## Acceptance Criteria (BDD-style)

### 1. Show State Summary
**Given** a DuckDB database with some extraction coverage
**When** I run `baliza state show`
**Then** I should see a summary table including:
- Total rows in the `contratos` table.
- The date range covered by the data.
- A count of identified gaps.
- The number of days with data.

### 2. List Gaps
**Given** a DuckDB database with missing data windows
**When** I run `baliza state gaps`
**Then** I should see a list of date ranges that are currently missing from the local store.

### 3. Show Run History
**Given** a DuckDB database with previous extraction runs recorded in `baliza_state.runs`
**When** I run `baliza state history`
**Then** I should see a table of recent runs including:
- Run ID
- Start Time
- Duration
- Status (Success/Failure)
- Rows Extracted
- Error Message (if failed)

## Data Contracts

- Tables in `baliza_state` schema: `coverage`, `runs`, `extraction_checkpoint`.
- `runs` table schema: `run_id`, `resource`, `started_at`, `finished_at`, `status`, `rows_extracted`, `error_message`.

## Out-of-Scope

- Automated gap-filling (this is for the `extract` command).
- Detailed data profiling (anomaly detection).

## Risk Notes

- Ensure `duckdb` connections are properly managed and closed to avoid locking.
