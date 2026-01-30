# Feature Brief: Autonomous Extraction

**Goal:** Upgrade the `baliza extract` command to be self-configuring, reducing the need for manual date range parameters.

## User Story

As a data engineer,
I want to run `baliza extract` without parameters,
So that the tool automatically identifies and fills missing data gaps using its own state and a default lookback.

## Acceptance Criteria (BDD-style)

### 1. Default Autonomous Behavior
**Given** a DuckDB database with a known "last successful extraction" date
**When** I run `baliza extract` without `--start` or `--end`
**Then** the tool should:
- Calculate `start_date` as `last_success_date - lookback_days` (default lookback: 3 days).
- Calculate `end_date` as today.
- Perform the extraction for this range.

### 2. Manual Override
**Given** I want to extract a specific range
**When** I run `baliza extract --start 2024-01-01 --end 2024-01-31`
**Then** the tool should ignore its state and perform the requested extraction.

## Data Contracts

- `baliza_state.coverage` table should be updated after every successful run.
- `PNCPExtractor.extract` should continue to handle the core fetching logic.

## Out-of-Scope

- Complex scheduling (handled by external orchestrators like GitHub Actions).
- Multi-endpoint extraction in a single command (focus on one resource at a time for now).

## Risk Notes

- If no previous state exists, a default start date (e.g., 3 days ago) should be used.
- Avoid extremely large ranges by default to prevent API rate limiting or long-running tasks.
