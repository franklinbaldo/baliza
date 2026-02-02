# Feature Brief: CLI Expansion (State & Backfill)

-   **ID:** 003
-   **Epic:** CLI Maturity & Observability
-   **Goal Mapping:** Excellent Developer/Operator Experience
-   **Status:** Ready for Implementation

## 1. User Story

As a Baliza operator, I want to be able to inspect the state of my data extraction process and perform historical backfills through simple CLI commands, so that I can ensure data completeness and manage historical data easily.

## 2. Acceptance Criteria (BDD-Style)

### Scenario 1: Show extraction state
-   **Given** I have a DuckDB database with some extraction history
-   **When** I run `baliza state show`
-   **Then** I should see a summary of:
    - Total records extracted per resource.
    - Coverage range (start date to end date).
    - Number of completed, incomplete, and missing windows.

### Scenario 2: List coverage gaps
-   **Given** my data store has missing date ranges (gaps)
-   **When** I run `baliza state gaps`
-   **Then** I should see a list of all identified gaps (date ranges) that need extraction.

### Scenario 3: View run history
-   **Given** several extraction runs have been executed
-   **When** I run `baliza state history`
-   **Then** I should see a table showing:
    - Run ID
    - Start time
    - Status (completed, failed)
    - Rows extracted
    - Duration

### Scenario 4: Perform backfill
-   **When** I run `baliza backfill 2024-01 2024-03`
-   **Then** the tool should process each month (Jan, Feb, Mar 2024) sequentially.
-   **And** it should not reuse the incremental state (forcing a full refresh for those months).
-   **And** it should update the coverage manifesto upon completion.

## 3. Implementation Details

- **Command Group:** Use `typer.Typer()` to create a `state` subcommand group in `src/baliza/cli_simple.py`.
- **Backend Logic:** Use the existing `baliza_state` tables (`coverage`, `runs`, `extraction_checkpoint`) in DuckDB.
- **Formatting:** Use `rich` for beautiful tables and panels, following the existing style in `cli_simple.py`.
- **Backfill Logic:** The `backfill` command should iterate through months and call the extraction logic for each month.

## 4. Out-of-Scope
- Advanced filtering in `state history`.
- Exporting state to other formats.

## 5. Risks
- Large history tables might slow down `state history`.
- Backfill overlapping with daily incremental runs needs to handle merge correctly (existing `INSERT OR IGNORE` or `INSERT OR REPLACE` should handle this).
