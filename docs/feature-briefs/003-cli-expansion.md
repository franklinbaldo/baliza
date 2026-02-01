# Feature Brief: Core CLI Expansion

-   **ID:** 003
-   **Epic:** Core CLI Expansion
-   **Goal Mapping:** Actionable Data Quality Monitoring, Excellent Developer/Operator Experience
-   **Status:** Ready for Implementation

## 1. User Story

As a data engineer, I want to have clear, dedicated CLI commands to inspect the state of my data extraction and to perform historical backfills, so that I can easily monitor coverage and manage large-scale data ingestion.

## 2. Acceptance Criteria (BDD-Style)

### Scenario 1: State Command Group Implementation
-   **Given** the current CLI structure
-   **When** I run `baliza state --help`
-   **Then** I should see subcommands: `show`, `gaps`, `history`
-   **And** the existing `status` command should be reachable via `baliza state show` (optionally keeping `baliza status` as an alias)
-   **And** the existing `verify` logic for gaps should be accessible via `baliza state gaps`.

### Scenario 2: Historical Backfill Command
-   **Given** a need to extract data for multiple months
-   **When** I run `baliza backfill --start-month 2024-01 --end-month 2024-03`
-   **Then** the command should iterate month-by-month
-   **And** it should call the extraction logic for each month separately.

### Scenario 3: Real BDD Tests
-   **Given** the `tests/features/state_management.feature`
-   **When** the tests are run
-   **Then** they should invoke the real `baliza state show`, `baliza state gaps`, and `baliza state history` commands
-   **And** they should NOT use workarounds like mapping to `status` or mocking output in the step definitions.

## 3. Data Contracts

### Commands

| Command | Arguments/Options | Source Table/Logic |
|---------|-------------------|-------------------|
| `baliza state show` | `--resource` | `baliza_raw.contratos` + `baliza_state.coverage` |
| `baliza state gaps` | `--resource`, `--start`, `--end` | `baliza_state.coverage` (logic from current `verify`) |
| `baliza state history`| `--resource`, `--limit` | `baliza_state.runs` |
| `baliza backfill` | `--start-month`, `--end-month` | Iterative call to `PNCPExtractor.extract` |

## 4. Technical Constraints

-   Use `typer.Typer()` for the `state` subcommand group.
-   Maintain backward compatibility for `baliza extract`, `baliza verify`, and `baliza status` if possible (as aliases or keeping them at root).
-   Use `rich` for formatting output (tables, panels) to match the existing CLI style.
-   The `backfill` command should handle failures gracefully (e.g., stopping at the first failed month or reporting a summary of failures).

## 5. Out-of-Scope

-   Advanced anomaly detection (e.g., record count spikes) is deferred to Epic 2.
-   Support for resources other than `contratos` (though the code should be generic enough to allow it later).

## 6. Implementation Notes

-   The `tests/step_defs/test_state_management.py` currently contains significant "cheating" (mapping commands and mocking Rich output). This must be cleaned up as part of this task.
-   The `PNCPExtractor` already maintains a `baliza_state.runs` table which should be the source for `state history`.
