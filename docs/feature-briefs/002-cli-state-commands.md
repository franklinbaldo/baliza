# Feature Brief: CLI State Management Commands

-   **ID:** 002
-   **Epic:** Resumable Extraction Pipeline
-   **Goal Mapping:** Achieve Full Extraction Resumability, Excellent Developer/Operator Experience
-   **Status:** Ready for Implementation

## 1. User Story

As a data pipeline operator, I want a dedicated set of `state` commands so that I can easily monitor the progress of extractions, identify coverage gaps, and view the history of previous runs without having to query the DuckDB database manually.

## 2. Acceptance Criteria (BDD-Style)

### Scenario: Show coverage summary
- **Given** I have a database with extraction records
- **When** I run `baliza state show --resource contratos`
- **Then** I should see a formatted summary (e.g., a Rich Panel) showing:
    - Total contracts extracted.
    - Date range covered.
    - Number of completed, incomplete, and missing days.

### Scenario: List specific gaps
- **Given** I have missing windows in my extraction coverage
- **When** I run `baliza state gaps --resource contratos`
- **Then** I should see a clear list of date ranges that are missing or incomplete, along with instructions on how to fill them (e.g., suggested `extract` command).

### Scenario: View run history
- **Given** several extraction runs have occurred
- **When** I run `baliza state history --resource contratos`
- **Then** I should see a table showing the last N runs, including:
    - Run ID
    - Start/End time
    - Status (completed/failed)
    - Rows extracted
    - Error message (if applicable)

## 3. Implementation Notes

-   **Namespace:** These commands should be grouped under a `state` subcommand in Typer.
-   **Logic Reuse:**
    -   `state show` can build upon the existing `status` command logic.
    -   `state gaps` can build upon the existing `verify` command logic.
-   **Output:** Use the `rich` library for all outputs to maintain consistency with the Tier 2 (Operator Experience) goals.
-   **Testing:** Update `tests/step_defs/test_state_management.py` to use these real CLI commands instead of mocks.

## 4. Out-of-Scope

-   Auto-healing of gaps (this remains in the `extract` command logic).
-   Advanced filtering of history (e.g., by user or parameter).
