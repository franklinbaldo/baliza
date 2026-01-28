# Feature Brief: Resumable Extraction Pipeline

**This brief outlines the acceptance criteria for a truly resumable and resilient data extraction pipeline.**

## 1. Goal Mapping

This feature directly addresses the following primary goal from `docs/MASTERPLAN.md`:

-   **Goal 1: Achieve Full Extraction Resumability:** Implement a robust state management system that makes the extraction process fully resumable and idempotent, recovering gracefully from network failures or API instability.

It also improves:
-   **Goal 4: Actionable Data Quality Monitoring:** By ensuring that transient errors don't lead to silent data gaps.

## 2. User Story

**As a** Data Pipeline Operator,
**I want** the `baliza extract` command to automatically recover from transient API errors and continue from where it left off,
**So that** the pipeline can run unattended without manual intervention, and data integrity is preserved even during periods of API instability.

## 3. Acceptance Criteria (BDD-Style)

The following BDD scenarios will be used to validate this feature. They will replace the existing scenarios in `tests/features/resilience.feature`.

```gherkin
Feature: Resilient and Resumable Extraction

  Background:
    Given a clean local data store
    And a PNCP API that will fail transiently

  @resilience @smoke
  Scenario: The extract command recovers from a transient API error
    Given the PNCP API will return a 500 error for the first half of a date range
    And the PNCP API will succeed for the second half of the date range
    When I run the "baliza extract" command for the full date range
    Then the command should eventually succeed
    And the final dataset should contain all records for the full date range
    And the final dataset should not contain duplicate records
    And the run history should show one failed run and one successful run

  @resilience @edge-case
  Scenario: The extract command gives up after multiple consecutive failures
    Given the PNCP API will consistently return a 500 error
    When I run the "baliza extract" command
    Then the command should fail after a reasonable number of retries
    And the error message should clearly indicate a persistent failure
    And the state history should log the multiple failed attempts
```

## 4. Data Contracts

-   **State Database:** The `baliza.duckdb` file must contain tables that track the state of extraction windows (e.g., `baliza_state.extraction_runs`, `baliza_state.windows`).
    -   `extraction_runs` must have a `status` column (e.g., 'running', 'failed', 'completed').
    -   Windows must be tracked with their start and end dates and a status (e.g., 'pending', 'incomplete', 'completed').

## 5. Observability

-   **Logs:** The CLI output must clearly indicate when a failure is detected, when a retry is being attempted, and when a recovery is successful.
-   **State Commands:** The `baliza state history` command should accurately reflect the failed attempts and the final successful run.

## 6. Out-of-Scope

-   **Complex Retry Logic:** This feature will implement a simple retry mechanism. More complex strategies (e.g., exponential backoff with jitter) are out of scope for this iteration.
-   **Non-API Errors:** This brief specifically covers recovery from transient HTTP 5xx errors from the PNCP API. Recovery from other error types (e.g., disk full, database corruption) is not covered.

## 7. Risk Notes

-   **State Corruption:** The primary risk is that a complex failure scenario could lead to a corrupted or inconsistent state in the `baliza.duckdb` file, preventing future runs from succeeding. The implementation must ensure that state transitions are atomic where possible.
-   **API Rate Limiting:** Aggressive retry logic could trigger rate limiting on the PNCP API. The implementation should use a reasonable, fixed delay between retries.

## 8. Checklist for Implementer

-   **Target file for step definitions:** `tests/step_defs/test_resilience.py`
-   **Action:** Implement the necessary step definitions to make the scenarios in `tests/features/resilience.feature` pass.
-   **Verification command:** `uv run pytest tests/features/resilience.feature`
-   **Expected outcome:** The tests should initially fail (as the steps are not yet implemented). After implementation, all tests in this feature file should pass.
