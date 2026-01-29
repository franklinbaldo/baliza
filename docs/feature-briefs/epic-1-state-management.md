# Feature Brief: State Management & Resumable Extraction

**Epic:** [Epic 1: Resumable Extraction Pipeline](docs/MASTERPLAN.md)
**Goal:** Achieve Full Extraction Resumability
**Status:** Ready for Implementation

## 1. User Story

As a Data Engineer operating the Baliza CLI,
I want the extraction process to be fully resumable and state-aware,
So that I can recover from failures without losing work, and I can clearly inspect the completeness of my local data store at any time.

## 2. Acceptance Criteria

The implementation is complete when all scenarios in `tests/features/state_management.feature` pass.

### Scenario: Show the state of the data extraction process
```gherkin
Given the database contains the following coverage windows:
  | resource  | window_start        | window_end          | status     |
  | contratos | 2024-01-01 00:00:00 | 2024-01-01 23:59:59 | complete   |
  | contratos | 2024-01-03 00:00:00 | 2024-01-03 23:59:59 | incomplete |
  | atas      | 2024-01-02 00:00:00 | 2024-01-02 23:59:59 | complete   |
When I run the command "state show"
Then I should see a table summarizing the state for each resource:
  | Resource  | Complete Windows | Incomplete Windows | Missing Windows (since first seen) |
  | contratos | 1                | 1                  | 1                                  |
  | atas      | 1                | 0                  | 0                                  |
```

### Scenario: List the gaps in the data extraction process
```gherkin
Given the database contains the following coverage windows:
  | resource  | window_start        | window_end          | status     |
  | contratos | 2024-01-01 00:00:00 | 2024-01-01 23:59:59 | complete   |
  | contratos | 2024-01-04 00:00:00 | 2024-01-04 23:59:59 | complete   |
When I run the command "state gaps --resource contratos --start 2024-01-01 --end 2024-01-05"
Then I should see a list of the missing daily windows:
  | 2024-01-02 |
  | 2024-01-03 |
```

### Scenario: Show the history of the data extraction process
```gherkin
Given the database contains the following extraction runs:
  | run_id | start_time          | end_time            | status    | windows_processed | rows_extracted |
  | run-1  | 2024-01-01 10:00:00 | 2024-01-01 10:30:00 | completed | 10                | 1000           |
  | run-2  | 2024-01-02 11:00:00 | 2024-01-02 11:15:00 | failed    | 5                 | 50             |
When I run the command "state history"
Then I should see a table of the previous extraction runs:
  | Run ID | Start Time          | Status    | Windows Processed | Rows Extracted |
  | run-1  | 2024-01-01 10:00:00 | completed | 10                | 1000           |
  | run-2  | 2024-01-02 11:00:00 | failed    | 5                 | 50             |
```

### Scenario: Resumable extraction process
```gherkin
Given the database contains the following coverage windows:
  | resource  | window_start        | window_end          | status     |
  | contratos | 2024-01-01 00:00:00 | 2024-01-01 23:59:59 | complete   |
  | contratos | 2024-01-03 00:00:00 | 2024-01-03 23:59:59 | incomplete |
And today's date is "2024-01-05"
When I run the command "extract --lookback-days 3"
Then the extractor should prioritize the following windows:
  | 2024-01-03 | # Incomplete
  | 2024-01-02 | # Missing
  | 2024-01-04 | # Recent
```

## 3. Data Contracts

The state will be managed in a dedicated `baliza_state` schema within the DuckDB file.

### `baliza_state.coverage`
This table tracks the status of each daily time window for each resource.

```sql
CREATE TABLE IF NOT EXISTS baliza_state.coverage (
    resource            VARCHAR,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    status              VARCHAR, -- ('complete', 'incomplete', 'failed')
    last_updated_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (resource, window_start)
);
```

### `baliza_state.extraction_runs`
This table logs each execution of the `extract` command.

```sql
CREATE TABLE IF NOT EXISTS baliza_state.extraction_runs (
    run_id              VARCHAR,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              VARCHAR, -- ('running', 'completed', 'failed')
    windows_processed   INTEGER,
    rows_extracted      INTEGER,
    error_message       VARCHAR,
    PRIMARY KEY (run_id)
);
```

## 4. Observability

- The `state` commands should output clean, human-readable tables using `rich`.
- The `extract` command should log which windows it is targeting (incomplete, missing, recent) at the start of a run.

## 5. Out of Scope

- Support for time windows other than a calendar day (e.g., hourly).
- Automatic repair of "suspicious" windows (e.g., where page counts have changed). This will be handled in a future "verify" epic.
- Complex filtering or sorting on the `state history` command.

## 6. Risk Notes

- The implementation will require careful handling of database connections to avoid concurrency issues, especially during test setup and teardown.

## 7. Checklist for Implementer Agent

- [ ] Implement the `state` subcommand in `src/baliza/cli_simple.py`.
- [ ] Implement the `state show` command.
- [ ] Implement the `state gaps` command.
- [ ] Implement the `state history` command.
- [ ] Implement the state management and window detection logic (a `StateManager` class is recommended).
- [ ] Integrate the state management logic into the `extract` command to prioritize windows as per the BDD scenario.
- [ ] Ensure all BDD tests in `test_state_management.py` pass.
- [ ] Update `README.md` to accurately reflect the new `state` commands.
