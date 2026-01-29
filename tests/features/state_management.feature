Feature: State Management
  As a data engineer,
  I want to inspect the state of the data extraction process,
  So that I can diagnose issues and understand the coverage of the local data store.

  Scenario: Show the state of the data extraction process
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

  Scenario: List the gaps in the data extraction process
    Given the database contains the following coverage windows:
      | resource  | window_start        | window_end          | status     |
      | contratos | 2024-01-01 00:00:00 | 2024-01-01 23:59:59 | complete   |
      | contratos | 2024-01-04 00:00:00 | 2024-01-04 23:59:59 | complete   |
    When I run the command "state gaps --resource contratos --start 2024-01-01 --end 2024-01-05"
    Then I should see a list of the missing daily windows:
      | 2024-01-02 |
      | 2024-01-03 |

  Scenario: Show the history of the data extraction process
    Given the database contains the following extraction runs:
      | run_id | start_time          | end_time            | status    | windows_processed | rows_extracted |
      | run-1  | 2024-01-01 10:00:00 | 2024-01-01 10:30:00 | completed | 10                | 1000           |
      | run-2  | 2024-01-02 11:00:00 | 2024-01-02 11:15:00 | failed    | 5                 | 50             |
    When I run the command "state history"
    Then I should see a table of the previous extraction runs:
      | Run ID | Start Time          | Status    | Windows Processed | Rows Extracted |
      | run-1  | 2024-01-01 10:00:00 | completed | 10                | 1000           |
      | run-2  | 2024-01-02 11:00:00 | failed    | 5                 | 50             |

  Scenario: Resumable extraction process
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
