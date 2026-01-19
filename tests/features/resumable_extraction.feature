Feature: Resumable Extraction Pipeline
  As an operator of the Baliza CLI,
  I want the data extraction process to be fully resumable,
  so that I can recover from failures without losing progress or reprocessing data unnecessarily.

  Background:
    Given a clean DuckDB state file "test_resumable.duckdb"
    And a mocked PNCP API server

  Scenario: Successful first-time extraction
    Given the PNCP API will successfully return data for the date range "2024-01-01" to "2024-01-03"
    When I run the "extract" command for "3" days
    Then the command should succeed
    And the state file should record "3" completed windows from "2024-01-01" to "2024-01-03"
    And "3" successful API calls should have been made

  Scenario: Resuming a failed extraction
    Given the PNCP API will fail on the second day of a "3" day extraction from "2024-01-01"
    When I run the "extract" command for "3" days
    Then the command should fail
    And the state file should record "1" completed window for "2024-01-01"
    And the state file should record "1" incomplete window for "2024-01-02"

    Given the PNCP API will now succeed for the remaining "2" days
    When I run the "extract" command again for "3" days
    Then the command should succeed
    And the state file should record "3" completed windows from "2024-01-01" to "2024-01-03"
    And only "2" new API calls should have been made

  Scenario: Filling a gap between two successful extractions
    Given a successful extraction has been run for "2024-01-01" to "2024-01-02"
    And another successful extraction has been run for "2024-01-04" to "2024-01-05"
    And the state file reflects these completed windows, leaving a gap on "2024-01-03"
    And the PNCP API will return data for "2024-01-03"
    When I run the "extract" command
    Then the command should succeed
    And the state file should now record a single, merged, completed window from "2024-01-01" to "2024-01-05"
    And only "1" new API call for the missing day should have been made

  Scenario: No new data to extract
    Given a successful extraction has been run for the last "3" days
    And the state file is up-to-date
    When I run the "extract" command for "3" days
    Then the command should succeed
    And the output should indicate that no new data was extracted
    And zero new API calls should have been made
