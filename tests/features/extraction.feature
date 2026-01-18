Feature: Resilient Data Extraction
  As a data pipeline operator,
  I want the `baliza extract` command to be reliable and efficient,
  so that I can build a complete and accurate data archive.

  Scenario: Basic extraction for a specific date range
    Given a clean DuckDB database
    When I run "baliza extract" from "2024-01-01" to "2024-01-03"
    Then the command should succeed
    And the coverage table should contain 1 completed time windows

  Scenario: Resuming an interrupted extraction
    Given a previous extraction run for the last 10 days was interrupted
    And the coverage table shows 5 completed windows and 5 missing windows
    When I run "baliza extract" for the last 10 days again
    Then the command should succeed
    And the pipeline should only process the 5 missing windows
    And the coverage table should now contain 10 completed time windows

  Scenario: Applying lookback to re-extract recent data
    Given a successful extraction has been run for the last 5 days
    When I run "baliza extract" with a lookback of 2 days
    Then the pipeline should re-process the last 2 days of data
    And the coverage table should still contain 5 completed time windows

  Scenario: Automatically detecting and filling gaps
    Given the coverage table has a gap of 3 missing days between two completed windows
    When I run "baliza extract"
    Then the pipeline should identify and process the 3 missing days
    And the coverage table should no longer contain any gaps
