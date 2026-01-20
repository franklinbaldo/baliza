Feature: Resumable Extraction
  As a data pipeline operator
  I want baliza to resume extractions from the last completed window
  So that temporary failures do not force me to re-process data

  Scenario: Extraction resumes after a failure
    Given a clean DuckDB database
    And the PNCP API will fail for dates after "2024-01-02"
    When I run "baliza extract --start 2024-01-01 --end 2024-01-03"
    Then the command should fail
    And the database should contain data for "2024-01-01"
    And the database should contain data for "2024-01-02"
    And the database should not contain data for "2024-01-03"
    And the coverage table should mark "2024-01-01" as complete
    And the coverage table should mark "2024-01-02" as complete
    And the coverage table should mark "2024-01-03" as failed

  Scenario: Running extract again completes the failed windows
    Given the previous extraction failed for "2024-01-03"
    And the PNCP API is now working correctly
    When I run "baliza extract --start 2024-01-01 --end 2024-01-03"
    Then the command should succeed
    And the database should contain data for "2024-01-03"
    And the coverage table should mark "2024-01-03" as complete
