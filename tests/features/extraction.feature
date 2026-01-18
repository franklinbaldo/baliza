Feature: Resumable Data Extraction
  As a data engineer
  I want to run a data extraction pipeline that can resume from failures
  So that I can ensure data integrity and avoid re-processing completed work

  Scenario: Successful extraction for a date range
    Given a clean DuckDB database
    And a mock PNCP API that will succeed
    When I run the baliza extract command for the dates "2024-01-01" to "2024-01-02"
    Then the command should succeed
    And the database should contain "1000" contracts for the resource "contratos"

  Scenario: Failed extraction due to API error
    Given a clean DuckDB database
    And a mock PNCP API that will fail on the second call
    When I run the baliza extract command for the dates "2024-01-01" to "2024-01-02"
    Then the command should fail
    And the database should contain "500" contracts for the resource "contratos"

  Scenario: Resumed extraction after a failure
    Given a database with a partially completed extraction
    And a mock PNCP API that will succeed
    When I run the baliza extract command for the dates "2024-01-01" to "2024-01-02"
    Then the command should succeed
    And the database should contain "1000" contracts for the resource "contratos"
