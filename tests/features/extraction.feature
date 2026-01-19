Feature: Data Extraction
  As a data engineer
  I want to extract data from the PNCP API
  So that I can have the data available locally for analysis

  Scenario: Basic extraction works
    Given a clean DuckDB database
    When I run "baliza extract --start 2024-01-01 --end 2024-01-01"
    Then the data should be saved to the database
    And the command should exit successfully

  Scenario: Incremental extraction doesn't duplicate data
    Given I have previously extracted data for 2024-01-01
    When I run "baliza extract --start 2024-01-01 --end 2024-01-02"
    Then the database should not contain duplicate records
    And the 2024-01-01 data should be updated, not duplicated
