Feature: Core data extraction
  As a data engineer,
  I want to extract data for a specific date range,
  So that I can create a local dataset for analysis.

  @tier0
  Scenario: Simple extraction for a date range
    Given the PNCP API has 2 contracts for "2024-10-01"
    When I run the extract command for "2024-10-01" to "2024-10-01"
    Then the database should contain 2 contracts
