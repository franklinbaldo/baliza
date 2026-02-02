@tier1
Feature: Historical Backfill
  As a data pipeline operator,
  I want to process historical data month-by-month,
  So that I can build a complete and deterministic archive of past procurement data.

  Scenario: Backfill a single month
    Given a clean local data store
    When I run the backfill command for "2024-01" to "2024-01"
    Then the system should extract data for the entire month of January 2024
    And the coverage state should reflect that "2024-01" is complete

  Scenario: Backfill multiple months sequentially
    Given a clean local data store
    When I run the backfill command for "2023-11" to "2024-01"
    Then the system should process November 2023, December 2023, and January 2024
    And the data for all three months should be present in the data store

  Scenario: Invalid month format
    When I run the backfill command with an invalid month "2024-1"
    Then the command should fail with a validation error
    And it should explain that the format must be YYYY-MM

  Scenario: Backfill updates coverage manifesto
    Given a clean local data store
    When I run the backfill command for "2024-02" to "2024-02"
    Then a run record should be created in the history
    And the coverage table should have entries for February 2024
