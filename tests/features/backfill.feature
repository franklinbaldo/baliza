Feature: Backfill Command
  As a data engineer
  I want to backfill data for a specific period
  So that I can ensure historical data is complete

  Scenario: Backfilling data for a given month
    Given a clean DuckDB database
    When I run the baliza backfill command for a specific month
    Then the data for that month should be extracted and saved to the DuckDB database
