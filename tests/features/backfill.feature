@tier1 @backfill
Feature: Historical Data Backfill
  As a data researcher,
  I want to extract large blocks of historical data month-by-month,
  So that I can consolidate a complete dataset for analysis.

  Scenario: Backfill processes full months sequentially
    Given a clean local data store
    When I run the "backfill" command from "2024-01" to "2024-02"
    Then the command should extract data for January 2024
    And the command should extract data for February 2024
    And the coverage table should show both months as "complete"

  Scenario: Backfill creates a separate pipeline run for each month
    Given a clean local data store
    When I run the "backfill" command for "2024-03"
    Then the run history should show a completed run for the "backfill" pipeline
    And the resource should be "contratos"
