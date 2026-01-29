Feature: Idempotent Data Extraction
  As a data engineer,
  I want to ensure that re-running extractions for overlapping date ranges
  does not introduce duplicate data.

  @tier0
  Scenario: Extracting overlapping date ranges results in a clean dataset
    Given a clean local data store
    And an external data source for a specific date range
    When I extract data for the first half of the date range
    And then I extract data for the full date range
    Then the final dataset should contain all records for the full date range
    And the final dataset should not contain duplicate records
