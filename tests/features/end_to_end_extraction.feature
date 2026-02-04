@tier0
Feature: End-to-end data extraction pipeline
  As a data engineer,
  I want to run the extraction pipeline,
  So that I have a complete and unique local dataset.

  @tier0
  @smoke
  Scenario: The data pipeline is resumable and idempotent
    Given a clean local data store
    And an external data source for a specific date range
    When I extract data for the first half of the date range
    And then I extract data for the full date range
    Then the final dataset should contain all records for the full date range
    And the final dataset should not contain duplicate records
