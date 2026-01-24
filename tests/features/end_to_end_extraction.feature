Feature: End-to-end data extraction
  As a user of the Baliza CLI,
  I want to extract data for a specific date range,
  So that I can create a local database of public contracts.

  @tier0
  Scenario: Extracting data for a specific date range
    Given a clean local database
    And a mock PNCP API server
    When I run the "extract" command with a specific start and end date
    Then the database should contain the correct number of records for that date range
    And the database should contain records with the expected data

  @tier1
  Scenario: Extracting data for a date range with no data
    Given a clean local database
    And a mock PNCP API server that will return no data for a specific date range
    When I run the "extract" command with that date range
    Then the database should contain zero records
