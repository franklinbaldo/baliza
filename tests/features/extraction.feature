Feature: Data Extraction
  As a data engineer
  I want to extract data from the PNCP API
  So that I can have the data available in a local database

  Scenario: Extracting data for a given period
    Given a clean DuckDB database
    When I run the baliza extract command for a specific date range
    Then the data should be extracted and saved to the DuckDB database
