Feature: Compras Data Extraction
  As a data engineer
  I want to extract data from the PNCP API's 'compras' endpoint
  So that I can have procurement data available in a local database

  Scenario: Extracting 'compras' data for a given period
    Given a clean DuckDB database
    When I run the baliza extract command for the 'compras' resource for a specific date range
    Then the 'compras' data should be extracted and saved to the DuckDB database
