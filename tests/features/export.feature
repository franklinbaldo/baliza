Feature: Data Export
  As a data engineer
  I want to export data from the DuckDB database to Parquet files
  So that I can use the data in other systems

  Scenario: Exporting data to Parquet
    Given a DuckDB database with existing data
    When I run the baliza export command
    Then Parquet files should be created in the output directory
