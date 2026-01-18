Feature: Data Export
  As a data engineer
  I want to export data from the DuckDB database to Parquet files
  So that I can use the data in other systems

  Scenario: Exporting data to Parquet
    Given a DuckDB database with existing data
    When I run the baliza export command
    Then Parquet files should be created in the output directory

  Scenario: Exporting data with year and month partitioning
    Given a DuckDB database with data spanning multiple months
    When I run the baliza export command with partitioning enabled
    Then the output directory should contain subdirectories for each year and month
    And each subdirectory should contain at least one Parquet file
