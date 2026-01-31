@tier0
Feature: Data Export
  As a data engineer
  I want to export data from DuckDB to Parquet
  So that baliza-site can consume it

  @tier0
  Scenario: Export creates valid parquet
    Given a DuckDB database with 100 contracts
    When I run "baliza export --table contratos --output ./exports"
    Then Parquet files should be created
    And the Parquet files should contain all 100 contracts
    And the command should exit successfully
