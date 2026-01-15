Feature: Extract `compras` data from PNCP API
  As a data analyst
  I want to use the Baliza CLI to extract procurement data
  So that I can analyze the full procurement lifecycle

  Scenario: Run `extract` command for the `compras` resource
    Given the `compras` resource is configured
    When I run the command `baliza extract --resource compras`
    Then the `baliza.duckdb` file should contain a `compras` table in the `baliza_raw` schema
    And the table should contain records from the `compras` endpoint

  Scenario: Run `backfill` command for the `compras` resource
    Given the `compras` resource is configured
    When I run the command `baliza backfill 2024-01 2024-01 --resource compras`
    Then the `compras` table in `baliza.duckdb` should contain records for January 2024

  Scenario: Run `export` command for the `compras` resource
    Given the `compras` table exists in `baliza.duckdb`
    When I run the command `baliza export --table compras --out data/compras`
    Then Parquet files for the `compras` data should be created in the `data/compras` directory
