Feature: Data Export
  As a data engineer
  I want to export data from the DuckDB database to Parquet files
  So that I can share, archive, and analyze data in other systems

  Background:
    Given I have a DuckDB database with extracted contract data
    And the contratos table contains data from 2024-01-01 to 2024-12-31

  Scenario: Basic Parquet export
    Given the database contains 10,000 contracts
    When I run "baliza export --table contratos --output ./exports"
    Then Parquet files should be created in the "./exports" directory
    And the files should be partitioned by year and month
    And the export should complete successfully

  Scenario: Export filters by date range
    Given I only want data from Q1 2024
    When I run "baliza export --table contratos --start 2024-01-01 --end 2024-03-31"
    Then only contracts from January to March 2024 should be exported
    And contracts from other months should be excluded
    And the output should show the filtered count

  Scenario: Export partitions by year and month
    Given the database contains contracts from multiple months
    When I run "baliza export --table contratos"
    Then files should be organized as: year=YYYY/month=MM/
    And each partition should contain only data from that month
    And partition paths should follow Hive-style naming
    Examples:
      | Partition Path         | Contains Data From |
      | year=2024/month=01/    | January 2024       |
      | year=2024/month=12/    | December 2024      |

  Scenario: Export to specific output directory
    Given I want to export to a custom location
    When I run "baliza export --table contratos --output /tmp/baliza-export"
    Then Parquet files should be created in "/tmp/baliza-export"
    And the directory should be created if it doesn't exist
    And existing files should not be overwritten without warning

  Scenario: Export specific table
    Given the database contains multiple tables (contratos, compras)
    When I run "baliza export --table contratos"
    Then only the contratos table should be exported
    And compras table should be excluded
    When I run "baliza export --table compras"
    Then only the compras table should be exported

  Scenario: Export uploads to Internet Archive
    Given I have configured Internet Archive credentials
    When I run "baliza export --table contratos --ia-identifier baliza-contratos-2024"
    Then the Parquet files should be exported locally first
    And the files should be uploaded to Internet Archive
    And the item should be created with identifier "baliza-contratos-2024"
    And upload progress should be displayed

  Scenario: Export validates data before writing
    Given the database contains invalid or null primary keys
    When I run "baliza export --table contratos"
    Then the export should detect the data quality issue
    And should display a warning message
    And should continue with valid records only
    Or optionally fail if --strict mode is enabled

  Scenario: Export shows progress for large datasets
    Given the database contains 1 million contracts
    When I run "baliza export --table contratos"
    Then a progress bar should be displayed
    And should show the number of rows processed
    And should show estimated time remaining
    And should update in real-time

  Scenario: Export handles empty tables gracefully
    Given the contratos table exists but has no data
    When I run "baliza export --table contratos"
    Then the export should complete without error
    And should display a message: "No data to export"
    And should not create empty Parquet files

  Scenario: Export uses efficient compression
    Given I want to minimize file size
    When I run "baliza export --table contratos --compression snappy"
    Then Parquet files should use Snappy compression
    And file sizes should be significantly smaller than uncompressed
    And data should be lossless

  Scenario: Export provides summary statistics
    Given I complete an export
    When the export finishes
    Then the output should show:
      | Metric              | Example Value      |
      | Total rows exported | 10,000             |
      | Number of files     | 12 (one per month) |
      | Total file size     | 25.3 MB            |
      | Export duration     | 3.2 seconds        |

  Scenario: Export fails safely on disk space issues
    Given the output directory has insufficient disk space
    When I run "baliza export --table contratos"
    Then the export should fail with a clear error message
    And should not leave partial/corrupt Parquet files
    And should suggest checking available disk space

  Scenario: Export supports dry-run mode
    Given I want to preview what will be exported
    When I run "baliza export --table contratos --dry-run"
    Then the command should show what would be exported
    And should display the number of rows and partitions
    And should NOT create any files
    And should complete quickly
