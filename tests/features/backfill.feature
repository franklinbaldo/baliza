Feature: Historical Data Backfill
  As a data engineer
  I want to backfill historical data for complete months
  So that I can consolidate past data in a deterministic, reproducible way

  Background:
    Given I have the Baliza CLI installed
    And the PNCP API is available
    And I have a DuckDB database configured

  Scenario: Backfill a single month
    Given I want to backfill January 2024
    When I run "baliza backfill 2024-01 2024-01"
    Then the command should process the full month deterministically
    And should fetch data from 2024-01-01 to 2024-01-31
    And should use a separate pipeline name (e.g., "baliza_backfill")
    And should complete successfully

  Scenario: Backfill multiple consecutive months
    Given I want to backfill Q1 2024
    When I run "baliza backfill 2024-01 2024-03"
    Then the command should process January, February, and March
    And each month should be processed sequentially
    And progress should be shown for each month
    And all three months should complete successfully

  Scenario: Backfill validates month format
    Given I provide an invalid month format
    When I run "baliza backfill 2024-1 2024-3"
    Then the command should reject the invalid format
    And should display: "Month format must be YYYY-MM"
    And should exit with an error code

  Scenario: Backfill uses separate pipeline instance
    Given I run a backfill for 2024-01
    When the backfill executes
    Then it should use pipeline name "baliza_backfill"
    And should not interfere with incremental pipeline state
    And should use its own dlt state tracking

  Scenario: Backfill processes without state reuse
    Given I previously extracted partial data for 2024-01
    When I run "baliza backfill 2024-01 2024-01"
    Then the backfill should process the entire month fresh
    And should not reuse previous extraction state
    And should create a complete, deterministic dataset

  Scenario: Backfill updates coverage manifesto
    Given I run a backfill for 2024-01
    When the backfill completes
    Then the coverage table should be updated
    And should record totalPaginas for each day
    And should store hashes of extracted data
    And should mark windows with status "ok"

  Scenario: Backfill handles overlapping data with merge
    Given the contratos table contains some 2024-01 data
    When I run "baliza backfill 2024-01 2024-01"
    Then the backfill should use merge write disposition
    And duplicate records should be upserted (not duplicated)
    And the final dataset should have no duplicates
    And primary keys should remain unique

  Scenario: Backfill for very large month
    Given I want to backfill a month with 100,000 contracts
    When I run "baliza backfill 2024-06 2024-06"
    Then the backfill should process all pages
    And should show progress updates
    And should complete without memory issues
    And the coverage manifesto should reflect all pages

  Scenario: Backfill with invalid date range
    Given I provide start month after end month
    When I run "baliza backfill 2024-06 2024-01"
    Then the command should fail with error
    And should display: "Start month must be before or equal to end month"

  Scenario: Backfill records run metadata
    Given I run "baliza backfill 2024-01 2024-01"
    When the backfill completes
    Then a run record should be created in baliza_state.runs
    And should record the backfill start_time and end_time
    And should record status as "completed"
    And should record total_rows extracted

  Scenario: Backfill is idempotent
    Given I run "baliza backfill 2024-01 2024-01" successfully
    When I run "baliza backfill 2024-01 2024-01" again
    Then the second run should produce identical data
    And the record count should be the same
    And the data should be deterministic and reproducible

  Scenario: Backfill can be combined with extract
    Given I have backfilled data for 2024-01 to 2024-06
    And I run incremental "baliza extract" for recent days
    When I query the database
    Then both backfilled and incremental data should coexist
    And there should be no conflicts or duplicates
    And coverage should show complete history

  Scenario: Backfill respects API pagination limits
    Given a month has 50,000 contracts (100 pages at 500/page)
    When I run "baliza backfill 2024-01 2024-01"
    Then the backfill should fetch all 100 pages
    And should respect the 500 items per page limit
    And all 50,000 contracts should be stored

  Scenario: Backfill handles empty months gracefully
    Given I want to backfill a future month with no data
    When I run "baliza backfill 2025-12 2025-12"
    Then the backfill should complete without error
    And should report 0 contracts extracted
    And the coverage should show the month as processed with 0 pages
