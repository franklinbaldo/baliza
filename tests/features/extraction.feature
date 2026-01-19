Feature: Data Extraction
  As a data engineer
  I want to extract data from the PNCP API
  So that I can have the data available in a local database for analysis

  Background:
    Given the PNCP API is available
    And I have a DuckDB database configured

  Scenario: Basic extraction for a date range
    Given a clean DuckDB database
    When I run the baliza extract command for a specific date range
    Then the data should be extracted and saved to the DuckDB database
    And the extraction should complete successfully

  Scenario: Incremental extraction with configurable lookback
    Given I have previously extracted data up to 10 days ago
    When I run "baliza extract --lookback-days 3"
    Then the extraction should process the last 3 days
    And should include any updates to previously extracted data
    And the coverage table should show the incremental windows

  Scenario: Extraction handles pagination correctly
    Given the PNCP API returns data in pages of 500 items
    And a date window contains 1500 contracts
    When I extract data for that window
    Then the pipeline should fetch exactly 3 pages
    And all 1500 contracts should be stored in the database
    And the coverage manifesto should record totalPaginas = 3

  Scenario: Extraction respects 10MB response size limit
    Given the PNCP API returns a response larger than 10MB
    When I attempt to extract data for that window
    Then the extraction should fail with a security error
    And the error message should mention the 10MB limit
    And the window should be marked as failed in the state

  Scenario: Extraction deduplicates using primary key
    Given a contract with numeroControlePNCP "12345678901234-1-2024" exists in the database
    And the PNCP API returns an updated version of that contract
    When I run an incremental extraction
    Then the database should contain only one version of the contract
    And it should have the most recent dataAtualizacao timestamp
    And the merge strategy should have prevented duplicates

  Scenario: Multiple small date windows processed efficiently
    Given I need to extract data with 10 separate 1-day windows
    When I run the extraction
    Then each window should be processed sequentially
    And adjacent windows should be merged where possible
    And the state should be persisted after each window
    And the total extraction should show progress for all windows

  Scenario: URL validation prevents injection attacks
    Given I provide a malicious URL with special characters
    When I attempt to configure the pipeline
    Then the configuration should reject non-http/https URLs
    And only valid PNCP endpoints should be accepted

  Scenario: Query parameters are redacted in error logs
    Given the extraction encounters an API error with query parameters
    When the error is logged
    Then sensitive query parameters should be redacted
    And the log should show "[REDACTED]" for sensitive values
    And the error should still be traceable for debugging

  Scenario: Extraction creates run record with metadata
    Given a clean state database
    When I run "baliza extract --lookback-days 5"
    Then a new run record should be created in baliza_state.runs
    And the run should have a unique run_id
    And the run should record start_time
    And the run should record status as "running"
    And when complete, status should change to "completed"
    And the run should record total_rows extracted

  Scenario: Empty date windows are handled gracefully
    Given a date window with no contracts in the PNCP API
    When I extract data for that window
    Then the extraction should complete successfully
    And the window should be marked as complete with 0 pages
    And no error should be raised
