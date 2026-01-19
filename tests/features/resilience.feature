Feature: Pipeline Resilience and Error Handling
  As a data pipeline operator
  I want the Baliza CLI to handle failures gracefully
  So that temporary issues don't cause data loss or corruption

  Background:
    Given I have the Baliza CLI installed
    And I have a DuckDB database configured

  Scenario: Extraction retries on transient network errors
    Given the PNCP API returns a temporary network error
    When I run "baliza extract"
    Then the pipeline should retry the request automatically
    And should use exponential backoff between retries
    And should eventually succeed if the error is transient

  Scenario: Extraction handles API timeouts
    Given the PNCP API is slow to respond
    And a request exceeds the configured timeout
    When the extraction is running
    Then the request should be cancelled after timeout
    And should be retried with backoff
    And should log the timeout event

  Scenario: Extraction respects API rate limits
    Given the PNCP API returns HTTP 429 (Too Many Requests)
    When the extraction encounters rate limiting
    Then the pipeline should pause before retrying
    And should respect the Retry-After header if present
    And should log the rate limit event

  Scenario: Extraction records failures in state
    Given an extraction window fails after all retries
    When the extraction completes
    Then the failed window should be recorded in state
    And status should be "failed" or "incomplete"
    And the error message should be logged
    And subsequent runs should retry the failed window

  Scenario: Extraction continues after partial window failure
    Given I am extracting 10 date windows
    And window 5 fails permanently
    When the extraction continues
    Then windows 6-10 should still be processed
    And the overall extraction should complete partially
    And the summary should show: "9/10 windows successful"

  Scenario: Empty API responses handled correctly
    Given the PNCP API returns an empty response (HTTP 200, no data)
    When the extraction processes this window
    Then the window should be marked complete with 0 pages
    And no error should be raised
    And the state should record totalPaginas = 0

  Scenario: Malformed JSON responses are logged and skipped
    Given the PNCP API returns invalid JSON
    When the extraction encounters this response
    Then the error should be logged with the invalid response
    And the window should be marked as failed
    And the extraction should continue with next window

  Scenario: Database connection errors trigger retry
    Given the DuckDB connection is temporarily lost
    When the extraction attempts to write data
    Then the pipeline should retry the database operation
    And should re-establish the connection if needed
    And should eventually succeed

  Scenario: State file corruption is detected and reported
    Given the state database file becomes corrupted
    When I run "baliza extract"
    Then the corruption should be detected immediately
    And a clear error message should be displayed
    And should suggest restoring from backup or recreating

  Scenario: Concurrent executions are prevented with locking
    Given an extraction is currently running
    When I attempt to start another extraction
    Then the second extraction should detect the lock
    And should display: "Another extraction is in progress"
    And should exit without starting

  Scenario: HTTP 500 errors are retried appropriately
    Given the PNCP API returns HTTP 500 (Internal Server Error)
    When the extraction encounters this error
    Then it should retry with exponential backoff
    And should give up after maximum retries (e.g., 5 attempts)
    And should log all retry attempts

  Scenario: HTTP 404 errors are handled as missing data
    Given the PNCP API returns HTTP 404 for a window
    When the extraction processes this window
    Then it should treat it as empty data (not an error)
    And should mark the window complete with 0 records
    And should not retry

  Scenario: Network interruption during extraction
    Given extraction is in progress
    And the network connection is lost mid-request
    When the network is restored
    Then the extraction should resume from last completed window
    And no data should be lost
    And no duplicate data should be created

  Scenario: Disk full error during export
    Given I run "baliza export --table contratos"
    And the disk becomes full during export
    When the export fails
    Then the error should be clearly reported
    And partial Parquet files should be cleaned up
    And the database should remain intact

  Scenario: Out of memory error during large extraction
    Given I extract a very large date range
    And the system runs out of memory
    When the extraction fails
    Then the error should be logged
    And the state should reflect completed windows
    And I can resume with smaller batches

  Scenario: Graceful shutdown on SIGINT (Ctrl+C)
    Given an extraction is in progress
    When I press Ctrl+C to interrupt
    Then the extraction should stop gracefully
    And should save state for completed windows
    And should display: "Extraction interrupted, progress saved"

  Scenario: Invalid configuration is detected early
    Given I provide an invalid DuckDB path
    When I run "baliza extract"
    Then the error should be detected before starting extraction
    And should display a helpful error message
    And should not make any API calls

  Scenario: API returns data exceeding 10MB limit
    Given the PNCP API returns a response > 10MB
    When the extraction attempts to process it
    Then the extraction should reject the response
    And should log a security error
    And should mark the window as failed
    And should suggest using smaller date ranges

  Scenario: Retry with exponential backoff
    Given a transient API error occurs
    When the extraction retries
    Then it should wait 1s before first retry
    And 2s before second retry
    And 4s before third retry
    And 8s before fourth retry
    And 16s before fifth retry
    And give up after 5 attempts

  Scenario: Pipeline recovers from dlt state corruption
    Given the dlt pipeline state becomes corrupted
    When I run "baliza extract"
    Then Baliza should detect the dlt state issue
    And should offer to reset the pipeline state
    And should preserve Baliza's own state tables
    And extraction should continue after reset
