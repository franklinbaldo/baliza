Feature: Resumable Extraction and State Management
  As a data pipeline operator,
  I want the `baliza extract` command to be fully resumable,
  so that I can recover from interruptions without losing work or reprocessing data.

  Background:
    Given I have a Baliza installation
    And the state management system is initialized

  Scenario: First-time extraction creates state
    Given no previous extraction state exists
    When I run "baliza extract" for the last 3 days
    Then the command should succeed
    And a run record with status "completed" should exist in the state database
    And the coverage table should contain 3 completed time windows
    And each window should have status "ok"

  Scenario: Resuming an interrupted extraction
    Given a previous extraction run for the last 10 days was interrupted
    And the coverage table shows 5 completed windows and 5 missing windows
    When I run "baliza extract" for the last 10 days again
    Then the command should succeed
    And the pipeline should only process the 5 missing windows
    And the coverage table should now contain 10 completed time windows
    And no duplicate data should be created

  Scenario: Gap detection identifies incomplete windows
    Given extraction was interrupted mid-window
    And a window shows only 2 of 5 expected pages
    When I run "baliza extract" again
    Then the GapDetector should identify an "incomplete" window
    And the incomplete window should be reprocessed
    And the window should be marked complete after reprocessing

  Scenario: Gap detection identifies suspect windows
    Given a window was previously extracted with 10 pages
    And the API now reports 15 pages for the same window
    When I run "baliza verify"
    Then the window should be flagged as "suspect"
    And the verify output should explain the page count mismatch
    When I run "baliza extract" again
    Then the suspect window should be reprocessed
    And the updated page count should be recorded

  Scenario: Gap detection applies lookback period
    Given data was last extracted 5 days ago
    And lookback is configured to 3 days
    When I run "baliza extract"
    Then windows from the last 3 days should be reprocessed
    And the gap reason should be "lookback"
    And older windows should not be reprocessed

  Scenario: Adjacent gaps are merged for efficiency
    Given I have gaps for dates: 2024-01-01, 2024-01-02, 2024-01-03
    When the GapDetector processes these gaps
    Then they should be merged into a single window: 2024-01-01 to 2024-01-03
    And only one API call should be made instead of three
    And the coverage manifesto should record all three dates

  Scenario: State persists after each successful window
    Given I am extracting 5 date windows
    And the extraction fails on window 3
    When I check the state database
    Then windows 1 and 2 should be marked complete
    And window 3 should be marked incomplete or missing
    And windows 4 and 5 should be unprocessed
    When I restart the extraction
    Then only windows 3, 4, and 5 should be processed

  Scenario: State show displays coverage summary
    Given several extraction runs have occurred
    When I run "baliza state show --resource contratos"
    Then the output should contain a coverage summary
    And should show the number of complete windows
    And should show the number of incomplete windows
    And should show the number of suspect windows
    And should display human-readable timestamps
    And should use a card layout for readability

  Scenario: State gaps lists missing date ranges
    Given extraction has gaps in coverage
    When I run "baliza state gaps --resource contratos --start 2024-01-01 --end 2024-12-31"
    Then the output should list specific missing date ranges
    And should show the reason for each gap (missing, incomplete, suspect, lookback)
    And should display gaps in chronological order
    And should show icons for each gap type

  Scenario: State history shows extraction runs
    Given multiple extraction runs have occurred
    When I run "baliza state history --resource contratos"
    Then the output should show a history of past extraction runs
    And each run should show: run_id, start_time, end_time, status, total_rows
    And runs should be ordered by most recent first
    And completed runs should be marked with ✓
    And failed runs should be marked with ✗

  Scenario: Coverage manifesto stores page hashes
    Given I extract a window with 3 pages
    When the extraction completes
    Then the coverage table should store a hash of numeroControlePNCP values
    And the hash should use SHA-256 algorithm
    And the totalPaginas should be 3
    When I verify the data later
    Then I can compare the stored hash with the current API response
    And detect if data has changed

  Scenario: Run tracking records timing and metrics
    Given I start an extraction run
    When the extraction is running
    Then the run record should have status "running"
    And should record the start_time
    When the extraction completes
    Then the status should change to "completed"
    And should record the end_time
    And should record the total_rows extracted
    And should calculate duration = end_time - start_time

  Scenario: State commands filter by resource
    Given I have extracted data for multiple resources (contratos, compras)
    When I run "baliza state show --resource contratos"
    Then only contratos data should be shown
    And compras data should be excluded
    When I run "baliza state show --resource compras"
    Then only compras data should be shown
