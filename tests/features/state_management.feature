Feature: Resumable Extraction and State Management
  As a data pipeline operator,
  I want the `baliza extract` command to be fully resumable,
  so that I can recover from interruptions without losing work or reprocessing data.

  Scenario: First-time extraction
    Given no previous extraction state exists
    When I run "baliza extract" for the last 3 days
    Then the command should succeed
    And a run record with status "completed" should exist in the state database
    And the coverage table should contain 3 completed time windows

  Scenario: Resuming an interrupted extraction
    Given a previous extraction run for the last 10 days was interrupted
    And the coverage table shows 5 completed windows and 5 missing windows
    When I run "baliza extract" for the last 10 days again
    Then the command should succeed
    And the pipeline should only process the 5 missing windows
    And the coverage table should now contain 10 completed time windows

  Scenario: State CLI commands provide observability
    Given several extraction runs have occurred
    When I run "baliza state show --resource contratos"
    Then the output should contain a summary of coverage
    When I run "baliza state gaps --resource contratos"
    Then the output should list the specific missing date ranges
    When I run "baliza state history --resource contratos"
    Then the output should show a history of past extraction runs
