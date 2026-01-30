Feature: State Management
  As a data engineer,
  I want to inspect the state of the data extraction process,
  So that I can diagnose issues and understand the coverage of the local data store.

  @tier1
  Scenario: Show the overall status of the data extraction process
    Given a data store with extracted records
    When I run the "baliza status" command
    Then the output should summarize the state of the data store

  @tier1
  Scenario: Verify the data coverage and find gaps
    Given a data store with partial coverage
    When I run the "baliza verify" command for the full range
    Then the output should list the missing windows

  @tier2
  Scenario: Show warnings for pending extractions
    Given a data store with pending checkpoints
    When I run the "baliza status" command
    Then the output should show a warning about incomplete extractions
