Feature: State Management
  As a data engineer,
  I want to inspect the state of the data extraction process,
  So that I can diagnose issues and understand the coverage of the local data store.

  @tier1
  Scenario: Show the state of the data extraction process
    Given a data store with complete and incomplete windows
    When I run the "status" command
    Then the output should summarize the state of the data store

  @tier1
  Scenario: List the gaps in the data extraction process
    Given a data store with complete and missing windows
    When I run the "verify" command for a specific range
    Then the output should list the missing windows

  @tier1 @xfail
  Scenario: Show the history of the data extraction process
    Given a data store with a history of extraction runs
    When I run the "status --history" command
    Then the output should list the previous extraction runs
