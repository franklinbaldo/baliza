Feature: State Management
  As a data engineer,
  I want to inspect the state of the data extraction process,
  So that I can diagnose issues and understand the coverage of the local data store.

  @tier1 @state
  Scenario: Show the state of the data extraction process
    Given a data store with complete, incomplete, and missing windows
    When I run the "state show" command
    Then the output should summarize the state of the data store

  @tier1 @state @gaps
  Scenario: List the gaps in the data extraction process
    Given a data store with complete, incomplete, and missing windows
    When I run the "state gaps" command
    Then the output should list the missing windows

  @tier1 @state @history
  Scenario: Show the history of the data extraction process
    Given a data store with a history of extraction runs
    When I run the "state history" command
    Then the output should list the previous extraction runs
