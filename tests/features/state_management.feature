@state
Feature: State Management
  As a data engineer,
  I want to inspect the state of the data extraction process,
  So that I can diagnose issues and understand the coverage of the local data store.

  Background:
    Given a local data store for the "contratos" resource
    And the data store has the following daily windows:
      | start_date | status      |
      | 2024-01-01 | complete    |
      | 2024-01-02 | incomplete  |
      | 2024-01-03 | missing     |
      | 2024-01-04 | complete    |
    And the data store has a history of two extraction runs

  @smoke
  Scenario: Summarizing the current state of the data store
    When I run the "state show --resource contratos" command
    Then the output should contain a summary table with the following counts:
      | status     | count |
      | complete   | 2     |
      | incomplete | 1     |
      | missing    | 1     |

  @smoke
  Scenario: Listing coverage gaps
    When I run the "state gaps --resource contratos" command
    Then the output should list the "2024-01-03" as a missing window
    And the output should list the "2024-01-02" as an incomplete window

  @smoke
  Scenario: Reviewing the history of extraction runs
    When I run the "state history --resource contratos" command
    Then the output should list 2 previous extraction runs
