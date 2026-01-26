Feature: State Inspection
  As a data pipeline operator,
  I want to inspect the state of the extraction pipeline,
  So that I can monitor its health and diagnose issues.

  Scenario: Show the current state of a resource
    Given a resource "contratos" with complete, incomplete, and suspect windows
    When I run "baliza state show --resource contratos"
    Then the output should summarize the state of the windows
    And the command should exit successfully

  Scenario: List coverage gaps for a resource
    Given a resource "contratos" with known coverage gaps
    When I run "baliza state gaps --resource contratos"
    Then the output should list the identified coverage gaps
    And the command should exit successfully

  Scenario: Show the history of extraction runs for a resource
    Given a resource "contratos" with a history of extraction runs
    When I run "baliza state history --resource contratos"
    Then the output should show the history of extraction runs
    And the command should exit successfully
