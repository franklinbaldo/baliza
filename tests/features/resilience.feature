@tier1
Feature: Resilience
  As a data engineer,
  I want the extraction process to be resilient to network failures and API errors,
  So that I can ensure complete data coverage without manual intervention.

  @smoke
  Scenario: The extract command recovers from a transient API error
    Given a PNCP API that will fail transiently
    When I run the "baliza extract" command for the full date range
    Then the command should eventually succeed
    And the final dataset should contain all records for the full date range

  Scenario: The extract command gives up after multiple consecutive failures
    Given the PNCP API will consistently return a 500 error
    When I run the "baliza extract" command
    Then the command should fail after a reasonable number of retries
    And the error message should clearly indicate a persistent failure
