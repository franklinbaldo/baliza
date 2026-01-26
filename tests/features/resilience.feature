Feature: Resilient and Resumable Extraction

  Background:
    Given a clean local data store
    And a PNCP API that will fail transiently

  @resilience @smoke
  Scenario: The extract command recovers from a transient API error
    Given the PNCP API will return a 500 error for the first half of a date range
    And the PNCP API will succeed for the second half of the date range
    When I run the "baliza extract" command for the full date range
    Then the command should eventually succeed
    And the final dataset should contain all records for the full date range
    And the final dataset should not contain duplicate records
    And the run history should show one failed run and one successful run

  @resilience @edge-case
  Scenario: The extract command gives up after multiple consecutive failures
    Given the PNCP API will consistently return a 500 error
    When I run the "baliza extract" command
    Then the command should fail after a reasonable number of retries
    And the error message should clearly indicate a persistent failure
    And the state history should log the multiple failed attempts
