Feature: Error Handling
  As a data pipeline operator
  I want baliza to handle API errors gracefully
  So that temporary issues don't crash the pipeline

  Scenario: Handles PNCP API errors gracefully
    Given the PNCP API returns a 500 error
    When I run "baliza extract --start 2024-01-01 --end 2024-01-01"
    Then the command should fail with exit code 1
    And the error message should be clear
    And no partial data should be saved
