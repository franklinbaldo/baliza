Feature: Data Verification
  As a data engineer
  I want to verify data coverage
  So that I can identify gaps in extraction

  @tier1
  Scenario: Verify command detects gaps
    Given I have extracted data for 2024-01-01 to 2024-01-10
    But 2024-01-05 to 2024-01-07 are missing
    When I run "baliza verify --resource contratos --start 2024-01-01 --end 2024-01-10"
    Then the output should show gaps for 2024-01-05 to 2024-01-07
    And the command should exit successfully
