Feature: Verify IA manifest coverage
  As a data engineer
  I want to diff requested months against the Internet Archive manifest
  So that I can identify which months still need to be synced

  @tier1
  Scenario: Verify reports complete coverage when manifest lists every month
    Given the IA manifest already lists every month from 2023-01 to 2023-12
    When I run "baliza verify --resource contratos --start 2023-01-01 --end 2023-12-31"
    Then the command should exit successfully
    And the output should mention "Complete month coverage"

  @tier1
  Scenario: Verify reports gaps when manifest is missing months
    Given the IA manifest lists only 2023-01 and 2023-02
    When I run "baliza verify --resource contratos --start 2023-01-01 --end 2023-06-30"
    Then the command should exit successfully
    And the output should mention a missing month "2023-03"
    And the output should mention a missing month "2023-06"

  @tier2
  Scenario: Verify tolerates a manifest read failure
    Given the IA manifest read fails
    When I run "baliza verify --resource contratos --start 2023-01-01 --end 2023-03-31"
    Then the command should exit successfully
    And the output should mention a missing month "2023-01"
