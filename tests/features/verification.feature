Feature: Data Verification
  As a data engineer
  I want to verify the data coverage
  So that I can identify any gaps or inconsistencies in the extracted data

  Scenario: Verifying coverage for a period with complete data
    Given a DuckDB database with complete data for a specific period
    When I run the baliza verify command for that period
    Then the verification should pass successfully

  Scenario: Verifying coverage for a period with gaps
    Given a DuckDB database with data gaps for a specific period
    When I run the baliza verify command for that period
    Then the verification should identify the gaps
