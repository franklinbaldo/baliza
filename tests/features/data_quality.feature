Feature: Data Quality and Integrity
  As a data analyst,
  I want to ensure the extracted data is accurate and follows the expected schema,
  So that I can trust the results for my research.

  @tier1 @smoke
  Scenario: Primary key uniqueness is maintained
    Given a clean local data store
    And some records already exist in the data store
    When I extract records that overlap with the existing data
    Then the data store should not contain any duplicate primary keys
    And the total record count should be the sum of new and unique old records

  @tier1
  Scenario: Schema validation for contracts
    Given a clean local data store
    When I extract data for a specific date
    Then the contracts table should have the expected schema
    And essential fields like "numeroControlePNCP" should not be null
