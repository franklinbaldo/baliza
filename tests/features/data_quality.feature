Feature: Data Quality and Validation
  As a data engineer
  I want to ensure data integrity and quality
  So that downstream analysis can trust the extracted data

  Background:
    Given I have extracted data in the DuckDB database
    And the contratos table contains data

  Scenario: Primary keys are unique
    Given the database contains 10,000 contracts
    When I query for duplicate numeroControlePNCP values
    Then there should be zero duplicates
    And each primary key should appear exactly once

  Scenario: Merge strategy prevents duplicates during updates
    Given a contract with numeroControlePNCP "12345678901234-1-2024" exists
    And the same contract is extracted again with updated dataAtualizacao
    When I query the database
    Then only one record should exist for that numeroControlePNCP
    And it should have the most recent dataAtualizacao timestamp

  Scenario: Date fields are properly formatted
    Given extracted contracts contain date fields
    When I validate the data
    Then all date fields should be in ISO format (YYYY-MM-DD) or PNCP format (AAAAMMDD)
    And no invalid dates should exist (e.g., 2024-13-01, 2024-02-30)
    And date fields should be parseable

  Scenario: Required fields are never null
    Given the PNCP schema requires numeroControlePNCP
    When I query the database
    Then numeroControlePNCP should never be NULL
    And all required fields defined in schema should be populated

  Scenario: Data types match schema expectations
    Given the contratos table has a defined schema
    When I validate the data types
    Then numeric fields should contain only numbers
    And date fields should be valid dates
    And string fields should be properly encoded (UTF-8)
    And boolean fields should be true/false only

  Scenario: Hash digests match stored values in coverage
    Given a window was extracted with hash SHA256:abc123...
    When I recalculate the hash from numeroControlePNCP values
    Then the calculated hash should match the stored hash
    And this confirms data has not been corrupted or modified

  Scenario: Incremental cursor works correctly
    Given extraction uses dataAtualizacao as incremental cursor
    When I run incremental extraction
    Then only records with dataAtualizacao >= last cursor should be fetched
    And the cursor should advance to the most recent dataAtualizacao
    And no records should be missed

  Scenario: Page totals are consistent across runs
    Given I extract a window and record totalPaginas = 10
    When I extract the same window again later
    Then totalPaginas should remain 10 (unless data actually changed)
    And any change should be flagged as "suspect" for investigation

  Scenario: Record counts match API pagination math
    Given the API reports totalPaginas = 10 with tamanhoPagina = 500
    When I count records in the database for that window
    Then I should have between 4501-5000 records (allowing for last page)
    And the count should be consistent with page count

  Scenario: No orphaned records after extraction failure
    Given an extraction fails mid-window
    When I check the database
    Then either all pages from that window should be present
    Or no pages from that window should be present
    And there should be no partial/incomplete window data

  Scenario: UTF-8 encoding is preserved
    Given contract data contains Brazilian Portuguese characters (ç, ã, õ, etc.)
    When I extract and query the data
    Then all special characters should be preserved correctly
    And text should be readable and properly encoded
    And no encoding corruption should occur

  Scenario: Numeric fields have reasonable ranges
    Given contracts contain numeric amounts
    When I validate the data
    Then amounts should be non-negative (>= 0)
    And amounts should be reasonable (not exceeding plausible maximums)
    And numeric precision should be maintained

  Scenario: Foreign key relationships are valid
    Given contracts reference CNPJ values
    When I validate the data
    Then CNPJ values should be properly formatted (14 digits)
    And should not contain invalid characters
    And should pass CNPJ checksum validation

  Scenario: Timestamps are in UTC
    Given the system stores timestamps
    When I query timestamp fields
    Then all timestamps should be in UTC timezone
    And should not have timezone ambiguity
    And should be comparable across runs

  Scenario: No duplicate API calls for same data
    Given I extract a window successfully
    And the window is marked complete in coverage
    When I run extraction again
    Then the completed window should not be re-fetched from API
    And API call count should be minimized
    And only new/incomplete/suspect windows should be fetched

  Scenario: Data matches API contract schema
    Given the PNCP API defines a schema for contratos
    When I compare extracted data to the schema
    Then all fields should match expected types
    And all required fields should be present
    And no unexpected fields should break the pipeline

  Scenario: Merge conflicts are resolved correctly
    Given a record with numeroControlePNCP "X" and dataAtualizacao "2024-01-01"
    And I extract an updated version with dataAtualizacao "2024-01-15"
    When I query the database
    Then the record should have the newer dataAtualizacao
    And no data from the newer version should be lost

  Scenario: Empty strings vs NULL are handled consistently
    Given some API fields may be empty strings or null
    When I extract the data
    Then empty strings should be preserved as empty strings
    And null values should be preserved as NULL
    And they should not be confused or conflated

  Scenario: Very long text fields are not truncated
    Given some contracts contain long descriptions (>10,000 characters)
    When I extract and store these contracts
    Then the full text should be preserved
    And no truncation should occur
    And text fields should have sufficient capacity

  Scenario: Data quality report can be generated
    Given I have extracted data in the database
    When I run a data quality validation
    Then a report should be generated showing:
      | Metric                     | Status     |
      | Duplicate primary keys     | 0 found    |
      | NULL required fields       | 0 found    |
      | Invalid dates              | 0 found    |
      | Invalid numeric ranges     | 0 found    |
      | Encoding errors            | 0 found    |
      | Total records validated    | 10,000     |
      | Quality score              | 100%       |
