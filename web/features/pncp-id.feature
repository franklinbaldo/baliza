Feature: PNCP ID canonical parsing
  PNCP's canonical numeroControlePNCP form is {cnpj}-{tipo}-{sequencial}/{ano}
  with a 14-digit CNPJ, 6-digit sequencial, and 4-digit year. The parser is
  strict — anything else is rejected at the boundary.

  Scenario: Canonical ID parses into its four segments
    Given the PNCP ID "12345678000195-1-000001/2024"
    When it is parsed
    Then the result should have cnpj "12345678000195"
    And the result should have tipo "1"
    And the result should have sequencial "000001"
    And the result should have ano "2024"

  Scenario: Hyphen-only ID is rejected
    Given the PNCP ID "12345678000195-1-000001-1"
    When it is parsed
    Then the result should be null

  Scenario: Missing year segment is rejected
    Given the PNCP ID "12345678000195-1-000001"
    When it is parsed
    Then the result should be null

  Scenario: Three-digit year is rejected
    Given the PNCP ID "12345678000195-1-000001/202"
    When it is parsed
    Then the result should be null

  Scenario: Lowercase letters are rejected
    Given the PNCP ID "abcdefghijklmn-1-000001/2024"
    When it is parsed
    Then the result should be null

  Scenario: ContractDetailView renders a targeted EntityNotFound for non-canonical IDs
    Given the URL has id "12345678000195-1-000001-1"
    When the contract detail view mounts
    Then I should see a non-canonical ID notice
    And I should see the expected format hint
