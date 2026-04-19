@journey1 @green
Feature: Supplier Detail View
  The supplier detail panel shows a fornecedor profile read from a URL query param (?cnpj=).
  PNCP has no supplier-by-CNPJ endpoint, so the view is always served from the Parquet snapshot.

  Scenario: Missing CNPJ shows EntityNotFound
    Given the URL has no cnpj parameter
    When the supplier detail view mounts
    Then I should see "FORNECEDOR não encontrada"

  Scenario: Archive pending shows skeleton
    Given the URL has cnpj "12345678000195"
    And the archive lookup is pending
    When the supplier detail view mounts
    Then I should see an aria-busy element

  Scenario: Archive failure shows AlertBanner
    Given the URL has cnpj "12345678000195"
    And the archive lookup fails
    When the supplier detail view mounts
    Then I should see an alert banner with the error message

  Scenario: Valid CNPJ without contracts shows EmptyState
    Given the URL has cnpj "12345678000195"
    And the archive lookup returns empty
    When the supplier detail view mounts
    Then I should see "Nenhum contrato arquivado"

  Scenario: Contracts render as links
    Given the URL has cnpj "12345678000195"
    And the archive lookup returns a contract with id "00000000000191-1-000001/2024"
    When the supplier detail view mounts
    Then I should see a link to that contract

  Scenario: Average ticket renders from archive data
    Given the URL has cnpj "12345678000195"
    And the archive lookup returns contracts with values 1000 and 2000
    When the supplier detail view mounts
    Then I should see the avg-ticket card showing "R$ 1.500,00"

  Scenario: Competing suppliers card is always rendered
    Given the URL has cnpj "12345678000195"
    And the archive lookup returns a contract with id "00000000000191-1-000001/2024"
    When the supplier detail view mounts
    Then I should see the competing-suppliers card
