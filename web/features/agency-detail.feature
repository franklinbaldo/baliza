Feature: Agency Detail View
  The agency detail panel shows an órgão profile read from a URL query param (?cnpj=).

  Scenario: Missing CNPJ shows EntityNotFound
    Given the URL has no cnpj parameter
    When the agency detail view mounts
    Then I should see "ÓRGÃO não encontrada"

  Scenario: Fetch pending shows skeleton
    Given the URL has cnpj "00000000000191"
    And the fetch is pending
    When the agency detail view mounts
    Then I should see an aria-busy element

  Scenario: Fetch error shows AlertBanner
    Given the URL has cnpj "00000000000191"
    And the fetch throws "Órgão não localizado"
    When the agency detail view mounts
    Then I should see an alert banner with the error message

  Scenario: Zero contracts shows EmptyState
    Given the URL has cnpj "00000000000191"
    And the PNCP API returns 0 contracts
    When the agency detail view mounts
    Then I should see "Nenhuma contratação recente"

  Scenario: Contracts render as links
    Given the URL has cnpj "00000000000191"
    And the PNCP API returns a contract with id "00000000000191-1-000001/2024"
    When the agency detail view mounts
    Then I should see a link to that contract
