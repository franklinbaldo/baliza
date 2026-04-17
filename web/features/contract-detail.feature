Feature: Contract Detail View
  The contract detail panel shows a single contratação read from a URL query param (?id=).

  Scenario: Missing ID shows EntityNotFound
    Given the URL has no id parameter
    When the contract detail view mounts
    Then I should see "CONTRATAÇÃO não encontrada"

  Scenario: Fetch pending shows skeleton
    Given the URL has id "00000000000191-1-000001-1"
    And the fetch is pending
    When the contract detail view mounts
    Then I should see an aria-busy element

  Scenario: Fetch error shows AlertBanner
    Given the URL has id "00000000000191-1-000001-1"
    And the fetch throws "Contratação não localizada"
    When the contract detail view mounts
    Then I should see an alert banner with the error message

  Scenario: Successful fetch renders both summary cards
    Given the URL has id "00000000000191-1-000001-1"
    And the PNCP API returns a valid contract payload
    When the contract detail view mounts
    Then I should see "Resumo Executivo"
    And I should see "Itens da Licitação"
