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

  Scenario: Successful fetch renders all detail blocks
    Given the URL has id "00000000000191-1-000001-1"
    And the PNCP API returns a valid contract payload
    When the contract detail view mounts
    Then I should see the "Detalhes da Contratação" block
    And I should see the "Órgão Responsável" block
    And I should see the "Valores" block
    And I should see the "Itens" block
    And I should see the "Fontes e Metadados" block

  Scenario: Successful fetch renders formatted currency and links
    Given the URL has id "00000000000191-1-000001-1"
    And the PNCP API returns a valid contract payload
    When the contract detail view mounts
    Then I should see the BRL-formatted valor "R$ 1.500,00"
    And I should see a link to the agency page with cnpj "00000000000191"
    And I should see a link to the municipality page with ibge "3550308"
    And I should see an external link to the origin system

  Scenario: Slash-form id uses the year segment when calling PNCP
    Given the URL has id "00000000000191-1-000001/2024"
    And the PNCP API returns a valid contract payload
    When the contract detail view mounts
    Then the PNCP consulta URL should contain "/contratacoes/2024/000001"
