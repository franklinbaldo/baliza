@journey4 @green
Feature: City Detail View
  The city detail panel shows município contracts read from a URL query param (?ibge=).

  Scenario: Missing IBGE shows EntityNotFound
    Given the URL has no ibge parameter
    When the city detail view mounts
    Then I should see "MUNICÍPIO não encontrada"

  Scenario: Fetch pending shows skeleton
    Given the URL has ibge "1721000"
    And the fetch is pending
    When the city detail view mounts
    Then I should see a loading skeleton

  Scenario: Fetch error shows AlertBanner
    Given the URL has ibge "1721000"
    And the fetch throws "Município não localizado"
    When the city detail view mounts
    Then I should see an alert banner with the error message

  Scenario: Zero contracts shows EmptyState
    Given the URL has ibge "1721000"
    And the PNCP API returns 0 contracts
    When the city detail view mounts
    Then I should see "Nenhuma contratação recente"

  Scenario: Contract count rendered as stat card
    Given the URL has ibge "1721000"
    And the PNCP API returns 1 contract
    When the city detail view mounts
    Then I should see a stat card with value "1"

  Scenario: Fan-out merges contracts from multiple modalities and dedupes
    Given the URL has ibge "1721000"
    And the PNCP API returns 2 contracts for modality 6 and 1 contract for modality 8
    When the city detail view mounts
    Then I should see a stat card with value "3"

  Scenario: Every PNCP consulta URL sends the required publicação parameters
    Given the URL has ibge "1721000"
    And the PNCP API returns 0 contracts
    When the city detail view mounts
    Then every PNCP consulta URL should include dataInicial, dataFinal, codigoModalidadeContratacao and pagina

  Scenario: Lookback window dias=30 narrows the publicação date range
    Given the URL has ibge "1721000" and dias "30"
    And the PNCP API returns 0 contracts
    When the city detail view mounts
    Then every PNCP consulta URL should span 30 days between dataInicial and dataFinal
