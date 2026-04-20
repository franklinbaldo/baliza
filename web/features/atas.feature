@journey2 @green
Feature: Atas de Registro de Preços Browser
  Public buyers browse vigent archive contracts matching an object description
  at /atas?objeto=. The page is archive-only — it issues a multi-filter DuckDB
  query with ILIKE on objeto_contrato plus a `>=` filter on data_vigencia_fim.

  Scenario: Empty objeto shows search prompt
    Given the URL has no objeto parameter
    When the atas view mounts
    Then I should see "Digite o objeto a pesquisar"

  Scenario: Short objeto shows search prompt
    Given the URL has objeto "ab"
    When the atas view mounts
    Then I should see "Digite o objeto a pesquisar"

  Scenario: Archive pending shows skeleton
    Given the URL has objeto "papel A4"
    And the archive lookup is pending
    When the atas view mounts
    Then I should see a loading skeleton

  Scenario: Archive failure shows AlertBanner
    Given the URL has objeto "papel A4"
    And the archive lookup fails
    When the atas view mounts
    Then I should see an alert banner with the error message

  Scenario: Valid objeto without matches shows EmptyState
    Given the URL has objeto "papel A4"
    And the archive lookup returns empty
    When the atas view mounts
    Then I should see "Nenhuma ata vigente encontrada"

  Scenario: Matching contracts render as atas-list
    Given the URL has objeto "papel A4"
    And the archive lookup returns a vigent contract
    When the atas view mounts
    Then I should see the atas-list

  Scenario: Query always includes the today-date gte filter
    Given the URL has objeto "papel A4"
    When the atas view mounts
    Then the archive query includes a data_vigencia_fim gte filter
