Feature: Detail view Parquet fallback
  Detail views try the latest Internet Archive Parquet snapshot when PNCP is down.

  Scenario: Agency view falls back to Parquet when PNCP fails
    Given the PNCP API is unavailable for cnpj "00000000000191"
    And the Parquet fallback returns one contract row for cnpj "00000000000191"
    When the agency detail view mounts for cnpj "00000000000191"
    Then I should see an info banner about PNCP indisponível
    And I should see the archived contract from the Parquet snapshot

  Scenario: Agency view shows combined error when both fail
    Given the PNCP API is unavailable for cnpj "00000000000191"
    And the Parquet fallback returns no rows for cnpj "00000000000191"
    When the agency detail view mounts for cnpj "00000000000191"
    Then I should see an error banner about arquivo histórico

  Scenario: Agency view does not query Parquet when PNCP succeeds
    Given the PNCP API returns one contract for cnpj "00000000000191"
    When the agency detail view mounts for cnpj "00000000000191"
    Then the Parquet fallback should not be called

  Scenario: City view falls back to Parquet when PNCP fails
    Given the PNCP API is unavailable for ibge "3550308"
    And the Parquet fallback returns one contract row for ibge "3550308"
    When the city detail view mounts for ibge "3550308"
    Then I should see an info banner about PNCP indisponível
    And I should see the archived contract from the Parquet snapshot

  Scenario: City view shows combined error when both fail
    Given the PNCP API is unavailable for ibge "3550308"
    And the Parquet fallback returns no rows for ibge "3550308"
    When the city detail view mounts for ibge "3550308"
    Then I should see an error banner about arquivo histórico

  Scenario: Contract view falls back to Parquet when PNCP fails
    Given the PNCP API is unavailable for id "00000000000191-1-000001/2024"
    And the Parquet fallback returns one row for id "00000000000191-1-000001/2024"
    When the contract detail view mounts for id "00000000000191-1-000001/2024"
    Then I should see an info banner about PNCP indisponível
    And I should see the archived contract objeto

  Scenario: Contract view shows combined error when both fail
    Given the PNCP API is unavailable for id "00000000000191-1-000001/2024"
    And the Parquet fallback returns no rows for id "00000000000191-1-000001/2024"
    When the contract detail view mounts for id "00000000000191-1-000001/2024"
    Then I should see an error banner about arquivo histórico

  Scenario: Agency fallback uses the exported cnpj_orgao column and orders by recency
    Given the PNCP API is unavailable for cnpj "00000000000191"
    When the agency detail view mounts for cnpj "00000000000191"
    Then the Parquet fallback should be called with column "cnpj_orgao" ordered by "data_publicacao_pncp"

  Scenario: City fallback uses the exported codigo_ibge column and orders by recency
    Given the PNCP API is unavailable for ibge "3550308"
    When the city detail view mounts for ibge "3550308"
    Then the Parquet fallback should be called with column "codigo_ibge" ordered by "data_publicacao_pncp"
