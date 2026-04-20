Feature: Daily Parquet Export
  As a data engineer
  I want to export daily data in a relational structure
  So that Internet Archive has self-contained daily packages

  @tier0
  Scenario: Daily export creates all required files
    Given a DuckDB database with contracts for 2023-01-15
    When I export daily parquet for 2023-01-15
    Then the following files should exist:
      | file                                        |
      | data/daily/2023-01-15/contratos.parquet     |
      | data/daily/2023-01-15/orgaos.parquet        |
      | data/daily/2023-01-15/unidades.parquet      |
      | data/daily/2023-01-15/fornecedores.parquet  |
      | data/daily/2023-01-15/_metadata.json        |

  @tier0
  Scenario: Contratos parquet has correct schema
    Given a daily export for 2023-01-15
    When I read contratos.parquet
    Then it should have columns:
      | column                  | type      |
      | numero_controle_pncp    | string    |
      | cnpj_orgao              | string    |
      | ni_fornecedor           | string    |
      | valor_inicial           | double    |
      | data_publicacao         | timestamp |
      | data_particao           | date32    |

  @tier1
  Scenario: Orgaos parquet is deduplicated per day
    Given a DuckDB database with 100 contracts from 10 unique orgs
    When I run the daily export
    Then orgaos.parquet should have exactly 10 rows
    And each org should have contratos_no_dia count

  @tier1
  Scenario: Foreign keys are valid
    Given a daily export for 2023-01-15
    When I join contratos with orgaos on cnpj_orgao
    Then all contracts should have matching orgs
    And no orphan contracts should exist

  @tier2
  Scenario: Metadata file contains stats
    Given a daily export with 10 contracts
    When I read _metadata.json
    Then schema_version should be "2.0.0"
    And tables.contratos.row_count should be 10
    And data_particao should be "2023-01-15"
