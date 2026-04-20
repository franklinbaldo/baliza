@journey5 @journey6 @green
Feature: Archive fallback hardening
  queryArchivedTable is the primary offline/historical data layer. Every
  failure mode is reported as a discriminated reason so callers can decide
  how to present it; limits are clamped and the layer can be preloaded.

  Scenario: Manifest endpoint 404 yields reason "no_manifest"
    Given the IA manifest endpoint returns 404
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the result should be a failure with reason "no_manifest"

  Scenario: DuckDB init failure yields reason "duckdb_init_failed"
    Given the IA manifest resolves to a parquet url
    And DuckDB initialization throws
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the result should be a failure with reason "duckdb_init_failed"

  Scenario: SQL returning zero rows yields reason "empty"
    Given the IA manifest resolves to a parquet url
    And DuckDB returns no rows
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the result should be a failure with reason "empty"

  Scenario: SQL thrown error yields reason "sql_error"
    Given the IA manifest resolves to a parquet url
    And DuckDB query throws
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the result should be a failure with reason "sql_error"

  Scenario: Served queries log "[archive] fallback served" with rowCount
    Given the IA manifest resolves to a parquet url
    And DuckDB returns two rows
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then console.info should log "[archive] fallback served" with rowCount 2
    And console.info should not log "[archive] fallback failed"

  Scenario: Failed queries log "[archive] fallback failed" with reason
    Given the IA manifest resolves to a parquet url
    And DuckDB returns no rows
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then console.info should log "[archive] fallback failed" with reason "empty"

  Scenario: Manifest 503 then 200 recovers via one retry
    Given the IA manifest endpoint returns 503 on the first call and 200 on the second
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the manifest endpoint should have been fetched twice
    And the result should succeed

  Scenario: Manifest 404 is terminal and does not retry
    Given the IA manifest endpoint returns 404
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the manifest endpoint should have been fetched once

  Scenario: SQL exceeding the timeout yields reason "timeout"
    Given the IA manifest resolves to a parquet url
    And DuckDB query never resolves
    When queryArchivedTable is called for "contratos" with timeoutMs 10
    Then the result should be a failure with reason "timeout"
    And DuckDB cancelSent should have been called to cancel the stuck query

  Scenario: Limit above 200 is clamped to 200
    Given the IA manifest resolves to a parquet url
    And DuckDB captures the query text and returns one row
    When queryArchivedTable is called for "contratos" with limit 5000
    Then the executed SQL should contain "LIMIT 200"

  Scenario: Unknown table is rejected without touching the manifest
    Given the IA manifest endpoint is never called
    When queryArchivedTable is called for "not_a_table" filtered by "cnpj_orgao"
    Then the result should be a failure with reason "sql_error"

  Scenario: prefetchArchive warms the manifest without booting DuckDB
    Given the IA manifest resolves to a parquet url
    When prefetchArchive is called for "contratos"
    Then DuckDB should not have been initialized

  Scenario: A later queryArchivedTable after prefetchArchive reuses the warmed manifest
    Given prefetchArchive has warmed the manifest for "contratos"
    When queryArchivedTable is called for "contratos" filtered by "cnpj_orgao"
    Then the IA manifest should have been fetched exactly once
    And DuckDB should have been initialized exactly once

  Scenario: queryArchivedOrgaos requests the orgaos parquet snapshot
    Given the IA manifest resolves to a parquet url for every table
    When queryArchivedOrgaos is called
    Then the query should be routed to the "orgaos" parquet snapshot

  Scenario: queryArchivedUnidades requests the unidades parquet snapshot
    Given the IA manifest resolves to a parquet url for every table
    When queryArchivedUnidades is called
    Then the query should be routed to the "unidades" parquet snapshot

  Scenario: queryArchivedFornecedores requests the fornecedores parquet snapshot
    Given the IA manifest resolves to a parquet url for every table
    When queryArchivedFornecedores is called
    Then the query should be routed to the "fornecedores" parquet snapshot

  Scenario: Malformed manifest rows are skipped with a warning
    Given the IA manifest endpoint returns a CSV with a valid contratos row and a malformed row missing parquet_url
    When getLatestParquetInfo is called for "contratos"
    Then the valid row's parquet url should be returned
    And console.warn should log "[ia-manifest] skipping malformed row" once
