@journey5 @journey6
Feature: PNCP Resource Atlas
  Baliza mirrors four first-class PNCP resources that together form a
  temporal procurement intelligence spine:

    PCA          -> intended demand / future procurement
    Publicações  -> current opportunity / what is open now
    Atas         -> reusable purchasing instruments
    Contratos    -> finalized legal/economic facts

  Every resource registered in ``RESOURCES`` satisfies the same
  architectural contract so the pipeline, archive layer and frontend
  can discover and process them generically.

  Background:
    Given the PNCP resource registry exposed by `src/baliza/resources/`

  Scenario: A registered PNCP resource declares a complete contract
    Given a resource registered in `RESOURCES`
    Then it declares an endpoint path under PNCP `/v1`
    And it declares a pagination parameter and page size parameter
    And it declares a date-range start parameter and end parameter
    And it declares a raw filename strategy that does not collide with other resources
    And it declares at least one canonical table with a schema_version and a primary key
    And it declares the first date on which PNCP exposed source data, or explicitly leaves it open

  Scenario: Contratos is the reference resource
    When I look up "contratos" in the registry
    Then the canonical table name is "contratos"
    And the schema_version is "2.0.0"
    And the primary key is "numero_controle_pncp"
    And the raw monthly ZIP filename keeps the legacy `raw-{YYYY-MM}.zip` shape
      so existing Internet Archive items do not need to be renamed

  @drift-0
  Scenario: The frontend exposure list mirrors the live registry
    Given the PNCP resource registry exposed by `src/baliza/resources/`
    When `scripts/build_frontend_config.py` writes
      `web/src/lib/generated/frontend_exposures.ts`
    Then the file contains one entry per `frontend_exposures` entry
      across every resource registered in `RESOURCES`
    And no entry comes from a separate parallel registry
      (the legacy `src/baliza/pncp_resources.py` module no longer exists)
    And both `contratos` and `atas` show up as `table_alias`
      so the website's archive page lists atas alongside contratos

  Scenario: Atas is a registered resource alongside contratos
    When I look up "atas" in the registry
    Then the canonical table name is "atas"
    And the primary key is "numero_controle_pncp_ata"
    And the raw monthly ZIP filename is `raw-atas-{YYYY-MM}.zip`
      so it cannot collide with the contratos ZIP for the same partition

  Scenario: Publicações is a registered resource with modalidade fan-out
    When I look up "publicacoes" in the registry
    Then it declares the PNCP endpoint `/v1/contratacoes/publicacao`
    And the primary key is "numero_controle_pncp"
    And `FetchSpec.required_params['codigoModalidadeContratacao']` covers IDs 1..14
      so the extractor fans out across all modalidades per partition window
    And the raw monthly ZIP filename is `raw-publicacoes-{YYYY-MM}.zip`
      so it cannot collide with contratos or atas ZIPs for the same partition

  Scenario: PCA is a registered resource with annual partitioning
    When I look up "pca" in the registry
    Then it declares the PNCP endpoint `/v1/pca/atualizacao`
    And the composite primary key is `[id_pca_pncp, numero_item]`
      because multiple items share an id_pca_pncp across planning cycles
    And the `partition_strategy` is `ANNUAL`
      so iter_partitions emits one year-label per calendar year
    And the raw annual ZIP filename is `raw-pca-{YYYY}.zip`
    And the only artifact is `annual_canonical`
      (no monthly_canonical — the annual file is the canonical for PCA)

  Scenario: Frontend discovers archive resources from manifest metadata
    Given the IA manifest exposes a `table_name` column per row
    When the frontend lists archive tables
    Then every distinct `table_name` value is recognized as an `ArchivedTable`
    And every `ArchivedTable` has a human-readable label and short description
    And the label set covers at minimum: contratos, atas, publicacoes, pca

  Scenario: A resource that is registered but has no partitions yet
    Given a resource registered in `RESOURCES`
    And the IA manifest has no rows whose `table_name` equals that resource
    When the frontend asks for that resource's latest partition
    Then it returns an empty result without raising
    And the explorer renders an empty-state explaining "ainda sem partições publicadas"

  Scenario: Adding a new resource is a single-file change
    Given a developer wants to register a new PNCP resource
    When they create `src/baliza/resources/<name>.py` and add an entry to `RESOURCES`
    Then no other backend module needs to be edited for the resource to be
      addressable from the CLI via `--resource <name>`
    And the only additional editing required to surface the resource in the
      frontend is to add a label/description in `web/src/lib/resourceCatalog.ts`

  Scenario: Resource names are filesystem- and SQL-safe
    Given any resource name accepted by `PNCPResource.__post_init__`
    Then the name matches `^[a-zA-Z0-9_]+$`
    And the name can be safely interpolated into raw paths, ZIP filenames,
      URL params and DuckDB identifiers without further escaping
