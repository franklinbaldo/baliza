@journey5 @journey6 @planned
Feature: PNCP Resource Atlas
  Baliza is evolving from a contratos-centered browser into a temporal
  procurement intelligence system that mirrors several first-class PNCP
  resources. This feature documents the architectural contract that every
  PNCP resource — registered today or planned — must satisfy so that the
  pipeline, the archive layer and the frontend can discover them
  generically.

  The conceptual spine is:

    PCA          -> intended demand / future procurement
    Publicações  -> current opportunity / what is open now
    Atas         -> reusable purchasing instruments
    Contratos    -> finalized legal/economic facts
    Itens        -> item-level comparability and price intelligence

  The "Atlas" is the union of every registered resource plus every planned
  resource that has been declared in code with documented gaps. Promotion
  from planned to registered is what these scenarios protect.

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

  Scenario: Atas is a registered resource alongside contratos
    When I look up "atas" in the registry
    Then the canonical table name is "atas"
    And the primary key is "numero_controle_pncp_ata"
    And the raw monthly ZIP filename is `raw-atas-{YYYY-MM}.zip`
      so it cannot collide with the contratos ZIP for the same partition

  Scenario: Publicações is declared as a first-class planned resource
    When I look up "publicacoes" in the planned resource atlas
    Then it declares the PNCP endpoint `/v1/contratacoes/publicacao`
    And it links to the existing Pydantic model `RecuperarCompraPublicacaoDTO`
    And it documents that the endpoint requires fan-out by `codigoModalidadeContratacao`
    And it carries TODO markers naming the missing facts that block ingestion
      (modalidade fan-out strategy, canonical column list, primary key choice)

  Scenario: PCA is declared as a documented future resource
    When I look up "pca" in the planned resource atlas
    Then it declares the PNCP endpoint family under `/v1/pca`
    And it links to the existing Pydantic model `PlanoContratacaoComItensDoUsuarioDTO`
    And it documents that PCA partitioning is annual, not monthly
    And it carries TODO markers for the canonical flatten function and PK

  Scenario: Frontend discovers archive resources from manifest metadata
    Given the IA manifest exposes a `table_name` column per row
    When the frontend lists archive tables
    Then every distinct `table_name` value is recognized as an `ArchivedTable`
    And every `ArchivedTable` has a human-readable label and short description
    And the label set covers at minimum: contratos, atas

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
