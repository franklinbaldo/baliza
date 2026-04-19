@journey6
Feature: Journey 6 — Developer integrator
  A third-party developer wants to consume Baliza's data inside their own
  project: a stable manifest, language-specific consumption examples, an
  embeddable explorer, and CORS that allows direct Parquet fetches.
  See VISION.md → "Developer integrator".

  @planned @manifest
  Scenario: Developer landing page lists the Parquet manifest
    # Planned: /desenvolvedores does not exist.
    Given the user opens "/desenvolvedores"
    Then the user sees a table with one row per Parquet file, including URL, sha256 hash, snapshot date and size in bytes

  @planned @api
  Scenario: Consumption examples are shown for Python, R and JavaScript
    # Planned: examples page does not exist.
    Given the user opens "/desenvolvedores"
    Then the user sees a Python (pandas) snippet that reads a Parquet file from Internet Archive
    And the user sees an R (arrow) snippet that does the same
    And the user sees a JavaScript (DuckDB WASM) snippet that does the same

  @green @api
  Scenario: Internet Archive item naming convention is stable
    # Covered by web/features/archive-fallback.feature: featured queries resolve a known IA URL.
    Given the manifest is loaded
    Then every contratos parquet URL matches the pattern "baliza-pncp-YYYY-MM/contratos-YYYY-MM.parquet"

  @planned @embed
  Scenario: Explorer can be embedded via an iframe with a query string
    # Planned: embed mode is not implemented.
    Given a third-party page loads "/explorador?sql=SELECT%201&embed=true" inside an iframe
    Then the chrome (header, footer, navigation) is hidden
    And the SQL is auto-executed on mount

  @wip @cors
  Scenario: Parquet files are fetchable from a third-party domain
    # WIP: assumed working today via Internet Archive defaults; no explicit test.
    Given a third-party page issues a fetch for a contratos Parquet URL
    Then the response includes an Access-Control-Allow-Origin header that does not block the request

  @green @api
  Scenario: Crossover with journey 5 — integrator and researcher rely on the same manifest
    # crosses @journey5
    # Covered by web/features/archive-fallback.feature: manifest resolution flow.
    Given the manifest is loaded
    Then the manifest exposes the same hash and date used by the academic citation block
