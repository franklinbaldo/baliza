@journey5
Feature: Journey 5 — Academic researcher
  An economist or political scientist needs a reproducible dataset, a
  documented schema, an academic citation block, a changelog of schema
  changes, and bulk download of the Parquet snapshots.
  See VISION.md → "Academic researcher".

  @green @schema
  Scenario: Table schemas are reachable from the explorer
    # Covered by DuckDBExplorer schema sidebar fed by SCHEMA_MAP.
    Given the user opens "/explorador"
    Then the user sees a sidebar listing the tables available in the loaded Parquet
    And the user can expand a table to see column names and types

  @green @explorer
  Scenario: Explorer query is encoded in the URL for reproducibility
    # Covered by web/features/duckdb-explorer.feature: Resolved IA URL replaces IA_URL placeholder in featured query chips.
    Given the user opens "/explorador"
    When the user runs a non-default SQL query
    Then the page URL contains the query in a "sql" parameter

  @planned @changelog
  Scenario: Schema changelog page lists historical changes
    # Planned: /schema/changelog does not exist.
    Given the user opens "/schema/changelog"
    Then the user sees a reverse-chronological list of column additions, removals and renames

  @green @citation
  Scenario: Generate a citable reference block for the current snapshot
    # Covered by CitationBlock on /sobre (citation trigger, BibTeX block, citation metadata).
    Given the user opens "/sobre"
    When the user clicks "Gerar citação acadêmica"
    Then a BibTeX block is rendered with the snapshot date and Internet Archive item URL

  @green @explorer
  Scenario: Each Parquet snapshot is identified by hash and date
    # Covered by web/features/archive-fallback.feature: manifest resolution flow.
    Given the user opens "/explorador"
    When the explorer mounts
    Then the manifest loaded from Internet Archive includes a snapshot date for each Parquet entry

  @green @schema
  Scenario: Crossover with journey 6 — researcher hands a stable URL to a developer integrator
    # crosses @journey6
    # Backed by ManifestTable.svelte on /desenvolvedores: it surfaces the
    # same sha256 hashes the explorer's citation block reads from the
    # manifest, proving both code paths share one source of truth.
    Given the user opens "/desenvolvedores"
    Then the user sees the same manifest entry hashes used by the explorer
