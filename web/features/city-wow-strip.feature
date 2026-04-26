@journey4 @green
Feature: City Wow Strip
  The homepage's first-fold magnitude tiles for the active city,
  computed in-browser from the archived parquet via DuckDB-WASM.
  Includes the silent auto-detect on CityHero that backs the
  "returning visitor sees their city without lifting a finger" UX.

  Scenario: Show skeleton tiles while archive aggregates are loading
    Given the city aggregates query is still pending
    When the wow strip mounts for "Porto Velho"
    Then I should see 4 skeleton tiles

  Scenario: Render 4 tiles with formatted pt-BR values
    Given the archive returns 12847 contracts totaling R$ 4200000000 for "Porto Velho"
    When the wow strip mounts for "Porto Velho"
    Then I should see "12.847" in the tiles
    And I should see "R$ 4.200.000.000,00" in the tiles
    And the contracts tile should link to "/busca?ibge=1100205"

  Scenario: Empty city renders a sober zero state
    Given the archive returns 0 contracts for "Alta Floresta D'Oeste"
    When the wow strip mounts for "Alta Floresta D'Oeste"
    Then I should see "0" in the tiles
    And I should see an empty-state message

  Scenario: Snapshot tile shows partition date and IA attribution
    Given the archive returns 1 contract with snapshot "2026-04-16" for "Porto Velho"
    When the wow strip mounts for "Porto Velho"
    Then I should see "16/04" in the snapshot tile
    And the snapshot tile should mention "Internet Archive"

  Scenario: Hide wow strip silently when archive fails
    Given the archive query fails with "no_manifest"
    When the wow strip mounts for "Porto Velho"
    Then the wow strip section should not be in the document

  Scenario: Auto-detect city when geolocation permission is already granted
    Given navigator.permissions reports geolocation as "granted"
    And the geo helper resolves to São Paulo
    When the city hero mounts with the default city source
    Then setCity should be called with São Paulo

  Scenario: Skip auto-detect when permission state is prompt
    Given navigator.permissions reports geolocation as "prompt"
    When the city hero mounts with the default city source
    Then setCity should not be called

  Scenario: Skip auto-detect when the visitor already has a stored city
    Given the active city was loaded from storage
    And navigator.permissions reports geolocation as "granted"
    When the city hero mounts
    Then setCity should not be called
