@journey4
Feature: Journey 4 — Informed citizen
  A non-specialist read a news headline and wants to verify the underlying
  contract without learning procurement jargon. They need plain-language
  search, an inline glossary, and visible data freshness.
  See VISION.md → "Informed citizen".

  @green @search
  Scenario: Search by hospital name without knowing the CNPJ
    # Covered by web/features/search-hero.feature: Free text triggers a PNCP search and renders results listbox.
    Given the user opens the home page
    When the user types "hospital municipal" into the search box
    Then the user sees a results listbox with at least one link

  @planned @glossary
  Scenario: Hover a technical term to see a plain-language definition
    # Planned: glossary tooltips are not implemented.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    When the user hovers the term "Dispensa"
    Then the user sees a tooltip explaining what a "Dispensa" is in plain language

  @planned @plain-language
  Scenario: Detail page renders a plain-language summary above the schema dump
    # Planned: plain-language summary is not generated today.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the user sees a one-paragraph summary that names the buyer, supplier, value and what was bought

  @wip @freshness
  Scenario: Data freshness is visible on every detail page
    # WIP: /status shows freshness; detail pages do not echo it.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the user sees the snapshot date of the underlying Parquet within the page header

  @planned @plain-language
  Scenario: Geographic context is shown when available
    # Planned: no geo enrichment today.
    Given the user opens "/municipio?ibge=3550308"
    Then the user sees the municipality population and the state it belongs to

  @green @search
  Scenario: Crossover with journey 3 — citizen reaches the same permalink a journalist would cite
    # crosses @journey3
    # Covered by web/features/search-hero.feature: Submitting a canonical PNCP contract ID navigates to the contract page.
    Given the user types "12345678000195-1-000001/2024" into the search box
    When the user submits the search form
    Then the browser navigates to "/baliza/contratacao?id=12345678000195-1-000001/2024"
