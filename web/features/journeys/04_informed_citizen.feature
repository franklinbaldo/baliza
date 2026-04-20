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

  @green @plain-language
  Scenario: Detail page renders a plain-language summary above the schema dump
    # Covered by ContractDetailView plain-language summary block.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the user sees a one-paragraph summary that names the buyer, supplier, value and what was bought

  @green @freshness
  Scenario: Data freshness is visible on every detail page
    # Covered by ContractDetailView snapshot badge (pulled from manifest dataParticao).
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the user sees the snapshot date of the underlying Parquet within the page header

  @green @search
  Scenario: Crossover with journey 3 — citizen reaches the same permalink a journalist would cite
    # crosses @journey3
    # Covered by web/features/search-hero.feature: Submitting a canonical PNCP contract ID navigates to the contract page.
    Given the user types "12345678000195-1-000001/2024" into the search box
    When the user submits the search form
    Then the browser navigates to "/baliza/contratacao?id=12345678000195-1-000001/2024"
