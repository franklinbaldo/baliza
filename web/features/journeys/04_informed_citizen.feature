@journey4
Feature: Journey 4 — Informed citizen
  A non-specialist read a news headline and wants to verify the underlying
  contract without learning procurement jargon. They need plain-language
  search, an inline glossary, and visible data freshness.
  See VISION.md → "Informed citizen".

  @planned @search
  Scenario: Search by hospital name without knowing the CNPJ
    # Planned: free-text PNCP search from the homepage will return when a
    # dedicated /busca page ships.
    Given the user opens the home page
    When the user types "hospital municipal" into the search box
    Then the user sees a results listbox with at least one link

  @green @glossary
  Scenario: Hover a technical term to see a plain-language definition
    # Backed by Glossary.svelte + lib/glossary.ts wrapping the modality cell
    # on ContractDetailView. Static-compatible — a bundled term→definition
    # map rendered by a Svelte tooltip.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    When the user hovers the term "Dispensa"
    Then the user sees a tooltip explaining what a "Dispensa" is in plain language

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

  @planned @plain-language
  Scenario: Geographic context is shown when available
    # Planned: no geo enrichment today. Static-compatible — a bundled
    # IBGE code → municipality/state/region lookup (JSON) is enough.
    Given the user opens "/municipio?ibge=3550308"
    Then the user sees the municipality population and the state it belongs to

  @planned @search
  Scenario: Crossover with journey 3 — citizen reaches the same permalink a journalist would cite
    # crosses @journey3
    # Planned: type-a-PNCP-id-and-jump lives on the future /busca page.
    Given the user types "12345678000195-1-000001/2024" into the search box
    When the user submits the search form
    Then the browser navigates to "/baliza/contratacao?id=12345678000195-1-000001/2024"
