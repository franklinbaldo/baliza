@journey3
Feature: Journey 3 — Investigative journalist
  A reporter chasing a story needs evidence that survives a libel suit:
  permanent links, back-links to PNCP, shareable searches, and exports
  ready to paste into a CMS.
  See VISION.md → "Investigative journalist".

  @green @permalink
  Scenario: Every contract has a stable permanent URL
    # Covered by web/features/contract-detail.feature: Successful fetch renders all detail blocks.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the URL of the page is shareable and resolves the same contract on reload

  @green @permalink
  Scenario: Each contract page links back to the original PNCP record
    # Covered by web/features/contract-detail.feature: Successful fetch renders formatted currency and links.
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the user sees an outbound link to the origin system that opens in a new tab

  @green @search
  Scenario: Search state is preserved in the query string
    # Covered by SearchHero pushState on submit + onMount ?q= restore.
    Given the user submits the free-text query "hospital municipal"
    Then the page URL contains "?q=hospital%20municipal"
    And reloading the page restores the same result list

  @planned @export
  Scenario: Export a result list as Markdown
    # Planned: only CSV export exists, in the explorer.
    Given a search result list is visible for "hospital municipal"
    When the user clicks "Exportar Markdown"
    Then the clipboard contains a Markdown table with headers and rows

  @green @agency-rollup
  Scenario: Agency page surfaces top suppliers and a time series
    # Covered by AgencyDetailView rollup cards (top-suppliers + monthly-chart).
    Given the user opens "/orgao?cnpj=00000000000191"
    Then the user sees the top five suppliers for that agency
    And the user sees a monthly contract-count chart for the last twelve months

  @green @permalink
  Scenario: Crossover with journey 7 — journalist subscribes to a saved query
    # crosses @journey7
    # Covered by SearchHero "Acompanhar esta busca" button → /alertas/<slug>.xml offer.
    Given a search result list is visible for "hospital municipal"
    When the user clicks "Acompanhar esta busca"
    Then the user is offered an RSS URL for new matches
