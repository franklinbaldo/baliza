@journey7
Feature: Journey 7 — Auditor and watchdog
  A recurring observer (NGO, control body, accountability journalist) wants
  to be told when something matches a saved pattern: new contracts for a
  CNPJ, exemptions above a value threshold, anomalies in a state.
  Static-only hosting: alerts travel via RSS files that the daily
  extraction workflow rebuilds and uploads to Internet Archive.
  See VISION.md → "Auditor / watchdog".

  @planned @alerts
  Scenario: Save the current query as a watch in localStorage
    # Planned: watch persistence is not implemented. Static-compatible —
    # localStorage only; no backend state.
    Given a search result list is visible for "dispensa acima de 1 milhão"
    When the user clicks "Salvar vigilância"
    Then a watch entry is persisted in localStorage
    And the watch appears in the user's "Minhas vigilâncias" list

  @planned @rss
  Scenario: Curated RSS feed on Internet Archive publishes new matches
    # Planned: the daily PNCP sync (pncp-sync.yml → ia_uploader) does not
    # yet emit feed-{slug}.xml alongside the monthly parquet. Feeds are for
    # curated public watches pre-configured in the repo (seeded from
    # web/src/lib/homepage-content.ts:FEATURED_QUERIES), not arbitrary
    # user queries — that keeps the architecture static-only.
    Given a curated watch "dispensas-acima-1mi" is configured in the repo
    When the user opens "https://archive.org/download/baliza-pncp-feeds/feed-dispensas-acima-1mi.xml"
    Then the response is a valid RSS 2.0 document
    And each item links to a /contratacao permalink

  @planned @diff
  Scenario: Diff view shows what changed since the last visit
    # Planned: visit-diff is not implemented. Static-compatible — the
    # client compares the current manifest's data_particao against a
    # stored snapshot timestamp in localStorage.
    Given the user has previously visited a saved watch
    When the user opens the watch again
    Then the user sees a "novidades desde sua última visita" section listing only new matches

  @planned @alerts
  Scenario: Subscribe to a CNPJ from its agency page
    # Planned: creates a local watch from the agency page. Static-compatible —
    # localStorage only.
    Given the user opens "/orgao?cnpj=00000000000191"
    When the user clicks "Receber alertas deste órgão"
    Then a watch is created with the agency CNPJ as the filter

  @planned @alerts
  Scenario: Crossover with journey 4 — citizen turns a one-off search into a watch
    # crosses @journey4
    Given the user has just inspected a contract via /contratacao
    When the user clicks "Acompanhar este fornecedor"
    Then the user is offered a watch form pre-filled with the supplier CNPJ
