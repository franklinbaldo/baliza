@journey7
Feature: Journey 7 — Auditor and watchdog
  A recurring observer (NGO, control body, accountability journalist) wants
  to be told when something matches a saved pattern: new contracts for a
  CNPJ, exemptions above a value threshold, anomalies in a state.
  Static-only hosting: alerts travel via RSS files that the daily
  extraction workflow rebuilds and uploads to Internet Archive.
  See VISION.md → "Auditor / watchdog".

  @green @alerts
  Scenario: Save the current query as a watch in localStorage
    # Covered by BuscaView at /busca?q= — a "Salvar vigilância" button
    # appears when results are present; clicking it calls addWatch('query')
    # from watchStore.svelte.ts and persists the entry in localStorage.
    # The underlying watchStore.svelte.ts (addWatch /
    # isWatched / hydrateWatches) is wired and tested via the agency- and
    # contract-page entry points below; this scenario stays @planned until
    # the search-results page surfaces the same affordance.
    Given a search result list is visible for "dispensa acima de 1 milhão"
    When the user clicks "Salvar vigilância"
    Then a watch entry is persisted in localStorage
    And the watch appears in the user's "Minhas vigilâncias" list

  @green @rss
  Scenario: Curated RSS feed on Internet Archive publishes new matches
    # Implemented: src/baliza/rss_feed.py builds RSS 2.0 XML from a list of
    # contract rows and IAUploader.upload_feeds emits feed-{slug}.xml to
    # baliza-pncp-feeds for each entry in CURATED_WATCHES (mirrored by
    # FEATURED_QUERIES in web/src/lib/homepage-content.ts). The scenario
    # asserts the published shape via a mocked fetch + DOMParser.
    Given a curated watch "dispensas-acima-1mi" is configured in the repo
    When the user opens "https://archive.org/download/baliza-pncp-feeds/feed-dispensas-acima-1mi.xml"
    Then the response is a valid RSS 2.0 document
    And each item links to a /contratacao permalink

  @green @diff
  Scenario: Diff view shows what changed since the last visit
    # Implemented: /vigilancia?id={watch-id} renders WatchDetailView, which
    # captures the previous lastVisited timestamp before stamping a new one
    # via markVisited() and surfaces a data-testid="watch-diff-section"
    # block listing only matches with dataPublicacaoPncp > cutoff.
    Given the user has previously visited a saved watch
    When the user opens the watch again
    Then the user sees a "novidades desde sua última visita" section listing only new matches

  @green @alerts
  Scenario: Subscribe to a CNPJ from its agency page
    # Planned: creates a local watch from the agency page. Static-compatible —
    # localStorage only.
    Given the user opens "/orgao?cnpj=00000000000191"
    When the user clicks "Receber alertas deste órgão"
    Then a watch is created with the agency CNPJ as the filter

  @green @alerts
  Scenario: Crossover with journey 4 — citizen turns a one-off search into a watch
    # crosses @journey4
    Given the user has just inspected a contract via /contratacao
    When the user clicks "Acompanhar este fornecedor"
    Then the user is offered a watch form pre-filled with the supplier CNPJ
