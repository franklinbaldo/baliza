@journey7
Feature: Journey 7 — Auditor and watchdog
  A recurring observer (NGO, control body, accountability journalist) wants
  to be told when something matches a saved pattern: new contracts for a
  CNPJ, exemptions above a value threshold, anomalies in a state.
  See VISION.md → "Auditor / watchdog".

  @green @alerts
  Scenario: Save the current query as a watch in localStorage
    # Covered by SearchHero "Salvar vigilância" → baliza.watches key + WatchList.
    Given a search result list is visible for "dispensa acima de 1 milhão"
    When the user clicks "Salvar vigilância"
    Then a watch entry is persisted in localStorage
    And the watch appears in the user's "Minhas vigilâncias" list

  @planned @rss
  Scenario: RSS feed publishes new matches for a saved watch
    # Planned: /alertas/{slug}.xml route does not exist.
    Given a watch named "dispensas-acima-1mi" exists
    When the user opens "/alertas/dispensas-acima-1mi.xml"
    Then the response is a valid RSS 2.0 document
    And each item links to a /contratacao permalink

  @planned @webhook
  Scenario: Webhook fires on new matches via a stateless GitHub Action
    # Planned: alerting infra is not implemented.
    Given a watch with webhook URL "https://example.org/hook" exists
    When a daily extraction adds a contract that matches the watch
    Then the GitHub Action posts a JSON payload to the webhook URL
    And the payload contains the matching contract's PNCP ID

  @planned @diff
  Scenario: Diff view shows what changed since the last visit
    # Planned: visit-diff is not implemented.
    Given the user has previously visited a saved watch
    When the user opens the watch again
    Then the user sees a "novidades desde sua última visita" section listing only new matches

  @green @alerts
  Scenario: Subscribe to a CNPJ from its agency or supplier page
    # Covered by AgencyDetailView "Receber alertas deste órgão" button → saveWatch kind=cnpj-agency.
    Given the user opens "/orgao?cnpj=00000000000191"
    When the user clicks "Receber alertas deste órgão"
    Then a watch is created with the agency CNPJ as the filter

  @green @alerts
  Scenario: Crossover with journey 4 — citizen turns a one-off search into a watch
    # crosses @journey4
    # Covered by ContractDetailView watch card: "Acompanhar este fornecedor" → form pre-filled with supplier CNPJ.
    Given the user has just inspected a contract via /contratacao
    When the user clicks "Acompanhar este fornecedor"
    Then the user is offered a watch form pre-filled with the supplier CNPJ
