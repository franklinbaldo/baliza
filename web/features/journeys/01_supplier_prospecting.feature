@journey1
Feature: Journey 1 — B2G supplier prospecting
  A vendor that sells to the public sector wants to discover new buyers,
  benchmark prices, and understand who already supplies what they sell.
  See VISION.md → "B2G supplier prospecting".

  @green @market
  Scenario: Search for an object opens an aggregated market page
    # Planned: route /mercado/{objeto} does not exist yet.
    Given the user opens the home page
    When the user submits the free-text query "merenda escolar"
    Then the user lands on a market page summarizing top buyers, top suppliers and price ranges
    And the page shows the number of distinct contracts found

  @green @search
  Scenario: Filter contracts by UF and modality recomputes aggregates
    # Covered: BuscaView renders UF + modality dropdowns above the result
    # list with a count / total / average aggregate strip that recomputes
    # client-side as the dropdowns change.
    Given a search result list is visible
    When the user picks UF "SP" and modality "Pregão Eletrônico"
    Then the visible aggregates (count, total value, average value) reflect the filtered subset

  @green @search
  Scenario: Empty search suggests accent-tolerant alternatives
    # Covered: empty-result state on BuscaView consults a closed lexicon
    # (web/src/lib/accentLexicon.ts, ~80 procurement nouns) to offer the
    # accented spelling as a clickable suggestion.
    Given the user submits the free-text query "construcao de escola"
    When PNCP returns zero results
    Then the user sees a suggestion to retry with "construção de escola"

  @green @supplier
  Scenario: Supplier page by CNPJ shows history, peers and average ticket
    # Covered by SupplierDetailView at /fornecedor?cnpj= backed by the Parquet
    # snapshot (PNCP has no supplier-by-CNPJ endpoint).
    Given the user opens "/fornecedor?cnpj=12345678000195"
    Then the user sees the supplier's contract history
    And the user sees the supplier's top three competing CNPJs for the same objects
    And the user sees the supplier's average ticket size

  @green @export
  Scenario: Export the current search result as CSV
    # Covered: BuscaView ships an "Exportar CSV" button that builds an
    # RFC 4180-style CSV from the visible (filtered) result list using
    # toCsv() in lib/exporters.ts and triggers a Blob download.
    Given a search result list is visible for "merenda escolar"
    When the user clicks "Exportar CSV"
    Then a CSV file is downloaded with named columns matching the visible table

  @planned @market
  Scenario: Crossover with journey 2 — supplier inspects the buyer's pricing reference
    # crosses @journey2
    Given a market page for "merenda escolar" is visible
    When the user clicks "Ver pesquisa de preços"
    Then the user sees the same evidenced price reference shown to public buyers
