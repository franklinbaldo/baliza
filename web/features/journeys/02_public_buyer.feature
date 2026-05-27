@journey2
Feature: Journey 2 — Public buyer
  A mayor, secretary or pregoeiro is about to make a purchase and needs to
  defend it against audit. They need vigent frameworks, evidenced reference
  prices, peer comparisons, and a clean catalog match.
  See VISION.md → "Public buyer".

  @green @frameworks
  Scenario: Browse vigent registered-price frameworks for an object
    # Covered by AtasView at /atas?objeto= (archive-only, ILIKE + vigent
    # date filter). Remaining quantity is not in the Parquet snapshot, so
    # the UI shows agency, objeto, validity window and value.
    Given the user opens "/atas?objeto=papel%20A4"
    Then the user sees a list of vigent atas with start date, end date and contracting agency

  @green @price-reference
  Scenario: Generate a price reference and export it as a citable PDF
    Given the user opens a market page for "papel A4"
    When the user clicks "Gerar pesquisa de preços"
    Then a PDF is produced containing min, average, median, max and standard deviation of unit price
    And the PDF includes the source contract IDs and snapshot date

  @green @market
  Scenario: Compare procurement practice across municipalities of similar size
    # Planned: peer-comparison page does not exist.
    Given the user opens "/comparar?ibge=3550308&objeto=merenda"
    Then the user sees three peer municipalities of similar population
    And the user sees the per-capita spend for the same object

  @green @catmat
  Scenario: Resolve a CATMAT or CATSER code from a free-text description
    # Planned: catalog resolver does not exist. Static-compatible — a
    # bundled CATMAT taxonomy JSON (or Parquet on IA queried via DuckDB WASM)
    # satisfies the lookup without a backend.
    Given the user types "papel sulfite branco A4 75g" into a catalog input
    Then the user sees the most likely CATMAT codes ranked by match confidence

  @green @catmat
  Scenario: Look up a short CATMAT code by typing its 2-digit number
    # catmat.json has 83 codes with 1–2 digit codes (e.g. "37" AGENDA).
    # The search threshold must be ≤ 2 so exact-code lookups are not blocked.
    Given the user types "37" into a catalog input
    Then the user sees the CATMAT entry for code "37"

  @green @frameworks
  Scenario: Inspect the legal basis cited by peers in similar exemptions
    # Covered by DispensasView at /dispensas?objeto= — paginates modality 8
    # (Dispensa) from PNCP, filters by objeto, aggregates fundamentacaoLegal.
    Given the user opens "/dispensas?objeto=papel%20A4"
    Then the user sees the most cited legal articles in similar dispensa contracts

  @green @price-reference
  Scenario: Crossover with journey 3 — buyer audits a peer's contract before riding on it
    # crosses @journey3
    # Covered by ContractDetailView (modalidade, valores, órgão + outbound link).
    Given the user opens "/contratacao?id=00000000000191-1-000001/2024"
    Then the user sees the contract's value, modality and supplier
    And the user sees an outbound link to the original PNCP record
