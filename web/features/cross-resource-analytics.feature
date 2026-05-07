@planned @phase4 @cross-resource
Feature: Cross-resource analytics
  Phase 4 of `docs/plans/pncp-resource-pipeline.md` §4: once intent
  (PCA), open opportunities (publicações), reusable instruments (atas),
  finalized contracts (contratos) and item-level data (itens) all live
  side-by-side, the explorer should make their relationships
  first-class.

  Pre-req: Phases 1–3 of the resource roadmap. Every scenario is
  @planned. Promotion happens by tagging individual scenarios @green
  in the PR that ships them.

  Background:
    Given every Phase 1–3 resource is registered in `RESOURCES`

  Rule: PCA × contratos — did the agency contract what it promised?

    Scenario: An agency dashboard shows planned vs realised
      Given an agency has a PCA for year 2026 with R$ 10M planned
      And the same agency has contratos in 2026 totalling R$ 7M
      When the user opens that agency's dashboard
      Then a "PCA cumprido" gauge shows 70%
      And clicking the gauge lists the unfulfilled CATMAT codes

    Scenario: The dashboard joins on `cnpj_orgao` AND time window
      Given two agencies share a `codigo_unidade` but have distinct CNPJs
      When the dashboard computes the gauge
      Then the join uses `cnpj_orgao` as the primary key
      And only contratos whose `data_publicacao` falls within the PCA year count

  Rule: Publicações × itens — open opportunities with price evidence

    Scenario: An open opportunity card shows historical reference price
      Given a publicação has `codigoItem = 7510` (CATMAT for office paper)
      And `itens_contratos` contains 50 prior rows for that code
      When the user opens the opportunity detail page
      Then a "Referência de preço" panel shows median, min and max R$
      And the panel cites the parquet snapshot date the prices came from

  Rule: Atas × contratos — vigentes vs já-contratada

    Scenario: An ata page distinguishes vigent from already-spent slots
      Given an ata has `numero_controle_pncp_compra = X`
      And there is at least one contrato whose `numero_controle_pncp_compra = X`
      When the user opens the ata detail page
      Then the slot is marked "já contratada (ver contrato)"
      And a link points to the resolved contract's permalink
