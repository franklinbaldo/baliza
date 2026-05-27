@phase2 @pca-demand
Feature: PCA — planned demand by CATMAT/CATSER code
  PCA (Plano de Contratações Anuais) is a public PNCP endpoint that
  exposes future procurement intent at the item level — including
  `codigoItem` (CATMAT/CATSER). Surfacing it transforms `catmat.json`
  from a static lookup into a queryable map of planned demand.

  Reference: `docs/plans/pncp-resource-pipeline.md` §4 Phase 2.
  Pre-req: Drift-A (composite PK) + Drift-D (annual partitioning).

  Rule: PCA is presented as planned demand, never as contracted fact

    @green
    Scenario: A PCA detail page warns the reader that the data is intent
      Given the user opens a PCA item detail page
      Then the page carries a banner warning
        """
        Esses dados indicam intenção/planejamento informado ao PNCP,
        não contratação realizada.
        """
      And every numeric value on the page is labelled "estimado"

  Rule: A user can pivot from a CATMAT code to its planned demand

    @green
    Scenario: The CATMAT search results page links to the PCA listing
      Given the user typed a CATMAT/CATSER code into the search box
      When the search resolves to a known code
      Then the result card includes a link "Ver demanda planejada (PCA)"
      And clicking it lands on `/pca?codigo=<code>`

    @green
    Scenario: The PCA listing groups planned items by year
      Given the route `/pca?codigo=7510` is opened
      When the data is loaded
      Then items are grouped by `anoPca`
      And each year shows the total planned `valorTotal` and item count
      And the source PCA URL on the PNCP portal is linked per item

  Rule: PCA partitions are annual, not monthly

    @green
    Scenario: The archive layer returns the right yearly partition
      Given the registered `pca` resource declares annual partitioning
      When the explorer asks for the latest PCA partition
      Then the answer is a year (e.g. "2026"), not a month
      And the manifest carries `data_particao = "2026"` for that row

  Rule: PCA item identity is composite

    @green
    Scenario: Re-ingesting a PCA item with the same composite key replaces
      Given an item with `(id_pca_pncp, numero_item) = (X, 1)` was ingested
      When the same `(X, 1)` is ingested again with updated `valorTotal`
      Then there is exactly one row for `(X, 1)` in `pca_itens_canonical`
      And the new `valorTotal` wins
