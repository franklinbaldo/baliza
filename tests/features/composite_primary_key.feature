@planned @drift-a
Feature: BalizaEngine.upsert_rows accepts a composite primary key
  Drift-A in `docs/plans/pncp-resource-pipeline.md` §3: today
  `BalizaEngine.upsert_rows(pk: str)` only accepts a single column.
  PCA's canonical row keys on `(id_pca_pncp, numero_item)`, so this
  blocker must be cleared before Phase 2 of the resource roadmap.

  These scenarios pin the behavioral contract the new signature must
  preserve. They are deliberately black-box — they describe what an
  outside caller observes, not the dedup mechanism.

  Background:
    Given an empty in-memory `BalizaEngine`

  Rule: Single-column PK keeps working byte-identically

    Scenario: Upserting twice with the same single-column PK replaces
      Given the engine has no `widgets` table
      When I upsert the rows
        | id  | value |
        | "A" | 1     |
      And I upsert the rows
        | id  | value |
        | "A" | 2     |
      Then the engine has exactly one row in `widgets`
      And the row with `id = "A"` has `value = 2`

  Rule: Composite PK deduplicates on the tuple of columns

    Scenario: Two rows differing only in the second PK column coexist
      Given the engine has no `parts` table with PK ["assembly", "slot"]
      When I upsert the rows
        | assembly | slot | qty |
        | "AX"     | 1    | 10  |
        | "AX"     | 2    | 5   |
      Then the engine has exactly two rows in `parts`
      And no row was treated as a duplicate of another

    Scenario: Re-upserting a row with the same composite PK replaces
      Given the engine has no `parts` table with PK ["assembly", "slot"]
      When I upsert the rows
        | assembly | slot | qty |
        | "AX"     | 1    | 10  |
      And I upsert the rows
        | assembly | slot | qty |
        | "AX"     | 1    | 99  |
      Then the engine has exactly one row in `parts`
      And the row keyed by `("AX", 1)` has `qty = 99`

    Scenario: Composite PK rejects rows with NULL in any key column
      Given the engine has no `parts` table with PK ["assembly", "slot"]
      When I upsert the rows
        | assembly | slot | qty |
        | "AX"     | null | 1   |
      Then the upsert raises an integrity error
      And no rows are inserted

  Rule: The PNCP resource registry drives the PK shape

    Scenario Outline: Each canonical table's declared PK is honoured at upsert
      Given the resource "<resource>" is registered in `RESOURCES`
      And the canonical table for "<resource>" declares PK <pk>
      When the pipeline upserts a row for that table
      Then the engine deduplicates by the same <pk>

      Examples:
        | resource  | pk                                              |
        | contratos | "numero_controle_pncp"                          |
        | atas      | "numero_controle_pncp_ata"                      |
        | pca       | ["id_pca_pncp", "numero_item"]                  |
