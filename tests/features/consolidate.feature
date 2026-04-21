Feature: Annual consolidation to Internet Archive
  As a steward of PNCP history
  I want `consolidate` to roll monthly parquet files into one annual file
  So that past years become a single self-contained download

  @tier1
  Scenario: Consolidate produces an annual parquet from daily files
    Given monthly parquet fixtures exist for 2023
    And IA credentials are configured
    When I run IAConsolidator.consolidate_year for 2023 with force=True
    Then the consolidator should report success
    And the IA upload mock should have been called for item "baliza-pncp-consolidated"
    And the uploaded files should include "contratos-2023.parquet"

  @tier1
  Scenario: Consolidate skips an already-consolidated frozen year
    Given IA credentials are configured
    And the consolidated 2023 file already exists on Internet Archive
    When I run IAConsolidator.consolidate_year for 2023 without force
    Then the consolidator should report no-op
    And the IA upload mock should not have been called
