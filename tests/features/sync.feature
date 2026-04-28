Feature: Sync a month to Internet Archive
  As the baliza operator
  I want `baliza sync` to extract, ingest, and upload one month
  So that the IA archive stays current without manual steps

  @tier0
  Scenario: Sync uploads a pending month to Internet Archive
    Given PNCP returns a single page of contracts
    And IA credentials are configured
    When I run baliza sync --force-month 2023-01 with IA mocked
    Then the sync command should exit successfully
    And the IA upload mock should have been called for item "baliza-pncp-2023-01"
    And the uploaded files for "baliza-pncp-2023-01" should include "contratos-2023-01.parquet"

  @tier1
  Scenario: Sync skips months already in the manifest
    Given the IA manifest lists every pending month
    And IA credentials are configured
    When I run baliza sync without --force-month
    Then the sync command should exit successfully
    And the IA upload mock should not have been called

  @tier1
  Scenario: Sync re-queues months whose manifest entry has no sha256
    Given the IA manifest lists a month with parquet_url but no sha256
    And PNCP returns a single page of contracts
    And IA credentials are configured
    When I run baliza sync without --force-month
    Then the sync command should exit successfully
    And the IA upload mock should have been called for item "baliza-pncp-2023-01"

  @tier1
  Scenario: Sync in dry-run mode extracts but does not upload
    Given PNCP returns a single page of contracts
    When I run baliza sync --force-month 2023-01 --dry-run
    Then the sync command should exit successfully
    And the IA upload mock should not have been called
    And the raw JSON cache for 2023-01 should exist

  @tier1
  Scenario: Sync surfaces an error when IA keys are missing
    Given IA credentials are missing
    When I run baliza sync --force-month 2023-01 without dry-run
    Then the sync command should fail with "Missing IA keys"
