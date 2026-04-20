@journey3 @journey5 @journey6 @green
Feature: DuckDB SQL Explorer
  The explorer lets analysts run SQL against Parquet files from Internet Archive.

  Scenario: Initial state shows explore button and default query
    When the explorer mounts
    Then I should see "Explorar Dados"
    And the SQL textarea should contain "SELECT"

  Scenario: Featured query chip inserts SQL into textarea
    Given the explorer has mounted
    When the user clicks a featured query chip
    Then the SQL textarea should contain the chip's SQL

  Scenario: IA_URL placeholder shows warning and disables button
    Given the explorer has mounted
    When the user clicks a featured query chip that still contains "IA_URL"
    Then I should see a placeholder warning
    And the explore button should be disabled

  Scenario: Valid SQL executes and renders results
    Given DuckDB returns one row with column "n" equal to 1
    And the explorer has mounted
    When the user clicks "Explorar Dados"
    Then I should see a results table with "n" as a column header

  Scenario: Resolved IA URL replaces IA_URL placeholder in featured query chips
    Given the IA manifest resolves to "https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet"
    And the explorer has mounted
    When the user clicks a featured query chip
    Then the SQL textarea should contain the resolved parquet URL
    And the SQL textarea should not contain "IA_URL"

  Scenario: Single quotes in resolved URL are escaped to prevent SQL injection
    Given the IA manifest resolves to a URL containing a single quote
    And the explorer has mounted
    When the user clicks a featured query chip
    Then the SQL textarea should contain the URL with doubled single quotes

  Scenario: Active query is backfilled when IA URL resolves after a chip click
    Given the IA manifest is slow to resolve to "https://archive.org/download/baliza-pncp-2025-01/contratos-2025-01.parquet"
    And the explorer has mounted
    And the user clicked a featured query chip before the manifest resolved
    When the manifest finishes resolving
    Then the SQL textarea should contain the resolved parquet URL
    And the explore button should be enabled
