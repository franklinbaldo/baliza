Feature: Data Verification
  As a data engineer
  I want to verify the data coverage and integrity
  So that I can identify any gaps, inconsistencies, or quality issues in the extracted data

  Background:
    Given I have a DuckDB database with coverage tracking enabled
    And the PNCP API is available for verification queries

  Scenario: Verifying complete coverage
    Given a DuckDB database with complete data for January 2024
    And all date windows were successfully extracted
    When I run "baliza verify --start 2024-01-01 --end 2024-01-31"
    Then the verification should pass successfully
    And should show 100% coverage
    And should display "✓ All windows verified successfully"

  Scenario: Verifying coverage identifies missing windows
    Given data extraction was never run for 2024-01-15 to 2024-01-20
    When I run "baliza verify --start 2024-01-01 --end 2024-01-31"
    Then the verification should identify gaps
    And should list the missing date range: 2024-01-15 to 2024-01-20
    And gap reason should be "missing"
    And should show coverage percentage (e.g., 80%)

  Scenario: Verification shows gap reasons
    Given I have various types of gaps in my data
    When I run "baliza verify --start 2024-01-01 --end 2024-01-31"
    Then the output should categorize gaps by reason:
      | Gap Reason   | Icon | Description                                    |
      | missing      | ⚠    | Window was never extracted                     |
      | incomplete   | ⚠    | Window extraction was interrupted              |
      | suspect      | 🔍   | Page count differs from previous extraction    |
      | lookback     | 🔄   | Window falls within lookback period            |

  Scenario: Verification displays coverage statistics
    Given a partially complete dataset
    When I run "baliza verify"
    Then the output should show:
      | Metric                | Example Value |
      | Total windows         | 365           |
      | Complete windows      | 340           |
      | Incomplete windows    | 10            |
      | Suspect windows       | 5             |
      | Missing windows       | 10            |
      | Coverage percentage   | 93.2%         |

  Scenario: Verification compares local vs API page counts
    Given a window with 10 pages in local database
    When I verify and the API now reports 15 pages for that window
    Then the window should be flagged as "suspect"
    And the output should show: "Expected: 10 pages, API reports: 15 pages"
    And should recommend re-extracting the suspect window

  Scenario: Verification detects suspect windows
    Given a window was extracted on 2024-01-01 with totalPaginas=10
    And the PNCP API now returns totalPaginas=15 for the same window
    When I run "baliza verify"
    Then the window should be marked as suspect
    And the verification should explain the page count mismatch
    And should suggest: "Run 'baliza extract' to update"

  Scenario: Verification uses manifesto hashes for integrity
    Given a window with stored hash SHA256:abc123...
    When I verify the data
    Then the verify command should fetch page 1 from the API
    And should compare totalPaginas with the stored value
    And if totalPaginas differs, should mark as suspect
    And if totalPaginas matches, should mark as ok

  Scenario: Verification outputs human-readable summary
    Given I run verification on a large dataset
    When the verification completes
    Then the output should include a summary panel
    And should use Rich formatting with colors
    And should highlight issues in yellow/red
    And should show success in green
    And should be easy to read in terminal

  Scenario: Verification filters by date range
    Given I have data from 2023 and 2024
    When I run "baliza verify --start 2024-01-01 --end 2024-12-31"
    Then only 2024 windows should be verified
    And 2023 data should be ignored
    And the statistics should only reflect 2024

  Scenario: Verification filters by resource
    Given I have extracted contratos and compras
    When I run "baliza verify --resource contratos"
    Then only contratos coverage should be verified
    And compras should be excluded
    When I run "baliza verify --resource compras"
    Then only compras coverage should be verified

  Scenario: Verification with no coverage data
    Given the coverage table is empty
    When I run "baliza verify"
    Then the command should display: "No coverage data found"
    And should suggest running "baliza extract" first
    And should exit with a non-zero code

  Scenario: Verification detects incomplete extractions
    Given a window extraction was interrupted mid-process
    And the coverage table shows 2 of 5 expected pages
    When I run "baliza verify"
    Then the window should be flagged as "incomplete"
    And should show "2/5 pages extracted"
    And should recommend completing the extraction

  Scenario: Verification shows icons for gap types
    Given I have multiple types of gaps
    When I run "baliza verify"
    Then missing windows should show ⚠ icon
    And incomplete windows should show ⚠ icon
    And suspect windows should show 🔍 icon
    And lookback windows should show 🔄 icon
    And complete windows should show ✓ icon

  Scenario: Verification performance for large datasets
    Given I have 10 years of data (3,650 windows)
    When I run "baliza verify"
    Then verification should complete in reasonable time (< 5 minutes)
    And should only fetch page 1 from API (not all pages)
    And should use efficient database queries
    And should show progress for long-running verifications
