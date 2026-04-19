@journey4 @green
Feature: Dashboard Stats
  The dashboard displays contract stats with loading and error states.

  Scenario: Show loading skeletons while fetching
    Given the fetch is pending
    When the dashboard mounts
    Then I should see 3 skeleton cards

  Scenario: Render stats when fetch succeeds
    Given the fetch returns total_contracts 42000
    When the dashboard mounts
    Then I should see "42000" in a stat card

  Scenario: Show AlertBanner on fetch error
    Given the fetch throws "Falha na rede"
    When the dashboard mounts
    Then I should see an alert banner with level error
    And the alert should contain "Falha na rede"

  Scenario: Skeleton cards not visible after data loads
    Given the fetch returns valid stats
    When the dashboard has finished loading
    Then I should not see any aria-busy element
