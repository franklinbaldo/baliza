@journey4 @green
Feature: Homepage Dashboard
  The landing page shows sync stats with proper loading and error states.

  Scenario: Show skeleton loading state before stats arrive
    Given the stats fetch is still pending
    When the dashboard mounts
    Then I should see skeleton placeholder cards

  Scenario: Render stats after successful fetch
    Given the fetch returns total_contracts 99000
    When the dashboard mounts
    Then I should see "99000" in the stats

  Scenario: Show error banner when stats fail to load
    Given the stats fetch fails with "Falha na rede"
    When the dashboard mounts
    Then I should see an error alert
    And the alert should contain "Falha na rede"

  Scenario: No skeleton visible after data loads
    Given the fetch returns valid stats
    When the dashboard has finished loading
    Then I should not see any aria-busy element
