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
    Then I should see "99.000" in the stats

  Scenario: KPI numbers render with Brazilian thousand separator
    Given the fetch returns total_contracts 5804
    When the dashboard mounts
    Then I should see "5.804" in the stats

  Scenario: Quarantine KPI links to the glossary anchor
    Given the fetch returns valid stats
    When the dashboard has finished loading
    Then the "Em quarentena" card should link to "/baliza/sobre#quarentena"

  Scenario: Show error banner when stats fail to load
    Given the stats fetch fails with "Falha na rede"
    When the dashboard mounts
    Then I should see an error alert
    And the alert should contain "Falha na rede"

  Scenario: No skeleton visible after data loads
    Given the fetch returns valid stats
    When the dashboard has finished loading
    Then I should not see a loading skeleton
