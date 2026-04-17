Feature: Local Bids Geolocation
  The local bids panel finds nearby contracts using the browser geolocation API.

  Scenario: Initial idle state shows activate button
    When the local bids panel loads
    Then I should see "Ativar Localizador"

  Scenario: Permission denied shows helpful empty state
    Given the browser geolocation returns a PERMISSION_DENIED error
    When the user clicks "Ativar Localizador"
    Then I should see "Acesso à localização negado"
    And I should see a link to search manually

  Scenario: General geolocation error shows error message
    Given the browser geolocation fails with a general error
    When the user clicks "Ativar Localizador"
    Then I should see a location failure message

  Scenario: Success with no results shows empty state
    Given geolocation succeeds with IBGE "1721000"
    And the PNCP API returns 0 results
    When the user activates the locator
    Then I should see "Nenhuma contratação recente"
