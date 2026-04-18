Feature: Search Hero
  The search input detects patterns and navigates to the right page, or
  queries the PNCP search API for free text.

  Scenario: Detect CNPJ pattern and show correct jump link
    Given the user types "12345678000195" into the search box
    Then I should see "Explorar Órgão" as a jump suggestion
    And the link should point to "/baliza/orgao?cnpj=12345678000195"

  Scenario: Detect IBGE code and show correct jump link
    Given the user types "3550308" into the search box
    Then I should see "Explorar Município" as a jump suggestion
    And the link should point to "/baliza/municipio?ibge=3550308"

  Scenario: Detect PNCP ID pattern and show jump link
    Given the user types "12345678000195-1-000001/2024" into the search box
    Then I should see "Ver Contratação" as a jump suggestion

  Scenario: Short query shows no jump suggestion
    Given the user types "ab" into the search box
    Then I should not see any jump suggestion

  Scenario: Hint chips fill the input
    Given the search hero has loaded
    When the user clicks the first hint chip
    Then the input value should not be empty

  Scenario: Search input has accessible label
    When the search hero loads
    Then the input should have an accessible label

  Scenario: Submitting a CNPJ navigates to the agency page
    Given the user types "12345678000195" into the search box
    When the user submits the search form
    Then the browser should navigate to "/baliza/orgao?cnpj=12345678000195"

  Scenario: Submitting an IBGE code navigates to the municipality page
    Given the user types "3550308" into the search box
    When the user submits the search form
    Then the browser should navigate to "/baliza/municipio?ibge=3550308"

  Scenario: Submitting a PNCP contract ID navigates to the contract page
    Given the user types "12345678000195-1-000001-1" into the search box
    When the user submits the search form
    Then the browser should navigate to "/baliza/contratacao?id=12345678000195-1-000001-1"

  Scenario: Free text triggers a PNCP search and renders results listbox
    Given the PNCP search API returns two results for "hospital"
    When the user types "hospital" into the search box
    Then I should see a results listbox with at least one link
    And the first result should link to a contratação page

  Scenario: PNCP search failure shows an alert banner
    Given the PNCP search API fails for "hospital"
    When the user types "hospital" into the search box
    Then I should see an alert banner about the search error

  Scenario: PNCP search zero results shows an empty state
    Given the PNCP search API returns zero results for "xyzneverexists"
    When the user types "xyzneverexists" into the search box
    Then I should see an empty state for the search
