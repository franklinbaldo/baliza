Feature: Search Hero
  The search input detects patterns and navigates to the right page.

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
