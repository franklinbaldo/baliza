Feature: contratacoes extraction
  As a user, I want to extract 'contratacoes' data from the PNCP API.

  Scenario: Basic extraction of contratacoes
    Given a clean database
    And a mock PNCP API for "contratacoes"
    When I run the extract command for "contratacoes" from "2024-12-01" to "2024-12-01"
    Then the "contratacoes" table should contain 1 record
