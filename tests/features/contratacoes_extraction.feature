Feature: Extract contratacoes_publicacao data from PNCP API

  As a user of the Baliza CLI,
  I want to extract `contratacoes_publicacao` data,
  so that I can analyze procurement publications.

  Scenario: Successfully extract contratacoes_publicacao
    Given the PNCP API will return a list of contratacoes_publicacao
    When I run the extract command for "contratacoes_publicacao"
    Then the "contratacoes_publicacao" table should contain the extracted data

  Scenario: Extract contratacoes_publicacao with no results
    Given the PNCP API will return an empty list for contratacoes_publicacao
    When I run the extract command for "contratacoes_publicacao"
    Then the "contratacoes_publicacao" table should be empty
