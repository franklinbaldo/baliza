Feature: End-to-end contratacoes extraction
  As a data engineer,
  I want to run the extraction pipeline for 'contratacoes',
  So that I have a complete local dataset for this resource.

  Scenario: The data pipeline can extract contratacoes
    Given a clean local data store
    And the PNCP API is mocked for 'contratacoes' for a specific date range
    When I extract 'contratacoes' data for the date range
    Then the final dataset should contain all 'contratacoes' records for the date range
