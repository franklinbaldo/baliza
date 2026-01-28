Feature: State Management
  As a user of the Baliza CLI
  I want to inspect the application's state
  So that I can understand the data coverage and extraction history.

  @tier1
  Scenario: Show current application state
    Given a pre-existing database with known state
    When the user runs "baliza state show"
    Then the output should summarize the database state
