Feature: Automatic Retry on Transient Errors
  As a user,
  I want the "extract" command to automatically retry on transient network errors,
  So that the pipeline can recover without manual intervention.

  @tier1
  Scenario: The "extract" command succeeds after a transient error
    Given a clean local data store
    And an external data source that will fail once before succeeding
    When I run the "extract" command
    Then the command should succeed
    And the external data source should have been called twice
    And the final dataset should contain all records
