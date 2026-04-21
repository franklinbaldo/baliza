Feature: PNCPExtractor resilience
  As a backfill operator
  I want the extractor to resume from its raw cache and retry transient errors
  So that one failed page does not wipe out a month of work

  @tier1
  Scenario: Resumes from a cached page instead of refetching
    Given a cached PNCP page for 2023-01 page 1
    When I fetch the same page with the network set to fail
    Then the extractor should return the cached payload
    And the PNCP API should not have been called

  @tier1
  Scenario: Retries a transient 5xx before succeeding
    Given PNCP returns 500 twice then 200
    When I fetch contratos for 2023-01 page 1
    Then the extractor should return the 200 payload
    And the PNCP API should have been called 3 times
