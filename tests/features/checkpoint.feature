@tier0
Feature: Extraction Checkpointing
  As a data engineer
  I want extraction to checkpoint progress per page
  So that I can resume from where I left off after a timeout

  @tier0
  Scenario: Extraction saves checkpoint after each page
    Given a clean local data store
    And an external data source with 3 pages of data
    When I extract the first page
    Then a checkpoint should exist with current_page=1
    And the database should contain data from page 1

  @tier0
  Scenario: Extraction resumes from checkpoint
    Given an existing checkpoint at page 2 of 5
    And 1000 rows already extracted
    When I resume extraction
    Then extraction should start from page 3
    And the final dataset should contain all 5 pages

  @tier1
  Scenario: Checkpoint is cleared after successful completion
    Given an extraction in progress with checkpoint at page 3
    When extraction completes successfully
    Then no checkpoint should exist for that date
    And coverage status should be 'complete'

  @tier1
  Scenario: Partial extraction data is preserved on timeout
    Given extraction starts for a date range
    And 5 pages are successfully extracted
    When extraction times out before completing
    Then the 5 pages of data should be in the database
    And a checkpoint should exist at page 5
