Feature: Resumable Extraction via Checkpoints
  As a data pipeline operator
  I want the extract command to be resumable
  So that I don't lose work if the process is interrupted

  @tier1
  Scenario: Extraction resumes from a checkpoint after interruption
    Given the PNCP API will return 2 pages for the date "2024-01-01"
    And the first page has 10 rows
    And the second page has 5 rows
    And the API will be interrupted after the first page is fetched
    When I run "baliza extract --start 2024-01-01 --end 2024-01-01"
    Then the command should fail
    And the database should contain a checkpoint for "2024-01-01" at page 1 with 10 rows
    And the database should contain 10 rows for "2024-01-01"

    When I run "baliza extract --start 2024-01-01 --end 2024-01-01" again without interruption
    Then the command should succeed
    And the extraction should resume from page 2
    And the database should contain a total of 15 rows for "2024-01-01"
    And the checkpoint for "2024-01-01" should be cleared
