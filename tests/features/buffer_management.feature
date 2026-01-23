Feature: Buffer Management
  As a data engineer
  I want the buffer to be cleaned after successful IA upload
  So that artifact size stays manageable

  @tier1
  Scenario: Buffer contains only recent unstable data
    Given extraction has run for 10 days
    And days 1-3 have been uploaded to Internet Archive
    When I check buffer stats
    Then the buffer should contain data for days 4-10 only
    And days 1-3 should be recorded in uploaded_to_ia table

  @tier1
  Scenario: Cleanup removes data after successful upload
    Given the buffer contains 5000 rows for 2023-01-15
    And 2023-01-15 was successfully uploaded to Internet Archive
    When cleanup runs for 2023-01-15
    Then the buffer should not contain data for 2023-01-15
    And the state tables should still exist

  @tier2
  Scenario: Get dates ready for export
    Given the buffer contains data for the last 14 days
    And stability window is 7 days
    When I query dates ready for export
    Then only dates older than 7 days should be returned
    And dates already uploaded to IA should be excluded

  @tier2
  Scenario: Buffer stats show current state
    Given the buffer contains 10000 rows across 5 dates
    And 2 dates have been uploaded to IA
    When I get buffer stats
    Then total_rows should be 10000
    And dates_in_buffer should be 5
    And dates_uploaded_to_ia should be 2
