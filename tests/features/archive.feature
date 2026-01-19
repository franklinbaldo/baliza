Feature: Internet Archive Inspection
  As a data manager
  I want to inspect the status of Internet Archive uploads
  So that I can verify the integrity and availability of the data

  Scenario: Displaying the status of an archive
    Given a known Internet Archive identifier
    When I run the baliza archive status command with the known identifier
    Then the archive status should be displayed

  Scenario: Attempting to display the status of a non-existent archive
    Given an unknown Internet Archive identifier
    When I run the baliza archive status command with the unknown identifier
    Then an error message should be displayed
