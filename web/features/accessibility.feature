Feature: Accessibility and Interaction Design
  As a user requiring high-contrast and keyboard navigation,
  I want to be able to focus interactive elements like cards and buttons,
  So that I can effectively navigate the system.

  Scenario: Important interactive elements receive proper focus states
    Given a dashboard page with interactive elements
    When the user navigates using the keyboard
    Then the ".btn" button receives focus
    And the ".bid-link-card" element receives focus
