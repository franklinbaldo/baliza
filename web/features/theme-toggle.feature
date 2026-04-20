@journey3 @journey4 @journey5 @journey7 @green
Feature: Theme Toggle
  A visitor can toggle between light and dark themes, and their choice
  sticks across reloads and respects system preferences on first visit.

  Scenario: Theme toggle persists choice across reloads
    Given the page loads with no stored theme preference
    When the user clicks the theme toggle button
    Then the theme should change
    And the new theme should persist after page reload

  Scenario: Stored theme preference is restored on page load
    Given a stored theme preference of "light" exists
    When the toggle mounts
    Then the button reflects the stored light theme

  Scenario: Theme toggle respects prefers-color-scheme on first visit
    Given the system prefers dark color scheme
    And there is no stored theme preference
    When the page loads
    Then the document should use the dark theme

  Scenario: Theme toggle respects prefers-color-scheme light on first visit
    Given the system prefers light color scheme
    And there is no stored theme preference
    When the page loads
    Then the document should use the light theme

  Scenario: Theme toggle is operable via keyboard
    Given the theme toggle button is focused
    When the user presses Enter
    Then the theme should toggle
    And the button should reflect the current theme state via aria-pressed

  Scenario: Theme toggle activates via Space key
    Given the theme toggle button is focused
    When the user presses Space
    Then the theme should toggle
