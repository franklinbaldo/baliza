Feature: Agent Protocol Communication
  As a Builder Agent, I need to communicate with the PM Agent
  using a maildir-based protocol to receive instructions and report status.

  Scenario: Builder Agent sends a STATUS message
    Given the maildir directories are empty
    When the Builder Agent sends a STATUS message with thread "CG-0012"
    Then a new message file should exist in the PM's "new" inbox
    And a new message file should exist in the Builder's "new" inbox
    And a new message file should exist in the Builder's "cur" sentbox
    And the message in the PM's inbox should have the kind "status"
    And the message in the PM's inbox should have the thread "CG-0012"

  Scenario: Builder Agent receives a message requiring an ACK
    Given the PM Agent has sent a message with "requires_ack: true"
    When the Builder Agent runs the ACK process
    Then a new ACK message should be sent to the PM's "new" inbox
    And the ACK message should reference the original message ID

  Scenario: Builder Agent synchronizes messages from PM's PR
    Given there is an open PR from the PM Agent containing new messages
    When the Builder Agent runs the SYNC process
    Then the new messages from the PR should be saved to the Builder's "new" inbox
