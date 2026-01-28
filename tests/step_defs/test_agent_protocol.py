from pytest_bdd import scenarios, given, when, then

scenarios('../features/agent_protocol.feature')

@given('the maildir directories are empty')
def maildir_directories_are_empty():
    pass

@when('the Builder Agent sends a STATUS message with thread "CG-0012"')
def builder_agent_sends_status_message():
    pass

@then("a new message file should exist in the PM's \"new\" inbox")
def a_new_message_file_should_exist_in_the_pm_new_inbox():
    pass

@then("a new message file should exist in the Builder's \"new\" inbox")
def a_new_message_file_should_exist_in_the_builder_new_inbox():
    pass

@then("a new message file should exist in the Builder's \"cur\" sentbox")
def a_new_message_file_should_exist_in_the_builder_cur_sentbox():
    pass

@then('the message in the PM\'s inbox should have the kind "status"')
def message_in_pm_inbox_should_have_kind_status():
    pass

@then('the message in the PM\'s inbox should have the thread "CG-0012"')
def message_in_pm_inbox_should_have_thread_cg_0012():
    pass

@given('the PM Agent has sent a message with "requires_ack: true"')
def pm_agent_has_sent_a_message_with_requires_ack_true():
    pass

@when('the Builder Agent runs the ACK process')
def builder_agent_runs_the_ack_process():
    pass

@then("a new ACK message should be sent to the PM's \"new\" inbox")
def a_new_ack_message_should_be_sent_to_the_pm_new_inbox():
    pass

@then('the ACK message should reference the original message ID')
def ack_message_should_reference_the_original_message_id():
    pass

@given("there is an open PR from the PM Agent containing new messages")
def there_is_an_open_pr_from_the_pm_agent_containing_new_messages():
    pass

@when('the Builder Agent runs the SYNC process')
def builder_agent_runs_the_sync_process():
    pass

@then("the new messages from the PR should be saved to the Builder's \"new\" inbox")
def the_new_messages_from_the_pr_should_be_saved_to_the_builder_new_inbox():
    pass
