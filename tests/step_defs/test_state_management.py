"""Step definitions for the state management feature."""

import pytest
from pytest_bdd import scenario


@pytest.mark.skip(reason="CLI commands 'state show', 'state gaps', and 'state history' are not implemented yet.")
@scenario(
    "../features/state_management.feature",
    "Summarizing the current state of the data store",
)
def test_show_state_summary():
    """Summarizing the current state of the data store."""


@pytest.mark.skip(reason="CLI commands 'state show', 'state gaps', and 'state history' are not implemented yet.")
@scenario(
    "../features/state_management.feature",
    "Listing coverage gaps",
)
def test_list_gaps():
    """Listing coverage gaps."""


@pytest.mark.skip(reason="CLI commands 'state show', 'state gaps', and 'state history' are not implemented yet.")
@scenario(
    "../features/state_management.feature",
    "Reviewing the history of extraction runs",
)
def test_show_history():
    """Reviewing the history of extraction runs."""
