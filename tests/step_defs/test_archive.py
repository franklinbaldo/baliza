from pytest_bdd import scenario, given, when, then, parsers
from typer.testing import CliRunner
import pytest

from baliza.cli import app


@pytest.fixture
def runner():
    return CliRunner()


from unittest.mock import MagicMock


@scenario(
    "../features/archive.feature",
    "Displaying the status of an archive",
)
def test_display_archive_status():
    pass


@scenario(
    "../features/archive.feature",
    "Attempting to display the status of a non-existent archive",
)
def test_display_non_existent_archive_status():
    pass


@given("a known Internet Archive identifier", target_fixture="known_ia_identifier")
def known_ia_identifier_fixture(mocker: MagicMock):
    mock_item = MagicMock()
    mock_item.exists = True
    mock_item.item_size = 12345
    mock_item.metadata = {
        'publicdate': '2024-07-01T12:00:00Z',
        'updatedate': '2024-07-02T12:00:00Z',
    }

    mock_session = MagicMock()
    mock_session.get_item.return_value = mock_item

    mocker.patch('baliza.cli.get_session', return_value=mock_session)
    return "baliza-contratos-2024-07"


@given("an unknown Internet Archive identifier", target_fixture="unknown_ia_identifier")
def unknown_ia_identifier_fixture(mocker: MagicMock):
    mock_item = MagicMock()
    mock_item.exists = False

    mock_session = MagicMock()
    mock_session.get_item.return_value = mock_item

    mocker.patch('baliza.cli.get_session', return_value=mock_session)
    return "non-existent-identifier"


@when(parsers.parse("I run the baliza archive status command with the known identifier"), target_fixture="archive_status_result")
def run_archive_status_command_known(runner: CliRunner, known_ia_identifier: str):
    result = runner.invoke(app, ["archive", "status", known_ia_identifier])
    return result


@when(parsers.parse("I run the baliza archive status command with the unknown identifier"), target_fixture="archive_status_result")
def run_archive_status_command_unknown(runner: CliRunner, unknown_ia_identifier: str):
    result = runner.invoke(app, ["archive", "status", unknown_ia_identifier])
    return result


@then("the archive status should be displayed")
def archive_status_displayed(archive_status_result, known_ia_identifier):
    result = archive_status_result
    assert result.exit_code == 0
    assert "Exists" in result.stdout
    assert "Yes" in result.stdout
    assert "12345" in result.stdout


@then("an error message should be displayed")
def error_message_displayed(archive_status_result, unknown_ia_identifier):
    result = archive_status_result
    assert result.exit_code == 1
    assert f"Identifier '{unknown_ia_identifier}' not found." in result.output
