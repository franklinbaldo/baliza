import pytest
import requests
from tenacity import RetryError # Keep for test_client_main_block_success_and_failure_paths if it expects RetryError
from pytest_httpserver import HTTPServer

from baliza.client import fetch_data_from_pncp, BASE_URL, alert_slack_on_retry_failure

# Test the alert_slack_on_retry_failure callback
def test_alert_slack_on_retry_failure(mocker):
    mock_print = mocker.patch("builtins.print")
    mock_typer_echo = mocker.patch("baliza.client.typer.echo")

    class MockOutcome:
        def exception(self):
            return ValueError("Test exception")

    class MockRetryState:
        def __init__(self, attempt_number, args, kwargs, outcome):
            self.attempt_number = attempt_number
            self.args = args
            self.kwargs = kwargs
            self.outcome = outcome

    retry_state = MockRetryState(
        attempt_number=3,
        args=("test_arg",),
        kwargs={"key": "value"},
        outcome=MockOutcome()
    )

    with pytest.raises(ValueError, match="Test exception"): # Expect ValueError due to re-raise
        alert_slack_on_retry_failure(retry_state)

    mock_print.assert_called_once()
    mock_typer_echo.assert_called_once()
    call_args = mock_typer_echo.call_args[0][0]
    assert "PNCP request failed ultimately after 3 attempts" in call_args
    assert "Test exception" in call_args


def test_fetch_data_success(httpserver: HTTPServer):
    endpoint_path = "/v1/test/success"
    expected_data = {"key": "value", "items": [{"id": 1}, {"id": 2}]}
    httpserver.expect_request(endpoint_path).respond_with_json(expected_data)

    data = fetch_data_from_pncp(endpoint_path, params={"page": 1})
    assert data == expected_data
    assert len(httpserver.log) == 1

def test_fetch_data_http_error_500_then_success(httpserver: HTTPServer):
    endpoint_path = "/v1/test/retry_500_success"
    expected_data = {"status": "ok_finally_500"}

    httpserver.expect_ordered_request(endpoint_path).respond_with_data("Internal Server Error", status=500)
    httpserver.expect_ordered_request(endpoint_path).respond_with_data("Internal Server Error", status=500)
    httpserver.expect_ordered_request(endpoint_path).respond_with_json(expected_data)

    data = fetch_data_from_pncp(endpoint_path, params={})
    assert data == expected_data
    assert len(httpserver.log) == 3

def test_fetch_data_http_error_429_then_success(httpserver: HTTPServer, mocker):
    mock_typer_echo = mocker.patch("baliza.client.typer.echo")
    endpoint_path = "/v1/test/retry_429_success"
    expected_data = {"status": "ok_finally_429"}

    httpserver.expect_ordered_request(endpoint_path).respond_with_data("Too Many Requests", status=429)
    httpserver.expect_ordered_request(endpoint_path).respond_with_data("Too Many Requests", status=429)
    httpserver.expect_ordered_request(endpoint_path).respond_with_json(expected_data)

    data = fetch_data_from_pncp(endpoint_path, params={})
    assert data == expected_data
    assert len(httpserver.log) == 3

    rate_limit_message_found = False
    for call_args in mock_typer_echo.call_args_list:
        if "Rate limited (429)" in call_args[0][0]:
            rate_limit_message_found = True
            break
    assert rate_limit_message_found, "typer.echo was not called with a rate limit message"


def test_fetch_data_all_retries_fail_503(httpserver: HTTPServer, mocker):
    endpoint_path = "/v1/test/persistent_failure_503" # Unique path
    mock_alert_callback = mocker.patch("baliza.client.alert_slack_on_retry_failure")

    for _ in range(7):
        httpserver.expect_request(endpoint_path).respond_with_data("Service Unavailable", status=503)

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        fetch_data_from_pncp(endpoint_path, params={})

    assert len(httpserver.log) == 7
    assert excinfo.value.response.status_code == 503

    assert mock_alert_callback.called
    if mock_alert_callback.called:
        retry_state_arg = mock_alert_callback.call_args[0][0]
        assert retry_state_arg.attempt_number == 7
        assert isinstance(retry_state_arg.outcome.exception(), requests.exceptions.HTTPError)

def test_fetch_data_server_timeout_504_then_success(httpserver: HTTPServer):
    endpoint_path = "/v1/test/server_timeout_504"
    expected_data = {"message": "finally_after_504"}

    httpserver.expect_ordered_request(endpoint_path).respond_with_data("Gateway Timeout", status=504)
    httpserver.expect_ordered_request(endpoint_path).respond_with_data("Gateway Timeout", status=504)
    httpserver.expect_ordered_request(endpoint_path).respond_with_json(expected_data)

    data = fetch_data_from_pncp(endpoint_path, params={"attempt": 1})
    assert data == expected_data
    assert len(httpserver.log) == 3

def test_fetch_data_json_decode_error_retries_and_fails(httpserver: HTTPServer, mocker):
    endpoint_path = "/v1/test/json_decode_error_persistent" # Unique path
    mock_alert_callback = mocker.patch("baliza.client.alert_slack_on_retry_failure")

    for _ in range(7):
        httpserver.expect_request(endpoint_path).respond_with_data("This is not JSON", content_type="application/json")

    with pytest.raises(requests.exceptions.JSONDecodeError):
        fetch_data_from_pncp(endpoint_path, params={})

    assert len(httpserver.log) == 7

    assert mock_alert_callback.called
    if mock_alert_callback.called:
        retry_state_arg = mock_alert_callback.call_args[0][0]
        assert retry_state_arg.attempt_number == 7
        assert isinstance(retry_state_arg.outcome.exception(), requests.exceptions.JSONDecodeError)

def test_fetch_data_404_not_found_retries_and_fails(httpserver: HTTPServer, mocker):
    endpoint_path = "/v1/test/not_found_persistent"
    mock_alert_callback = mocker.patch("baliza.client.alert_slack_on_retry_failure")

    for _ in range(7):
        httpserver.expect_request(endpoint_path).respond_with_data("Not Found", status=404)

    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        fetch_data_from_pncp(endpoint_path, params={})

    assert len(httpserver.log) == 7
    assert excinfo.value.response.status_code == 404

    assert mock_alert_callback.called
    if mock_alert_callback.called:
        retry_state_arg = mock_alert_callback.call_args[0][0]
        assert retry_state_arg.attempt_number == 7
        assert isinstance(retry_state_arg.outcome.exception(), requests.exceptions.HTTPError)
        assert retry_state_arg.outcome.exception().response.status_code == 404

def test_client_main_block_success_and_failure_paths(httpserver: HTTPServer, mocker, capsys):
    success_endpoint = "/v1/contratacoes/publicacao_main"
    success_params = {"dataInicial": "2023-01-01", "dataFinal": "2023-01-01", "pagina": 1, "tamanhoPagina": 1}
    success_data = {"items": [{"id": "test_main_1"}], "totalPaginas": 1}

    fail_endpoint = "/v1/nonexistent/path_main"
    fail_params = {"pagina": 1}

    httpserver.expect_request(success_endpoint, query_string=success_params).respond_with_json(success_data)

    for _ in range(7):
         httpserver.expect_request(fail_endpoint, query_string=fail_params).respond_with_data("Not Found", status=404)

    mock_typer_echo_main = mocker.patch("baliza.client.typer.echo")

    # Simulate first call in __main__-like logic
    data_main_success = fetch_data_from_pncp(success_endpoint, success_params)
    assert data_main_success == success_data

    # Simulate second call in __main__-like logic (expecting HTTPError after retries)
    with pytest.raises(requests.exceptions.HTTPError) as excinfo_fail:
        fetch_data_from_pncp(fail_endpoint, fail_params)
    assert excinfo_fail.value.response.status_code == 404

    # Check that typer.echo was called by the alert_slack_on_retry_failure for the second case
    # This is an indirect way to check the callback was hit during the failing call.
    found_failure_message_in_echo = False
    for call_args_tuple in mock_typer_echo_main.call_args_list:
        # call_args_tuple can be call(args, kwargs) or just call(args)
        # We are interested in the first positional argument of any call to typer.echo
        # Ensure there's at least one positional argument.
        if call_args_tuple[0]: # Check if there are positional arguments
            message = call_args_tuple[0][0]
            if isinstance(message, str) and "PNCP request failed ultimately" in message and "404" in message:
                found_failure_message_in_echo = True
                break
    assert found_failure_message_in_echo, "Expected failure message from alert_slack_on_retry_failure (via typer.echo) not found"
    httpserver.reset()


@pytest.fixture(autouse=True)
def mock_time_sleep(mocker):
    return mocker.patch("time.sleep", return_value=None)

def test_retry_speed_with_mock_sleep(httpserver: HTTPServer, mock_time_sleep):
    endpoint_path = "/v1/test/fast_retry"

    for _ in range(7):
        httpserver.expect_request(endpoint_path).respond_with_data("Error", status=500)

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_data_from_pncp(endpoint_path, params={})

    assert len(httpserver.log) == 7
    mock_time_sleep.assert_called()
    assert mock_time_sleep.call_count >= 6
    assert mock_time_sleep.call_count == (fetch_data_from_pncp.retry.stop.max_attempt_number -1)

@pytest.fixture(autouse=True)
def patch_base_url(httpserver: HTTPServer, mocker):
    test_server_url = httpserver.url_for("/")
    if test_server_url.endswith('/'):
        test_server_url = test_server_url[:-1]
    return mocker.patch("baliza.client.BASE_URL", test_server_url)

def test_base_url_is_patched(httpserver: HTTPServer):
    endpoint_path = "/v1/test/base_url_check"
    httpserver.expect_request(endpoint_path).respond_with_json({"status": "ok"})

    fetch_data_from_pncp(endpoint_path, {})

    assert len(httpserver.log) == 1
    logged_request_tuple = httpserver.log[0]
    actual_request_object = logged_request_tuple[0]
    assert actual_request_object.path == endpoint_path # Changed to .path
    pass
