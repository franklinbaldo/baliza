import pytest
from unittest.mock import patch, MagicMock, call
import time
import requests # For requests.exceptions used in simulated_exception

from baliza import pipeline # Assuming client.py is in the same package as pipeline.py
from tenacity import RetryError as TenacityRetryError # For test_harvest_fetch_raises_retryerror_from_client
from baliza.pipeline import harvest_endpoint_data

# Mock for baliza.client.fetch_data_from_pncp
# This will be used by most tests in this file.
@pytest.fixture
def mock_fetch_data():
    with patch("baliza.pipeline.client.fetch_data_from_pncp") as mock_fetch:
        yield mock_fetch

# Mock for time.sleep to speed up tests that involve delays
@pytest.fixture(autouse=True) # Apply to all tests in this module
def mock_time_sleep(mocker):
    return mocker.patch("time.sleep", return_value=None)


# Test basic successful harvest with multiple pages
def test_harvest_success_multiple_pages(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-01"
    endpoint_key = "contratacoes"
    endpoint_cfg = {"api_path": "/v1/contratacoes/publicacao", "page_size": 2} # Small page size for test

    # Page 1 response data
    page1_response_data = {
        "items": [{"id": 1, "data": "item1"}, {"id": 2, "data": "item2"}],
        "totalPaginas": 2,
        "paginaAtual": 1
    }
    # Page 2 response data
    page2_response_data = {
        "items": [{"id": 3, "data": "item3"}],
        "totalPaginas": 2,
        "paginaAtual": 2
    }

    # Custom side_effect function to inspect calls and manage responses
    # Use a list to ensure it's mutable within the side_effect_func
    call_tracker = {"count": 0, "received_params": []}
    responses = [page1_response_data, page2_response_data]

    def side_effect_func(endpoint, params):
        print(f"SIDE_EFFECT_FUNC received call {call_tracker['count']+1} for endpoint '{endpoint}' with params: {params}")
        call_tracker["received_params"].append(params.copy()) # Store a copy of params
        response = responses[call_tracker["count"]]
        call_tracker["count"] += 1
        return response

    mock_fetch_data.side_effect = side_effect_func

    expected_records = [
        {"id": 1, "data": "item1"},
        {"id": 2, "data": "item2"},
        {"id": 3, "data": "item3"}
    ]

    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert records == expected_records
    assert mock_fetch_data.call_count == 2 # Same as call_tracker["count"]
    assert call_tracker["count"] == 2

    # Check params for each call using the tracked params
    # Call 1
    params_call_1 = call_tracker["received_params"][0]
    assert params_call_1["dataInicial"] == day_iso
    assert params_call_1["dataFinal"] == day_iso
    assert params_call_1["pagina"] == 1
    assert params_call_1["tamanhoPagina"] == 2

    # Call 2
    params_call_2 = call_tracker["received_params"][1]
    assert params_call_2["dataInicial"] == day_iso
    assert params_call_2["dataFinal"] == day_iso
    assert params_call_2["pagina"] == 2
    assert params_call_2["tamanhoPagina"] == 2

    # Check that time.sleep(1) was called between page fetches
    # Since there are 2 pages, there should be 1 sleep call after the first page.
    assert mock_time_sleep.call_count == 1
    mock_time_sleep.assert_called_with(1)


def test_harvest_no_data_first_page(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-02"
    endpoint_key = "empty_contratacoes"
    endpoint_cfg = {"api_path": "/v1/empty/publicacao"}

    # API returns no items on the first page
    mock_fetch_data.return_value = {"items": [], "totalPaginas": 0}

    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert records == []
    mock_fetch_data.assert_called_once_with(
        "/v1/empty/publicacao",
        {"dataInicial": day_iso, "dataFinal": day_iso, "pagina": 1, "tamanhoPagina": 500} # Default page size
    )
    mock_time_sleep.assert_not_called() # No further pages, so no sleep

def test_harvest_no_items_key_first_page(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-03"
    endpoint_key = "malformed_contratacoes"
    endpoint_cfg = {"api_path": "/v1/malformed/publicacao"}

    # API returns a response without the "items" key
    mock_fetch_data.return_value = {"totalPaginas": 0, "message": "No items field"}

    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert records == [] # Should handle gracefully and return empty list
    mock_fetch_data.assert_called_once()
    mock_time_sleep.assert_not_called()

def test_harvest_empty_response_first_page(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-04"
    endpoint_key = "empty_response_contratacoes"
    endpoint_cfg = {"api_path": "/v1/empty_response/publicacao"}

    # API returns None or empty dict
    mock_fetch_data.return_value = None # Or {}

    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert records == [] # Should handle gracefully
    mock_fetch_data.assert_called_once()
    mock_time_sleep.assert_not_called()


def test_harvest_fetch_raises_retryerror(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-05"
    endpoint_key = "failing_contratacoes"
    endpoint_cfg = {"api_path": "/v1/failing/publicacao"}

    # Simulate that client.fetch_data_from_pncp raises an exception (e.g., one re-raised by its own retry logic)
    # This will be caught by the generic `except Exception as e:` in harvest_endpoint_data
    simulated_exception = requests.exceptions.ConnectionError("Simulated connection error from client")
    mock_fetch_data.side_effect = simulated_exception

    with patch("baliza.pipeline._log_error") as mock_log_error:
        records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

        assert records is None # harvest_endpoint_data should return None
        mock_fetch_data.assert_called_once()
        mock_time_sleep.assert_not_called()
        # Check that the generic exception was logged
        mock_log_error.assert_called_once()
        assert "An unexpected error occurred" in mock_log_error.call_args[0][0]
        assert str(simulated_exception) in mock_log_error.call_args[0][0]


def test_harvest_fetch_raises_retryerror_from_client(mock_fetch_data, mock_time_sleep):
    # This test specifically checks how pipeline handles a RetryError propagated from client
    # (if client's retry_error_callback was None or didn't re-raise, which is not current client setup)
    # For this, we need a RetryError with a last_attempt.
    from tenacity import RetryError as TenacityRetryError # Alias to avoid confusion with pipeline.RetryError if any
    from unittest.mock import Mock

    class MockAttempt:
        def __init__(self, exc):
            self._exception = exc
        def exception(self):
            return self._exception

    mock_last_attempt = MockAttempt(requests.exceptions.ReadTimeout("Client timeout on last attempt"))
    simulated_retry_error = TenacityRetryError(last_attempt=mock_last_attempt)

    day_iso = "2023-01-06" # Different date
    endpoint_key = "retry_error_contratacoes"
    endpoint_cfg = {"api_path": "/v1/retry_error/publicacao"}
    mock_fetch_data.side_effect = simulated_retry_error

    with patch("baliza.pipeline._log_error") as mock_log_error_retry:
        records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
        assert records is None
        mock_fetch_data.assert_called_once()
        mock_log_error_retry.assert_called_once()
        # Now this log should correctly access e.last_attempt.exception()
        assert "Failed to fetch data" in mock_log_error_retry.call_args[0][0]
        assert "Client timeout on last attempt" in mock_log_error_retry.call_args[0][0]


def test_harvest_fetch_raises_unexpected_exception(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-06" # Original test, keep for basic non-RequestException
    endpoint_key = "exception_contratacoes"
    endpoint_cfg = {"api_path": "/v1/exception/publicacao"}

    mock_fetch_data.side_effect = ValueError("Simulated unexpected client error")

    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert records is None # Should return None on any exception from client
    mock_fetch_data.assert_called_once()
    mock_time_sleep.assert_not_called()

def test_harvest_invalid_total_pages_in_response(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-07"
    endpoint_key = "invalid_totalpages"
    endpoint_cfg = {"api_path": "/v1/invalid_totalpages/publicacao", "page_size": 1}

    # API returns items but totalPaginas is missing or invalid
    mock_fetch_data.side_effect = [
        {"items": [{"id": 1}], "totalPaginas": None}, # Missing totalPaginas
        {"items": [{"id": 2}], "totalPaginas": 0},    # Invalid totalPaginas (0)
        {"items": [{"id": 3}], "totalPaginas": -1},   # Invalid totalPaginas (negative)
        {"items": [{"id": 4}]},                       # No totalPaginas field
    ]

    # Test case 1: totalPaginas is None
    records1 = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
    assert records1 == [{"id": 1}] # Should fetch the first page and then stop
    assert mock_fetch_data.call_count == 1
    mock_time_sleep.assert_not_called() # Stops after first page
    mock_fetch_data.reset_mock() # Reset for next case
    mock_time_sleep.reset_mock()

    # Test case 2: totalPaginas is 0
    records2 = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
    assert records2 == [{"id": 2}]
    assert mock_fetch_data.call_count == 1
    mock_time_sleep.assert_not_called()
    mock_fetch_data.reset_mock()
    mock_time_sleep.reset_mock()

    # Test case 3: totalPaginas is -1
    records3 = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
    assert records3 == [{"id": 3}]
    assert mock_fetch_data.call_count == 1
    mock_time_sleep.assert_not_called()
    mock_fetch_data.reset_mock()
    mock_time_sleep.reset_mock()

    # Test case 4: totalPaginas field is completely missing
    records4 = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
    assert records4 == [{"id": 4}]
    assert mock_fetch_data.call_count == 1
    mock_time_sleep.assert_not_called()


def test_harvest_single_page_api_response(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-08"
    endpoint_key = "single_page_contratacoes"
    endpoint_cfg = {"api_path": "/v1/single_page/publicacao", "page_size": 5}

    mock_fetch_data.return_value = {
        "items": [{"id": 1}, {"id": 2}, {"id": 3}],
        "totalPaginas": 1 # API indicates only one page
    }

    expected_records = [{"id": 1}, {"id": 2}, {"id": 3}]
    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert records == expected_records
    mock_fetch_data.assert_called_once_with(
        "/v1/single_page/publicacao",
        {"dataInicial": day_iso, "dataFinal": day_iso, "pagina": 1, "tamanhoPagina": 5}
    )
    mock_time_sleep.assert_not_called() # Only one page, no sleep needed


def test_harvest_stops_if_page_greater_than_totalpages(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-09"
    endpoint_key = "contratacoes_page_gt_total"
    endpoint_cfg = {"api_path": "/v1/contratacoes_page_gt_total/publicacao", "page_size": 1}

    mock_fetch_data.side_effect = [
        {"items": [{"id": 1}], "totalPaginas": 3},
        {"items": [{"id": 2}], "totalPaginas": 3},
        # On 3rd call, pagina will be 3. If totalPaginas is 3, it should fetch and then stop.
        {"items": [{"id": 3}], "totalPaginas": 3},
        # This call should not happen if logic is correct
        # {"items": [{"id": 4}], "totalPaginas": 3}
    ]

    expected_records = [{"id": 1}, {"id": 2}, {"id": 3}]
    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
    assert records == expected_records
    assert mock_fetch_data.call_count == 3
    # Sleeps after page 1 and page 2 fetches
    assert mock_time_sleep.call_count == 2


# Test logging calls (basic check, could be more specific with log content)
@patch("baliza.pipeline._log_info") # Patching the internal _log_info
@patch("baliza.pipeline._log_error") # Patching the internal _log_error
def test_harvest_logging(mock_log_error, mock_log_info, mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-10"
    endpoint_key = "logging_test"
    endpoint_cfg = {"api_path": "/v1/logging/publicacao", "page_size": 1}

    # Scenario 1: Successful run with one page
    mock_fetch_data.return_value = {"items": [{"id": "log1"}], "totalPaginas": 1}
    harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    assert mock_log_info.call_count >= 3 # Start harvest, fetching page, page/total, harvested total
    # Example:
    # INFO: Starting harvest for 'logging_test' on 2023-01-10...
    # INFO: Fetching page 1 for 'logging_test'...
    # INFO: Page 1/1 for 'logging_test' fetched 1 items. Total collected so far: 1.
    # INFO: Reached last page (1) for 'logging_test'.
    # INFO: Harvested a total of 1 records for 'logging_test' on 2023-01-10.
    # So, at least 5 info calls.
    # Actual calls:
    # 1. Starting harvest...
    # 2. Fetching page 1...
    # 3. Page 1/1 for 'logging_test' fetched...
    # 4. Reached last page (1)...
    # 5. Harvested a total of 1 records...
    assert mock_log_info.call_count == 5
    mock_log_error.assert_not_called()

    mock_log_info.reset_mock()
    mock_log_error.reset_mock()
    mock_fetch_data.reset_mock()

    # Scenario 2: Fetch error (client raises a non-RetryError exception directly)
    simulated_exception_for_log = ValueError("Simulated ValueError for logging")
    mock_fetch_data.side_effect = simulated_exception_for_log
    harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    # Calls: Starting harvest, Fetching page 1
    assert mock_log_info.call_count == 2 # Reset from previous scenario
    # Error log for the generic Exception
    mock_log_error.assert_called_once()
    assert "An unexpected error occurred" in mock_log_error.call_args[0][0]
    assert str(simulated_exception_for_log) in mock_log_error.call_args[0][0]


# Test with custom page_size from endpoint_cfg
def test_harvest_uses_custom_page_size(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-11"
    endpoint_key = "custom_pagesize"
    custom_page_size = 50
    endpoint_cfg = {"api_path": "/v1/custom_pagesize/publicacao", "page_size": custom_page_size}

    mock_fetch_data.return_value = {"items": [], "totalPaginas": 0} # Simple response
    harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)

    mock_fetch_data.assert_called_once_with(
        "/v1/custom_pagesize/publicacao",
        {"dataInicial": day_iso, "dataFinal": day_iso, "pagina": 1, "tamanhoPagina": custom_page_size}
    )

# Test the direct __main__ block of pipeline.py (if it had one)
# pipeline.py's __main__ block is for illustrative/testing and makes live calls.
# We should test its behavior if it's meant to be a usable script.
# However, the current __main__ in pipeline.py uses typer.echo and makes live calls.
# It's better to test functions directly or test the CLI interface (test_cli.py).
# For now, we will not add a specific test for pipeline.py's __main__ block,
# as its primary purpose seems to be ad-hoc testing of harvest_endpoint_data with live calls.
# If it were intended as a script entry point, it would need refactoring for testability
# (e.g., dependency injection for ENDPOINTS_CONFIG_TEST, client calls).

# Example of testing the placeholder logging functions directly (if they were more complex)
# def test_log_info_placeholder(capsys):
#     pipeline._log_info("Test info message")
#     captured = capsys.readouterr()
#     assert "Test info message" in captured.out

# def test_log_error_placeholder(capsys):
#     pipeline._log_error("Test error message")
#     captured = capsys.readouterr()
#     assert "Test error message" in captured.err

# These are trivial as _log_info and _log_error currently just use typer.echo.
# Once proper logging is implemented, tests for log output format/content would be valuable.
# For now, patching them (as in test_harvest_logging) is sufficient to check if they are called.
# (Note: pipeline.py currently uses typer.echo directly in its __main__, but _log_info/_log_error in functions)
# The _log_info and _log_error in pipeline.py use typer.echo.
# We can patch `baliza.pipeline.typer.echo` to check these.

@patch("baliza.pipeline.typer.echo")
def test_internal_log_functions_call_typer_echo(mock_typer_echo):
    pipeline._log_info("Info test from _log_info")
    mock_typer_echo.assert_called_with("Info test from _log_info")

    pipeline._log_error("Error test from _log_error")
    mock_typer_echo.assert_called_with("Error test from _log_error", err=True)

# Test case: API returns items, but totalPaginas suggests fewer pages than items would imply
# (e.g., 10 items, page_size=5, but totalPaginas=1).
# The current logic respects totalPaginas.
def test_harvest_totalpages_limits_fetching(mock_fetch_data, mock_time_sleep):
    day_iso = "2023-01-12"
    endpoint_key = "totalpages_limit"
    endpoint_cfg = {"api_path": "/v1/tp_limit/publicacao", "page_size": 2}

    # API returns 2 items on page 1, but says totalPaginas is 1.
    mock_fetch_data.return_value = {
        "items": [{"id": 1}, {"id": 2}], # More items than one page if totalPaginas was higher
        "totalPaginas": 1
    }

    records = harvest_endpoint_data(day_iso, endpoint_key, endpoint_cfg)
    assert records == [{"id": 1}, {"id": 2}]
    mock_fetch_data.assert_called_once() # Fetches only page 1
    mock_time_sleep.assert_not_called() # No second page, so no sleep
