# This module will handle all API calls to PNCP.
# It will include the fetch_data_from_pncp function with improved retry logic.

import requests
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError
import typer # For logging, will be replaced by logging module later
import json # Added for JSONDecodeError

# Base URL for PNCP API
BASE_URL = "https://pncp.gov.br/api/consulta"

def alert_slack_on_retry_failure(retry_state):
    """Placeholder for actual Slack alerting. Will be replaced by proper logging/alerting."""
    # In a real scenario, this would send a message to a Slack channel or other monitoring service.
    print(f"PNCP request failed ultimately after {retry_state.attempt_number} attempts for args: {retry_state.args}, kwargs: {retry_state.kwargs}. Last exception: {retry_state.outcome.exception()}")
    # For now, using typer.echo for visibility, will be replaced by logging.
    typer.echo(f"PNCP request failed ultimately after {retry_state.attempt_number} attempts. Last exception: {retry_state.outcome.exception()}", err=True)
    # Re-raise the last exception to allow callers to handle RetryError
    if retry_state.outcome:
        raise retry_state.outcome.exception()


@retry(
    wait=wait_exponential(multiplier=2, min=5, max=300), # Exponential backoff: 5s, 10s, 20s, 40s, 80s, 160s, 300s
    stop=stop_after_attempt(7),
    retry_error_callback=alert_slack_on_retry_failure # Called if all retries fail
)
def fetch_data_from_pncp(endpoint_path: str, params: dict):
    """
    Fetches data from a PNCP endpoint with robust retries.
    """
    try:
        response = requests.get(f"{BASE_URL}{endpoint_path}", params=params, timeout=30) # Standard 30s timeout
        response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
        return response.json()
    except requests.exceptions.Timeout as e:
        typer.echo(f"Request timed out for {endpoint_path} with params {params}: {e}", err=True)
        raise # Re-raise to trigger tenacity retry
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429: # Too Many Requests
            typer.echo(f"Rate limited (429) for {endpoint_path}, params {params}. Retrying...: {e}", err=True)
        else:
            typer.echo(f"HTTP error for {endpoint_path}, params {params}: {e}", err=True)
        raise # Re-raise to trigger tenacity retry
    except requests.exceptions.RequestException as e:
        typer.echo(f"Generic request failed for {endpoint_path} with params {params}: {e}", err=True)
        raise # Re-raise to trigger tenacity retry
    except json.JSONDecodeError as e:
        typer.echo(f"Failed to decode JSON response from {endpoint_path}, params {params}: {e}", err=True)
        # Consider if this should be retried or handled as a permanent failure
        raise # Re-raise, might indicate server-side issue or unexpected response format

if __name__ == '__main__':
    # Example usage (for testing this module directly)
    # This part would not run when imported by other modules.
    typer.echo("Testing fetch_data_from_pncp...")
    # Test with a known public endpoint if available, or a mock.
    # For PNCP, we need a specific endpoint and valid (even if past) date.
    # Example for /contratacoes/publicacao
    # Note: This is a live test and depends on PNCP availability.
    # In real unit tests, you would mock `requests.get`.
    test_endpoint = "/v1/contratacoes/publicacao"
    # Using a date far in the past to minimize data, or a specific test date
    # For this example, let's assume this call might return empty or some data
    test_params = {"dataInicial": "2023-01-01", "dataFinal": "2023-01-01", "pagina": 1, "tamanhoPagina": 1}
    try:
        data = fetch_data_from_pncp(test_endpoint, test_params)
        if data:
            typer.echo(f"Successfully fetched data (first item if any): {data.get('items', [])[:1]}")
            typer.echo(f"Total items on page: {len(data.get('items', []))}, Total pages: {data.get('totalPaginas')}")
        else:
            typer.echo("No data returned or 'items' key missing.")
    except RetryError as e:
        typer.echo(f"Could not fetch data after multiple retries: {e}", err=True)
    except Exception as e:
        typer.echo(f"An unexpected error occurred during direct test: {e}", err=True)

    # Test case for a known non-existent path to see retry and failure
    typer.echo("\nTesting with a non-existent endpoint (expecting retries and failure)...")
    non_existent_endpoint = "/v1/nonexistent/path"
    try:
        fetch_data_from_pncp(non_existent_endpoint, {"pagina": 1})
    except RetryError as e:
        typer.echo(f"As expected, failed after retries for non-existent path: {e.last_attempt.exception()}", color=typer.colors.YELLOW)
    except Exception as e:
        typer.echo(f"An unexpected error occurred for non-existent path: {e}", err=True)
