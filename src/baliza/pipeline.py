# This module will orchestrate the data processing flow.
# It will contain the logic for paginating through API results,
# calling the client to fetch data, and then passing the data to the storage module.

import time
import typer # For logging, will be replaced by logging module later
from tenacity import RetryError

from . import client # Assuming client.py is in the same package

# Placeholder for actual logging
# Will be replaced by Python's logging module configured for JSON output
def _log_info(message):
    typer.echo(message)

def _log_error(message):
    typer.echo(message, err=True)


def harvest_endpoint_data(day_iso: str, endpoint_key: str, endpoint_cfg: dict):
    """
    Harvests all data for a given day and endpoint from PNCP.
    Manages pagination and calls the client to fetch data.
    Includes a 1-second delay between page fetches.

    Args:
        day_iso: The target date in YYYY-MM-DD format.
        endpoint_key: Configuration key for the endpoint (e.g., "contratacoes").
        endpoint_cfg: Configuration dictionary for the endpoint, must contain "api_path".

    Returns:
        A list of all records fetched for the day, or None if a persistent error occurs.
    """
    all_records = []
    current_page = 1
    # tamanhoPagina can be up to 500 for PNCP.
    # Using a smaller number for testing if needed, but 500 is good for production.
    page_size = endpoint_cfg.get("page_size", 500)
    params = {"dataInicial": day_iso, "dataFinal": day_iso, "pagina": current_page, "tamanhoPagina": page_size}

    _log_info(f"Starting harvest for '{endpoint_key}' on {day_iso} from API path '{endpoint_cfg['api_path']}'...")

    while True:
        _log_info(f"Fetching page {current_page} for '{endpoint_key}' (date: {day_iso}, size: {page_size})...")
        try:
            # Call the client module to fetch data
            data = client.fetch_data_from_pncp(endpoint_cfg["api_path"], params)
        except RetryError as e:
            _log_error(f"Failed to fetch data for {endpoint_key} (page {current_page}) after multiple retries: {e.last_attempt.exception()}")
            # Depending on desired robustness, could continue to next page or abort for the day.
            # For now, aborting for this endpoint for this day if any page fetch ultimately fails.
            return None
        except Exception as e:
            _log_error(f"An unexpected error occurred fetching page {current_page} for '{endpoint_key}': {e}")
            return None # Abort for this endpoint

        if not data or "items" not in data or not data["items"]:
            if current_page == 1 and (not data or "items" not in data):
                 _log_info(f"No items found or malformed response for '{endpoint_key}' on page {current_page} (initial request). Response: {str(data)[:200]}")
            else:
                _log_info(f"No more items found for '{endpoint_key}' on page {current_page}.")
            break # Exit loop if no items or malformed response on first page, or no items on subsequent pages

        all_records.extend(data["items"])

        # PNCP API provides "totalPaginas"
        total_pages = data.get("totalPaginas", 0)
        if not isinstance(total_pages, int) or total_pages <= 0:
            _log_error(f"Warning: 'totalPaginas' not found or invalid in response for {endpoint_key} (page {current_page}). Response: {str(data)[:200]}. Assuming single page or error.")
            # If total_pages is missing or weird, we might be done or there's an API issue.
            # For safety, if items were returned, we break. If no items, the earlier check handles it.
            break


        _log_info(f"Page {current_page}/{total_pages} for '{endpoint_key}' fetched {len(data['items'])} items. Total collected so far: {len(all_records)}.")

        if params["pagina"] >= total_pages:
            _log_info(f"Reached last page ({total_pages}) for '{endpoint_key}'.")
            break

        current_page += 1 # Increment current_page
        params["pagina"] = current_page # Update params for the next iteration's fetch_data_from_pncp call

        # Add a 1-second delay between paged requests as per requirements
        _log_info(f"Waiting 1 second before fetching next page for '{endpoint_key}'...")
        time.sleep(1)


    _log_info(f"Harvested a total of {len(all_records)} records for '{endpoint_key}' on {day_iso}.")
    return all_records

if __name__ == '__main__':
    # Example usage for testing this module directly
    # This requires client.py to be in the same directory or Python path.
    # And it makes live calls to PNCP.
    typer.echo("Testing pipeline.harvest_endpoint_data...")

    # Configuration similar to what would be in main.py or cli.py
    ENDPOINTS_CONFIG_TEST = {
        "contratacoes_test": {
            "api_path": "/v1/contratacoes/publicacao", # Real endpoint
            "file_prefix": "pncp-ctrt", # New naming
            "ia_title_prefix": "PNCP Contratações Teste",
            "page_size": 50 # Smaller page size for faster testing
        }
        # Add a misconfigured endpoint to test error handling
        # "faulty_endpoint": {
        #     "api_path": "/v1/doesnotexist/publicacao",
        #     "file_prefix": "pncp-fault",
        #     "ia_title_prefix": "PNCP Faulty Test",
        # }
    }
    test_date = "2023-01-05" # A date known to have some, but not excessive, data.

    for key, config in ENDPOINTS_CONFIG_TEST.items():
        typer.echo(f"\n--- Testing endpoint: {key} for date {test_date} ---")
        records = harvest_endpoint_data(test_date, key, config)
        if records is not None:
            typer.echo(f"Successfully harvested {len(records)} records for {key}.")
            if records:
                typer.echo(f"First record sample: {str(records[0])[:200]}...")
        else:
            typer.echo(f"Harvest failed for {key}.", err=True)

    # Test with an endpoint designed to fail (if client.py is robust enough)
    # config_fail = {
    #     "api_path": "/v1/nonexistent/path",
    #     "file_prefix": "pncp-fail",
    #     "ia_title_prefix": "PNCP Failure Test"
    # }
    # typer.echo(f"\n--- Testing failing endpoint for date {test_date} ---")
    # records_fail = harvest_endpoint_data(test_date, "failure_test", config_fail)
    # if records_fail is None:
    #     typer.echo("Harvest correctly failed for non-existent endpoint.", color=typer.colors.GREEN)
    # else:
    #     typer.echo(f"Harvest unexpectedly succeeded or returned data for non-existent endpoint: {len(records_fail)} records.", err=True)

    # Test with a date that might have no data
    # test_date_no_data = "2042-01-01" # Future date, likely no data
    # typer.echo(f"\n--- Testing endpoint: contratacoes_test for date {test_date_no_data} (expecting no data) ---")
    # records_no_data = harvest_endpoint_data(test_date_no_data, "contratacoes_test", ENDPOINTS_CONFIG_TEST["contratacoes_test"])
    # if records_no_data is not None and len(records_no_data) == 0:
    #     typer.echo(f"Correctly harvested 0 records for {test_date_no_data}.", color=typer.colors.GREEN)
    # elif records_no_data is not None:
    #     typer.echo(f"Harvested {len(records_no_data)} records for {test_date_no_data}, expected 0.", err=True)
    # else:
    #     typer.echo(f"Harvest failed for {test_date_no_data} when expecting 0 records.", err=True)
