
# src/baliza/fetcher.py

def fetch_data_for_endpoint(date: str, config: dict):
    """Fetches data for a given endpoint configuration."""
    print(f"Fetching data for {config.get('output_name')} for date {date}")
    # Placeholder for actual data fetching logic
    return [{"id": 1, "date": date, "value": 100}]
