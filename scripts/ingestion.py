
# scripts/ingestion.py
from src.baliza.config import ENDPOINTS_CONFIG # Centralized config
from src.baliza.fetcher import fetch_data_for_endpoint
from src.baliza.loader import save_to_parquet, upload_to_ia

def run_ingestion(date: str):
    """The single entry point for all data ingestion."""
    print(f"Running ingestion for date: {date}")
    for endpoint_name, config in ENDPOINTS_CONFIG.items():
        print(f"Processing endpoint: {endpoint_name}")
        # data = fetch_data_for_endpoint(date, config)
        # if data:
        #     filepath = save_to_parquet(data, endpoint_name, date)
        #     upload_to_ia(filepath, config)

if __name__ == "__main__":
    # This will be called by cli.py, so direct execution here is for testing/dev
    import sys
    if len(sys.argv) > 1:
        run_ingestion(sys.argv[1])
    else:
        print("Usage: python ingestion.py <date>")
