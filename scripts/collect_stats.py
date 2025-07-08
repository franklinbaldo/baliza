import os
import json
import datetime
import csv

LOGS_DIR = os.path.join("baliza_run_logs")
OUTPUT_FILE = os.path.join("baliza_data", "run_statistics.csv")

def collect_statistics():
    print(f"Collecting statistics from {LOGS_DIR}...")
    all_stats = []

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    for filename in os.listdir(LOGS_DIR):
        if filename.startswith("run_summary_") and filename.endswith(".json"):
            filepath = os.path.join(LOGS_DIR, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    summary = json.load(f)
                    
                    # Extract relevant data
                    run_date = summary.get("run_date_iso")
                    target_data_date = summary.get("target_data_date")
                    overall_status = summary.get("overall_status")

                    # Aggregate endpoint data
                    for endpoint_key, endpoint_data in summary.get("endpoints", {}).items():
                        stats_entry = {
                            "run_date": run_date,
                            "target_data_date": target_data_date,
                            "overall_status": overall_status,
                            "endpoint": endpoint_key,
                            "endpoint_status": endpoint_data.get("status"),
                            "records_fetched": endpoint_data.get("records_fetched", 0),
                            "upload_status": "N/A",
                            "ia_identifier": "N/A",
                            "ia_item_url": "N/A",
                            "sha256_checksum": "N/A",
                        }
                        
                        # Get details from the first file generated (assuming one per endpoint for now)
                        if endpoint_data.get("files_generated"):
                            first_file = endpoint_data["files_generated"][0]
                            stats_entry["upload_status"] = first_file.get("upload_status", "N/A")
                            stats_entry["ia_identifier"] = first_file.get("ia_identifier", "N/A")
                            stats_entry["ia_item_url"] = first_file.get("ia_item_url", "N/A")
                            stats_entry["sha256_checksum"] = first_file.get("sha256_checksum", "N/A")

                        all_stats.append(stats_entry)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from {filepath}: {e}")
            except Exception as e:
                print(f"An unexpected error occurred while processing {filepath}: {e}")

    if not all_stats:
        print("No run summary files found or processed.")
        return

    # Write to CSV
    fieldnames = all_stats[0].keys()
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_stats)
    print(f"Statistics collected and saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    collect_statistics()
