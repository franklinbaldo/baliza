import csv
import datetime
import os

STATS_CSV = os.path.join("baliza_data", "run_statistics.csv")
TEMPLATE_FILE = os.path.join("docs", "stats_template.html")
OUTPUT_FILE = os.path.join("docs", "index.html")


def generate_stats_page():
    print("Generating statistics page...")

    if not os.path.exists(STATS_CSV):
        print(
            f"Error: Statistics CSV not found at {STATS_CSV}. Run collect_stats.py first."
        )
        return

    if not os.path.exists(TEMPLATE_FILE):
        print(f"Error: HTML template not found at {TEMPLATE_FILE}.")
        return

    with open(STATS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        stats_data = list(reader)

    with open(TEMPLATE_FILE, encoding="utf-8") as f:
        template = f.read()

    stats_rows = []
    total_runs = 0
    successful_runs = 0
    failed_runs = 0
    uploads_skipped = 0

    # Process data for HTML table and summary
    processed_run_dates = set()
    for row in stats_data:
        # Collect overall run stats only once per run_date
        if row["run_date"] not in processed_run_dates:
            total_runs += 1
            if row["overall_status"] == "success":
                successful_runs += 1
            elif row["overall_status"] == "failed":
                failed_runs += 1
            elif row["overall_status"] == "completed_upload_skipped":
                uploads_skipped += 1
            processed_run_dates.add(row["run_date"])

        # Prepare table rows
        status_class = ""
        if row["endpoint_status"] == "success":
            status_class = "status-success"
        elif (
            row["endpoint_status"] == "failed"
            or row["endpoint_status"] == "upload_failed"
        ):
            status_class = "status-failed"
        elif (
            row["endpoint_status"] == "upload_skipped"
            or row["endpoint_status"] == "no_data"
        ):
            status_class = "status-skipped"

        stats_rows.append(f"""
                <tr>
                    <td>{row["run_date"]}</td>
                    <td>{row["target_data_date"]}</td>
                    <td>{row["overall_status"]}</td>
                    <td>{row["endpoint"]}</td>
                    <td class="{status_class}">{row["endpoint_status"]}</td>
                    <td>{row["records_fetched"]}</td>
                    <td>{row["upload_status"]}</td>
                    <td>{row["ia_identifier"]}</td>
                    <td><a href="{row["ia_item_url"]}" target="_blank">Link</a></td>
                    <td>{row["sha256_checksum"]}</td>
                </tr>
        """)

    # Replace placeholders in the template
    generated_html = template.replace(
        "{{ last_updated }}", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    )
    generated_html = generated_html.replace("{{ stats_rows }}", "\n".join(stats_rows))
    generated_html = generated_html.replace("{{ total_runs }}", str(total_runs))
    generated_html = generated_html.replace(
        "{{ successful_runs }}", str(successful_runs)
    )
    generated_html = generated_html.replace("{{ failed_runs }}", str(failed_runs))
    generated_html = generated_html.replace(
        "{{ uploads_skipped }}", str(uploads_skipped)
    )

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(generated_html)

    print(f"Statistics page generated at {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_stats_page()
