import json
import os

import requests

# URL of the OpenAPI specification
OPENAPI_URL = "https://pncp.gov.br/pncp-consulta/v3/api-docs"  # Corrected URL

# Path to store the downloaded spec
OUTPUT_DIR = os.path.join("docs", "openapi")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "pncp_openapi.json")


def fetch_and_save_spec():
    """Fetches the OpenAPI spec and saves it locally."""
    print(f"Fetching OpenAPI spec from {OPENAPI_URL}...")
    try:
        response = requests.get(OPENAPI_URL, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        spec_content = response.json()  # Assuming the content is JSON
        print("Successfully fetched the OpenAPI spec.")

        # Ensure the output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"Ensured directory {OUTPUT_DIR} exists.")

        # Save the spec content to the file
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(spec_content, f, ensure_ascii=False, indent=2)
        print(f"OpenAPI spec saved to {OUTPUT_FILE}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching OpenAPI spec: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from response: {e}")
    except OSError as e:
        print(f"Error saving OpenAPI spec to file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    fetch_and_save_spec()
