import os
import subprocess
import shutil

# Define paths
SCRIPT_DIR = os.path.dirname(__file__)
BALIZA_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
OPENAPI_SPEC_PATH = os.path.join(BALIZA_ROOT, "docs", "openapi", "pncp_openapi.json")
GENERATED_CLIENT_DIR = os.path.join(BALIZA_ROOT, "src", "baliza", "pncp_client")
FETCH_SPEC_SCRIPT = os.path.join(SCRIPT_DIR, "fetch_openapi_spec.py")

def generate_client():
    """Fetches the latest OpenAPI spec and generates the Python client."""
    print("--- Starting client generation process ---")

    # Step 1: Fetch the latest OpenAPI spec
    print(f"Running {FETCH_SPEC_SCRIPT} to get the latest OpenAPI spec...")
    try:
        subprocess.run(["uv", "run", "python", FETCH_SPEC_SCRIPT], check=True, cwd=BALIZA_ROOT)
        print("OpenAPI spec fetched successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error fetching OpenAPI spec: {e}")
        return

    # Step 2: Clean up old generated client directory
    if os.path.exists(GENERATED_CLIENT_DIR):
        print(f"Removing existing client directory: {GENERATED_CLIENT_DIR}")
        shutil.rmtree(GENERATED_CLIENT_DIR)
    os.makedirs(GENERATED_CLIENT_DIR, exist_ok=True)

    # Step 3: Generate new client using openapi-python-client
    print(f"Generating client code into {GENERATED_CLIENT_DIR} from {OPENAPI_SPEC_PATH}...")
    try:
        # openapi-python-client generate --path <spec_path> --output-path <output_path> --overwrite
        subprocess.run([
            "uv", "run", "openapi-python-client", "generate",
            "--path", OPENAPI_SPEC_PATH,
            "--output-path", GENERATED_CLIENT_DIR,
            "--overwrite" # Add --overwrite option
        ], check=True, cwd=BALIZA_ROOT)
        print("Client code generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error generating client code: {e}")
        return

    # Step 4: Create an __init__.py in the generated client directory
    # This makes it a Python package
    init_file = os.path.join(GENERATED_CLIENT_DIR, "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "w") as f:
            f.write("# This file makes pncp_client a Python package.\n")
        print(f"Created {init_file}")

    print("--- Client generation process completed ---")

if __name__ == "__main__":
    generate_client()