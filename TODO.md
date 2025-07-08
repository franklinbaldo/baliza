# BALIZA - TODO List

This file tracks the development tasks to make the BALIZA project fully functional, based on its `README.md`.

## Phase 1: Core Functionality (Getting to a stable, testable state)

- [x] **Test PNCP API Connectivity and Data Retrieval**: Performed real tests with the API and obtained consistent data.
- [x] **Create Tests**:
    - [x] Test the PNCP API client, mocking the HTTP requests.
    - [x] Test the data processing and compression logic.
    - [x] Test the Internet Archive upload functionality, mocking the `internetarchive` library.
- [x] **Implement Checksumming**:
    - [x] Implement SHA256 checksum calculation for the generated `.jsonl.zst` files.
    - [x] Create a mechanism to store and check these checksums (e.g., in a `state/processed.csv` file) to prevent duplicate uploads.
- [x] **Refine Configuration**:
    - [ ] Ensure the script gracefully handles missing `IA_KEY` and `IA_SECRET` environment variables, providing clear instructions to the user.

## Phase 2: Statistics and Expansion (Future goals)

- [x] **Implement Statistics Collection**:
    - [x] Develop a script to aggregate data from daily runs.
- [x] **Implement Additional Endpoints**:
    - [x] Add support for the `/v1/contratos/publicacao` endpoint.
    - [x] Add support for the `/v1/pca` endpoint.
- [x] **Build Statistics Page**:
    - [x] Create an HTML template for the status page.
    - [x] Develop a script to generate the static HTML page.
    - [x] Set up GitHub Pages to host the page.
