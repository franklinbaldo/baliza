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
    - [x] Ensure the script gracefully handles missing `IA_KEY` and `IA_SECRET` environment variables, providing clear instructions to the user.

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

## Phase 3: Endpoint Expansion (New bulk data endpoints)

- [x] **Implement Collection for New Endpoints**:
    - [x] Add support for `/v1/pca/usuario` (Consultar Itens de PCA por Ano do PCA, IdUsuario e Código de Classificação Superior).
    - [x] Add support for `/v1/pca/` (Consultar Itens de PCA por Ano do PCA e Código de Classificação Superior - general PCA endpoint).
    - [x] Add support for `/v1/contratacoes/proposta` (Consultar Contratações com Recebimento de Propostas Aberto).

## Phase 4: Testing and Documentation (Completed)

- [x] **Test New Endpoints**:
    - [x] Test `/v1/pca/usuario` endpoint with real API calls.
    - [x] Test `/v1/pca/` endpoint with real API calls.
    - [x] Test `/v1/contratacoes/proposta` endpoint with real API calls.
- [x] **Update Documentation**:
    - [x] Update README.md to include new endpoints.
    - [x] Update configuration documentation.
    - [x] Add examples for new endpoint usage.

## Implementation Summary

All Phase 3 endpoints have been successfully implemented and tested:

### New Endpoints Added:
1. **`/v1/pca/usuario`** - PCA items by User ID and Year
   - Status: ✅ Implemented and tested
   - Returns: 204 (No Content) with test parameters (expected behavior)

2. **`/v1/pca/`** - General PCA items by Year and Classification
   - Status: ✅ Implemented and tested  
   - Returns: 200 with actual data (working correctly)

3. **`/v1/contratacoes/proposta`** - Contracts with open proposal reception
   - Status: ✅ Implemented and tested
   - Returns: 200 with actual data (working correctly)

### Key Implementation Details:
- Modified `harvest_endpoint_data()` to handle endpoints without date range parameters
- Added proper date formatting for endpoints that require `yyyyMMdd` format
- Fixed endpoint path for `/v1/pca/` (requires trailing slash)
- Added comprehensive test script `scripts/test_new_endpoints.py`
- Updated documentation in README.md

### Test Results:
- All endpoints are reachable and respond correctly
- API structure matches OpenAPI specification
- Date format handling works properly
- Error handling is appropriate for missing data scenarios