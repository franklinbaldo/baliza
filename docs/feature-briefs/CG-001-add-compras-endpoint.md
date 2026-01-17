# Feature Brief: CG-001 - Add `compras` endpoint support

**Goal Mapping:**
- **Masterplan Goal:** 2. Comprehensive Endpoint Coverage
- **Epic:** 3. Expanded Endpoint Coverage

**User Story:**
As a data analyst, I want to extract procurement data (`compras`) from the PNCP, in addition to contracts data, so that I can analyze the full procurement lifecycle.

**Acceptance Criteria (BDD):**
```gherkin
Feature: Compras Data Extraction
  As a data engineer
  I want to extract data from the PNCP API's 'compras' endpoint
  So that I can have procurement data available in a local database

  Scenario: Extracting 'compras' data for a given period
    Given a clean DuckDB database
    When I run the baliza extract command for the 'compras' resource for a specific date range
    Then the 'compras' data should be extracted and saved to the DuckDB database
```

**Data Contracts:**

**Assumption:** The `compras` endpoint behaves similarly to the `contratos` endpoint. The Implementer Agent must verify and adjust these assumptions.

- **Endpoint Path (ASSUMPTION):** `/v1/compras`
- **Primary Key (ASSUMPTION):** `numeroControlePNCP` (or a similar unique identifier for procurements)
- **Incremental Date Column (ASSUMPTION):** `dataAtualizacao`
- **Date Parameters (ASSUMPTION):** `dataInicial` and `dataFinal` in `AAAAMMDD` format.

**Observability:**
- The `baliza state` commands should work for the `compras` resource.
- Extraction runs, successes, and failures for `compras` must be logged in the state database.

**Out-of-Scope:**
- This feature only covers the addition of the `compras` endpoint. Other endpoints like `licitacoes` are not included.
- No changes to the `export` command are required at this stage.

**Risk Notes:**
- The exact API endpoint path and data schema for `compras` are unverified due to inaccessible documentation. The implementer must dedicate time to discover the correct parameters and response structure.
- The primary key and incremental date column are assumptions. If they are incorrect, the core extraction logic may need significant adjustments.

**Checklist for Implementer Agent:**
1.  **Discover API Contract:**
    - [ ] Determine the correct API path for `compras`. It might be `/v1/compras`, `/v1/contratacoes`, or something else.
    - [ ] Identify the primary key field in the `compras` data.
    - [ ] Identify the date field suitable for incremental extraction.
2.  **Update Configuration:**
    - [ ] Add a new resource entry for `compras` in `src/baliza/config/pncp.yml`, mirroring the `contratos` configuration but with the correct details for `compras`.
3.  **Implement Step Definitions:**
    - [ ] Create `tests/step_defs/test_compras_extraction.py`.
    - [ ] Implement the steps defined in `tests/features/compras_extraction.feature`.
4.  **Run Tests:**
    - [ ] Run the new feature test (`uv run pytest tests/features/compras_extraction.feature`). This will likely require recording a new VCR cassette.
    - [ ] Run the full test suite (`uv run pytest`) to ensure no regressions.
5.  **Update Documentation:**
    - [ ] Add `compras` to the list of available resources in `README.md`.
    - [ ] Update any other relevant documentation.
