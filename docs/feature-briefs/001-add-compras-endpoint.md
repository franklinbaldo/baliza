# Feature Brief: Add `compras` Endpoint Extraction

- **Title:** Add Support for `compras` (Procurements) Endpoint
- **Goal Mapping:**
  - **Concrete Goal 2:** Comprehensive Endpoint Coverage
  - **Epic 3:** Expanded Endpoint Coverage
- **User Story:** As a data analyst, I want to extract procurement data (`compras`) from the PNCP, in addition to contracts, so that I can analyze the entire procurement lifecycle.

---

## Acceptance Criteria (BDD)

```gherkin
Feature: Extract `compras` data from PNCP API

  Scenario: Run `extract` command for the `compras` resource
    Given the `compras` resource is configured
    When I run the command `baliza extract --resource compras`
    Then the `baliza.duckdb` file should contain a `compras` table in the `baliza_raw` schema
    And the table should contain records from the `compras` endpoint

  Scenario: Run `backfill` command for the `compras` resource
    Given the `compras` resource is configured
    When I run the command `baliza backfill 2024-01 2024-01 --resource compras`
    Then the `compras` table in `baliza.duckdb` should contain records for January 2024

  Scenario: Run `export` command for the `compras` resource
    Given the `compras` table exists in `baliza.duckdb`
    When I run the command `baliza export --table compras --out data/compras`
    Then Parquet files for the `compras` data should be created in the `data/compras` directory
```

---

## Data Contracts

### PNCP `compras` Schema

The `compras` resource corresponds to the PNCP `/v1/contratacoes/publicacao` endpoint. The data schema includes, but is not limited to:

```typescript
{
  "numeroControlePNCP": string,       // Primary Key (e.g., "00000000000000-1-000001/2024")
  "anoCompra": number,
  "dataPublicacaoPncp": string,       // (e.g., "2024-01-15T10:30:00")
  "dataAtualizacao": string,
  "objetoCompra": string,
  "valorEstimado": number,
  "modalidadeId": number,
  "modalidadeNome": string,
  "modoDisputaId": number,
  "modoDisputaNome": string,
  "situacaoCompraId": number,
  "situacaoCompraNome": string,
  "orgaoEntidade": {
    "cnpj": string,
    "razaoSocial": string,
    "poderId": number,
    "poderNome": string,
    "esferaId": number,
    "esferaNome": string
  },
  "unidadeOrgao": {
    "codigoUnidade": string,
    "nomeUnidade": string
  }
}
```
*Note: This is a representative schema. The actual implementation should handle all fields returned by the API.*

---

## Observability

- The `baliza state` commands (`show`, `gaps`, `history`) must work for the `compras` resource.
- Extraction runs, successes, and failures for `compras` must be logged in the `baliza_state.extraction_runs` table.
- Coverage data for `compras` must be tracked in the `baliza_state.cobertura` and `baliza_state.janelas` tables.

---

## Out-of-Scope

- This feature does not include adding support for other endpoints like `atas` or `licitacoes`.
- It does not include modifying the `baliza-site` frontend.
- No changes to the underlying `dlt` pipeline runner are expected, only configuration changes and schema definitions.

---

## Risk Notes & Constraints

- **Required `codigoModalidadeContratacao` Parameter:** The `/v1/contratacoes/publicacao` endpoint requires a `codigoModalidadeContratacao` parameter. To extract all data for a given date range, the extractor **must iterate through all 13 known modality codes** (from 1 to 13). A single request without this parameter will fail. This is a key difference from the `contratos` endpoint.
- **Schema Differences:** The schema for `compras` is different from `contratos`. The implementation must define a new `dlt` table schema for it.
- **Configuration:** The `pncp.yml` configuration will need to be refactored to support multiple, distinct endpoint configurations under a single resource name.
