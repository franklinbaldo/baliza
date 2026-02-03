# Feature Brief: Endpoint Generalization & Orgaos Support

-   **ID:** 004
-   **Epic:** Expanded Endpoint Coverage
-   **Goal Mapping:** Comprehensive Endpoint Coverage, Data Preservation
-   **Status:** Ready for Implementation

## 1. User Story

As a data researcher, I want Baliza to support more than just the `contratos` endpoint, starting with `orgaos`, so that I can have a more complete picture of the procurement landscape and properly link contracts to their respective entities.

## 2. Acceptance Criteria (BDD-Style)

### Scenario: Extract orgaos data
- **Given** I want to extract organization data for a specific date
- **When** I run `baliza extract --resource orgaos --start 2024-01-01 --end 2024-01-01`
- **Then** the `PNCPExtractor` should correctly call the `/v1/orgaos` endpoint (or relevant variant)
- **And** it should store the data in a dedicated `orgaos` table in DuckDB
- **And** it should handle the specific schema for organizations.

## 3. Implementation Notes

-   **Refactoring `PNCPExtractor`:**
    -   Move the hardcoded `contratos` schema and insertion logic to a more flexible, resource-mapped configuration.
    -   Consider using a mapping of resource names to Arrow schemas.
-   **Resource Mapping:**
    -   `contratos` -> `https://pncp.gov.br/api/consulta/v1/contratos`
    -   `orgaos` -> (Check exact endpoint in `docs/pncp_endpoints_analysis.md`)
-   **Data Storage:** Each resource should have its own table in the `{dataset}` schema (e.g., `baliza_raw.orgaos`, `baliza_raw.contratos`).

## 4. Risks & Assumptions

-   **Schema Variety:** Different PNCP endpoints have wildly different JSON structures. The generalization must be robust enough to handle this (possibly via Pydantic models or dynamic Arrow schemas).
-   **API Inconsistency:** Some endpoints might use different parameter names for dates.

## 5. Out-of-Scope

-   Implementing ALL 10 endpoints in one go. The goal of this brief is the *refactoring for generalization* and adding the *first* new endpoint (`orgaos`).
