# ADR-011: Official PNCP Schema Compliance

## Status
Accepted

## Context
BALIZA's database schema was initially designed based on empirical observation of PNCP API responses. However, the government provides official documentation that defines precise schemas, data types, and domain tables:
- `MANUAL-PNCP-CONSULSTAS-VERSAO-1.md` - Official manual with 17 domain tables
- `api-pncp-consulta.json` - OpenAPI schema with exact field types and constraints

Compliance with official schema ensures:
- Data integrity and validation
- Future compatibility with API changes
- Optimal storage through proper typing
- Regulatory compliance

## Decision
We will redesign BALIZA's database schema to be 100% compliant with official PNCP documentation.

**Key Compliance Requirements:**
1. **Official ENUMs** - Use exact codes from 17 domain tables (Modalidade, Situação, UF, etc.)
2. **Precise data types** - Match OpenAPI specifications exactly
3. **Required vs Optional fields** - Respect official field requirements
4. **Official hierarchies** - Follow Órgão → Unidade → Contratação → Contratos structure
5. **Validation constraints** - Implement official business rules

**Domain Tables to Implement:**
- Modalidade de Contratação (13 values: 1=Leilão Eletrônico, 6=Pregão Eletrônico, etc.)
- Situação da Contratação (4 values: 1=Divulgada, 2=Revogada, 3=Anulada, 4=Suspensa)
- UF (27 official state codes)
- Natureza Jurídica (47 specific codes)
- And 13 additional domain tables

**Breaking Changes:**
- This will break compatibility with existing database schema
- Requires data migration for existing records
- May require API client updates

## Consequences

### Positive
- 100% compliance with government standards
- Future-proof against API changes
- Better data validation and integrity
- Optimal storage through proper ENUMs
- Regulatory compliance assurance
- Enables compression optimizations (ENUMs vs VARCHAR)

### Negative
- Breaking change requires migration effort
- Existing queries and code must be updated
- May require downtime during migration
- Legacy data may need transformation

## Implementation Strategy
1. **Legacy branch** - Create `legacy-reader` branch with views to new schema
2. **Progressive migration** - Migrate data in batches to minimize downtime
3. **Validation tools** - Build validators against official schema
4. **Automated enum updates** - Handle new domain values automatically

## References
- Database Optimization Plan Section 1.1: "Auditoria da Base Atual vs Schema Oficial"
- `docs/openapi/MANUAL-PNCP-CONSULSTAS-VERSAO-1.md`
- `docs/openapi/api-pncp-consulta.json`
- ADR-010: Compression Heuristics (ENUMs enable better compression)