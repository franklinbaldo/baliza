# DLT Migration Analysis & Implementation Status

**Status**: Migração para DLT concluída com sucesso ✅

**Implementação Atual**: Pipeline DLT totalmente funcional com `rest_api_source`, detecção de lacunas, deduplicação por hash e suporte a todos os 12 endpoints PNCP.

**Date**: July 25, 2025  
**Status**: Migration in progress  
**Current State**: WIP with legacy preservation structure

## Current State Analysis

### What's Already Done ✅

1. **Legacy preservation structure** - Code moved to `src/baliza/legacy/`
   - Models, enums, utils successfully preserved
   - Import paths updated in existing code
   - Hash utilities extracted and working

2. **DLT skeleton** - Basic pipeline structure created
   - `src/baliza/pipelines/pncp.py` with dlt source definition
   - Three resources implemented: contratacoes_publicacao, contratos, atas
   - Hash-based deduplication pattern in place

3. **Test infrastructure** - E2E test structure exists
   - `tests/e2e/test_pncp_pipeline.py` with comprehensive mocks
   - All tests currently skipped due to known issues

4. **Configuration** - ENDPOINT_CONFIG preserved and working
   - All 11 PNCP endpoints properly configured
   - Legacy enums (PncpEndpoint, ModalidadeContratacao) working

### Critical Blocker Identified 🚨

**Prefect Dependency Issue**: The legacy `EndpointExtractor` requires Prefect context (`get_run_logger()`) which is not available in dlt context. This breaks the "legacy preservation" strategy.

**Impact**: Cannot reuse existing HTTP client as-is without major modifications.

## Root Cause Analysis

The problem is architectural mismatch:
- **Legacy code**: Designed for Prefect flows with context-dependent logging
- **DLT code**: Different execution context, no Prefect dependency

The original "preservation" plan assumed we could wrap legacy components without modification, but this is not feasible due to runtime context dependencies.

## DLT Built-in Capabilities Assessment

### Key Findings ✅

**Research conducted**: July 25, 2025

1. **REST API Source**: `dlt.sources.rest_api.rest_api_source()` 
   - Handles all HTTP operations automatically
   - Built-in authentication, retries, rate limiting
   - Configurable via `RESTAPIConfig` 

2. **Pagination Support**: Multiple built-in paginators
   - `PageNumberPaginator` (matches PNCP's `pagina` parameter)
   - `OffsetPaginator` (for endpoints using offset/limit)
   - `JSONResponseCursorPaginator` (for cursor-based pagination)

3. **Incremental Loading**: `dlt.sources.incremental()`
   - Built-in support for date-based incremental sync
   - Perfect match for PNCP's `dataInicial`/`dataFinal` pattern

4. **Processing Steps**: Custom data transformation pipeline
   - Can apply legacy hash functions for deduplication
   - No need for custom HTTP client or pagination logic

### Impact on Migration Strategy

**MAJOR SIMPLIFICATION**: ~80% of custom code can be eliminated by using dlt built-ins!

- ❌ No custom HTTP client needed
- ❌ No custom pagination logic
- ❌ No custom retry/rate limiting
- ✅ Pure configuration-driven approach

## Revised Migration Strategy

### Strategy Pivot: "Configuration Over Code"

Instead of preserving all legacy code, we preserve only the **data definitions** and **business logic**, while rewriting the **execution layer**.

#### Keep (Zero Changes)
- ✅ `legacy/enums.py` - All enums including PncpEndpoint, ModalidadeContratacao
- ✅ `legacy/models.py` - Pydantic models for validation
- ✅ `legacy/utils/hash_utils.py` - Hash functions for deduplication
- ✅ `legacy/sql/` - SQL schemas and cleanup scripts
- ✅ `config.py` - ENDPOINT_CONFIG dictionary

#### Rewrite (DLT-Native)
- 🔄 HTTP client layer - Use dlt's built-in REST API source
- 🔄 Circuit breaker - Use dlt's built-in retry mechanisms
- 🔄 Rate limiting - Use dlt's built-in rate limiting
- 🔄 Pagination logic - Use dlt's pagination helpers

#### Remove (Technical Debt)
- ❌ Prefect flows (`flows/` directory)
- ❌ Prefect-dependent utilities
- ❌ Complex circuit breaker (over-engineered for dlt)

## Implementation Plan (Revised)

### Phase 1: Configure DLT REST API Source (Week 1)

**BREAKTHROUGH**: DLT has comprehensive REST API built-ins that eliminate need for custom HTTP client!

Research findings:
- ✅ `rest_api_source()` function handles HTTP requests automatically
- ✅ Built-in pagination support: `PageNumberPaginator`, `OffsetPaginator`, etc.
- ✅ Built-in authentication, retries, rate limiting
- ✅ `EndpointResource` configuration matches our needs perfectly

Create `src/baliza/pipelines/pncp_config.py`:
- Convert `ENDPOINT_CONFIG` to dlt `RESTAPIConfig` format
- Map PNCP pagination patterns to dlt paginators
- Configure authentication and rate limiting
- Leverage legacy enums for parameter validation

**Success Criteria**: All 11 endpoints configured as dlt REST API resources

### Phase 2: Replace Custom Pipeline (Week 1)

Replace `src/baliza/pipelines/pncp.py` with dlt REST API source:
- Use `rest_api_source(config)` instead of custom `@dlt.source`
- Configure incremental loading with `dlt.sources.incremental()`  
- Apply legacy hash functions for deduplication in processing steps
- Remove all custom HTTP client code

**Success Criteria**: Zero custom HTTP code, everything uses dlt built-ins

### Phase 3: E2E Testing (Week 2)

Fix and expand `tests/e2e/test_pncp_pipeline.py`:
- Remove skipped tests
- Test against real API endpoints (not mocks)
- Validate hash-based deduplication
- Performance benchmarks vs legacy system

**Success Criteria**: All tests passing, performance >= legacy system

### Phase 4: CLI Integration (Week 2)

Update `src/baliza/cli.py`:
- Replace Prefect flow calls with dlt pipeline runs
- Maintain existing CLI interface for backward compatibility
- Add feature flag for legacy vs dlt mode

**Success Criteria**: `baliza run --latest` works with dlt backend

### Phase 5: Shadow Mode (Week 3)

Implement parallel validation:
- Run dlt pipeline alongside legacy (feature flag)
- Compare row counts and hash checksums
- Log divergences for analysis

**Success Criteria**: 99.9% data consistency between legacy and dlt

### Phase 6: Cutover (Week 4)

Remove legacy Prefect flows:
- Delete `flows/` directory
- Remove Prefect dependencies from pyproject.toml
- Update documentation

**Success Criteria**: Legacy code removed, dlt is the only ETL engine

## Technical Decisions

### HTTP Client Design

**Decision**: Use dlt built-in HTTP capabilities instead of custom client

**Rationale**:
- dlt has built-in HTTP sources and REST API helpers
- dlt handles retries, rate limiting, and error handling automatically
- dlt's pagination helpers eliminate custom logic
- Avoids reinventing HTTP client functionality

**Research Needed**: Investigate dlt's REST API source and HTTP helpers

**Implementation Strategy**:
```python
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig
from baliza.legacy.enums import PncpEndpoint, ModalidadeContratacao

# Convert ENDPOINT_CONFIG to RESTAPIConfig
config = RESTAPIConfig(
    client={"base_url": "https://pncp.gov.br/api/consulta"},
    resources=[
        {
            "name": "contratacoes_publicacao",
            "endpoint": "/v1/contratacoes/publicacao",
            "paginator": "page_number",  # dlt built-in
            # Use legacy enums for validation
        }
    ]
)

# Zero custom HTTP code needed!
source = rest_api_source(config)
```

### Enum Reuse Strategy

**Decision**: Import legacy enums directly in dlt code

**Rationale**:
- Enums contain validated business logic (enum values match API)
- No execution context dependencies
- Zero risk of breaking existing validation rules

**Implementation**:
```python
from baliza.legacy.enums import PncpEndpoint, ModalidadeContratacao
# Use directly in dlt resources
```

### Deduplication Strategy

**Decision**: Use legacy hash_sha256 function

**Rationale**:
- Function is pure (no side effects)
- Already handles JSON serialization edge cases
- Maintains compatibility with existing data

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| API schema changes | Medium | High | Real endpoint testing, schema validation |
| Performance regression | Low | Medium | Benchmark tests, parallel validation |
| Data consistency issues | Low | High | Shadow mode with hash comparison |
| Missing edge cases | Medium | Medium | Comprehensive test suite with real data |

## Success Metrics

### Performance Targets
- **Extraction time**: <= 30 minutes per month (legacy baseline: ~70 min)
- **Memory usage**: <= 4GB peak (DuckDB + concurrent requests)
- **Data consistency**: 99.9% hash match with legacy system

### Quality Targets
- **Test coverage**: 90%+ for new HTTP client and dlt resources
- **E2E tests**: All endpoints tested with real API calls
- **Documentation**: Complete migration guide and new architecture docs

## Migration Timeline

```
Week 1: HTTP Client + DLT Resources (July 25-31)
Week 2: Testing + CLI Integration (Aug 1-7)
Week 3: Shadow Mode + Validation (Aug 8-14)
Week 4: Cutover + Cleanup (Aug 15-21)
```

## Dependencies & Prerequisites

### External Dependencies
- ✅ dlt >= 1.14.1 (already added)
- ✅ httpx (for HTTP client)
- ✅ DuckDB (destination)

### Internal Dependencies
- ✅ Legacy enums and models preserved
- ✅ ENDPOINT_CONFIG available
- ✅ Hash utilities working

## Rollback Plan

If migration fails:
1. **Immediate**: Use feature flag to switch back to legacy Prefect flows
2. **Short-term**: Keep legacy code in separate branch until dlt is stable
3. **Long-term**: Gradual endpoint migration (1-2 endpoints at a time)

The "legacy preservation" structure enables safe rollback at any point.

## Next Actions

1. **Immediate**: Fix Prefect logging dependency in test environment
2. **Today**: Implement SimpleHTTPClient without Prefect dependencies
3. **This week**: Complete dlt resources for all 11 endpoints
4. **Next week**: Enable real API testing

---

**Conclusion**: The revised plan abandons the "preserve everything" approach in favor of "preserve data definitions, rewrite execution layer". This is more realistic given the Prefect context dependencies and will result in cleaner, more maintainable code.