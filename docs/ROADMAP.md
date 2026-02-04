# 🛣️ Baliza CLI Roadmap

This roadmap describes the planned evolution of the **Baliza CLI** — the command-line tool for PNCP data extraction. The goal is to maintain exclusive focus on reliable extraction, transformation, and export of data.

**⚠️ IMPORTANT:** This repository contains **only the CLI**. Visualization features, web interface, dashboards, and interactive queries are part of the `baliza-site` project (separate repository). See `docs/ARCHITECTURE.md` to understand the separation of responsibilities.

## Current State (Q1 2026)

- ✅ **Coverage:** The public endpoint `GET /v1/contratos` is fully supported.
- ✅ **Execution:** Commands `baliza extract` and `baliza state` operate on a direct `httpx` + `DuckDB` pipeline.
- ✅ **Resilience:** Automatic checkpointing allows resuming interrupted extractions per-page.
- ✅ **Testing:** BDD-based test suite with Tier hierarchy and Smoke tests.

## Immediate Priorities

1. **Enhanced State Management**
   - Consolidate the window manifest (`totalPaginas`, hashes, status) as the single source of truth for coverage auditing.
   - Implement `baliza state history` to show previous extraction runs and their outcomes.
2. **Observability and Monitoring**
   - Add structured logging with page counts, total time, and total records processed.
   - Improve error reporting for common PNCP API failure modes.
3. **Quality**
   - Expand unit tests for critical utilities and CLI error cases.
   - Resolve quarantined E2E tests by stabilizing the test environment.

## Backlog (Future Vision - CLI only)

The following initiatives remain as inspiration once the core of the project is stable. They are **not in active development**:

### CLI Scope (This Repository)
- ⏳ Support for all other 11 public PNCP endpoints (compras, licitacoes, etc.)
- ⏳ Improved Parquet export (compression, custom schemas)
- ⏳ Automated release publishing with data artifacts
- ⏳ Technical documentation generated with MkDocs
- ⏳ Docker container distribution
- ⏳ BI tool integrations (connectors)
- ⏳ Data validation with Pydantic schemas
- ⏳ Execution metrics and telemetry

### Out of Scope for CLI (Moved to `baliza-site`)
- ❌ Web visualization interface
- ❌ Interactive dashboards
- ❌ Web search and filters
- ❌ Charts and graphs
- ❌ REST/GraphQL query API
- ❌ Authentication system
- ❌ Frontend in React/Vue/etc.

Contributions are welcome, especially on immediate priorities. Please open an issue before starting backlog items to align on scope.

To contribute to visualization features, please wait for the `baliza-site` repository.
