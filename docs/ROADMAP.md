# 🛣️ Baliza CLI Roadmap

This roadmap describes the planned evolution of the **Baliza CLI** — the command-line tool for PNCP data extraction. The goal is to maintain a sharp focus on reliable extraction, transformation, and export of data.

**⚠️ IMPORTANT:** This repository contains **only the CLI**. Visualization features, web interface, dashboards, and interactive queries are part of the `baliza-site` project (separate repository). See `docs/ARCHITECTURE.md` to understand the separation of concerns.

## Current State (Q1 2026)

- ✅ **Coverage:** Only the public `GET /v1/contratos` endpoint is supported.
- ✅ **Execution:** `baliza extract` and `baliza export` commands are functional and use a direct `httpx` + `DuckDB` pipeline.
- ⚠️ **Known Limitations:**
  - The `PNCPExtractor` is currently hardcoded for the `contratos` resource.
  - The CLI is missing `state` subcommands (`show`, `gaps`, `history`) and `backfill`.
  - BDD test coverage for CLI commands is partially mocked.

## Immediate Priorities (Tier 1)

1. **CLI Refactoring & Observability**
   - Implement the `state` command group with `show`, `gaps`, and `history` subcommands.
   - Implement the `backfill` command for easy historical data extraction.
   - Ensure BDD tests for these commands use real CLI invocations instead of mocks.

2. **Endpoint Generalization**
   - Refactor `PNCPExtractor` to support multiple endpoints beyond `contratos`.
   - Add support for the `orgaos` endpoint as the first secondary resource.

3. **Resilience & Monitoring**
   - Improve structured logging for better extraction tracking.
   - Refine the `verify` command to handle more complex gap scenarios.

## Backlog (Future Vision - CLI only)

The following initiatives are for when the core project is stable. They are **not in active development**:

### CLI Scope (This Repository)
- ✅ Support for all 10+ PNCP public endpoints
- ✅ Automated release publishing with data artifacts
- ✅ MkDocs generated technical documentation
- ✅ Docker container distribution
- ✅ BI tool connectors
- ✅ Additional export formats (CSV, JSON)
- ✅ Pydantic schema validation
- ✅ Execution metrics and telemetry

### Out of Scope (Moves to `baliza-site`)
- ❌ Web visualization interface
- ❌ Interactive dashboards
- ❌ Web-based search and filters
- ❌ Charts and graphs
- ❌ REST/GraphQL Query API
- ❌ Authentication system
- ❌ React/Vue/etc. Frontend

Contributions are welcome, especially for Tier 1 priorities. Please open an issue before starting work on backlog items to align on scope.
