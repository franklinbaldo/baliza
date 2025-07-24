# Architectural Decision Records (ADRs)

This directory contains the architectural decision records for the BALIZA project. ADRs are short documents that capture significant architectural decisions, along with their context and consequences.

## Current ADRs

1. [**ADR-001: Adopt DuckDB as the Primary Database**](001-adopt-duckdb.md)
   - Decision to use DuckDB for analytical workloads and data distribution

2. [**ADR-002: Resilient and Task-Driven Extraction Architecture**](002-resilient-extraction.md)
   - Four-phase extraction process with fault tolerance and progress tracking

3. [**ADR-003: Adopt dbt and the Medallion Architecture (Bronze/Silver/Gold)**](003-medallion-architecture.md)
   - Data transformation framework using Medallion layers (implementation via Ibis)

4. [**ADR-004: End-to-End (E2E) Only Testing Strategy**](004-e2e-testing.md)
   - E2E-first testing approach with no unit tests or mocks

5. [**ADR-005: Adopt a Modern, High-Performance Python Toolchain**](005-modern-python-toolchain.md)
   - Modern Python development tools (uv, ruff, httpx, typer)

6. [**ADR-006: Publish Data to the Internet Archive**](006-internet-archive.md)
   - Open data publishing strategy using Internet Archive

7. [**ADR-007: Implement an MCP Server for AI-Powered Analysis**](007-mcp-server.md)
   - AI-powered data analysis using Model Context Protocol

8. [**ADR-013: dbt Integration for DDL Management**](013-dbt-integration-for-ddl.md) ❌ **SUPERSEDED**
   - dbt integration (abandoned due to complexity)

9. [**ADR-014: Adopt Ibis Pipeline for Data Transformation**](014-ibis-pipeline-adoption.md) ✅ **ACTIVE**
   - Modern Python-native data transformation replacing dbt

## ADR Summary

These architectural decisions form the foundation of BALIZA's design philosophy:

- **DuckDB-centric** data architecture for portability and performance
- **Resilient, task-driven** extraction with fault tolerance
- **Medallion architecture** implemented via Ibis pipeline (dbt abandoned)
- **E2E-only testing** strategy for maximum confidence
- **Modern Python toolchain** for developer experience and performance
- **Internet Archive** for open data publishing
- **MCP server** for AI-powered data access

Each decision balances technical requirements with the project's mission of creating an accessible, open backup of Brazilian public procurement data.