# baliza Roadmap

## 🎯 Project Overview
**baliza** is the foundational CLI tool for the Baliza Ecosystem, dedicated to the **reliable extraction, storage, and export** of Brazilian public procurement data from the PNCP (Portal Nacional de Contratações Públicas).

Its primary purpose is to serve as the **"Bronze Layer" ingestion engine**: fetching raw JSON data, securing it in a local DuckDB instance, and producing optimized Parquet datasets for downstream consumption (analysis, visualization, and archiving).

## 📊 Current State Summary

### **Strengths**
- **Robust Pipeline**: Direct implementation using `httpx` + `duckdb` + `pyarrow` provides high performance and fine-grained control over error handling.
- **Resilience**: Built-in checkpointing allows resuming interrupted extractions at the page level.
- **Performance**: Use of Arrow tables for bulk insertion into DuckDB is ~250x faster than standard row insertion.
- **Security**: Explicit SSRF protection (`validate_url`), identifier validation, and parameterized queries prevent common vulnerabilities.
- **Architecture**: Clear separation of concerns defined in `ARCHITECTURE.md` (CLI vs. Visualization Site).

### **Weaknesses**
- **Synchronous Extraction**: The current `httpx` implementation fetches pages sequentially, which limits throughput for massive backfills.
- **Visualization Coupling**: The legacy dashboard exists within this repo (`docs/dashboard`), conflicting with the new `baliza-site` strategy.
- **Resource Limits**: Primary focus is currently on `contratos`, with limited support for other PNCP resources like `atas` or `termos`.

### **Technical Debt**
- **Linting Violations**: Several `ruff` rule violations exist (imports outside top-level, unused variables, too many arguments).
- **Hardcoded Fallbacks**: The "slow path" insertion fallback in `extractor.py` is necessary but adds maintenance complexity.
- **Test Granularity**: High reliance on integration tests; unit test coverage for individual extractor components could be improved.

### **Test Coverage**
- **Integration**: Strong coverage for SSRF protection and basic API interaction (via VCR cassettes).
- **Unit**: Moderate coverage. Critical paths are tested, but edge cases in the extractor (e.g., schema evolution handling) need more attention.

---

## 🚀 Roadmap (Prioritized)

### Phase 1: Critical Fixes & Stability (Next 2 weeks)
Focus on cleaning up technical debt and ensuring the codebase meets strict quality standards before feature expansion.

- [ ] **Fix Linting & Formatting Violations (P1)**
  - **Why**: `ruff` rules `I001` (imports), `F841` (unused vars), and `PLC0415` (imports scope) are currently violated.
  - **Impact**: Clean, standard-compliant code; unblocks CI strict checks.
  - **Effort**: S
  - **Dependencies**: None

- [ ] **Refactor `_save_checkpoint` Signature (P1)**
  - **Why**: Violates `PLR0913` (too many arguments).
  - **Impact**: Improved readability and maintainability of state management.
  - **Effort**: S
  - **Dependencies**: None

- [ ] **Verify & Harden SSRF Protection (P0)**
  - **Why**: Security is paramount for a tool fetching data from user-definable URLs.
  - **Impact**: Confirmed security posture against network attacks.
  - **Effort**: S
  - **Dependencies**: Existing tests

- [ ] **Establish `CHANGELOG.md`**
  - **Why**: Track release history and breaking changes.
  - **Impact**: Better developer experience and release transparency.
  - **Effort**: S
  - **Dependencies**: None

### Phase 2: Core Architecture Alignment (Next month)
Execute the separation strategy defined in `ARCHITECTURE.md` to focus this repo purely on data engineering.

- [ ] **Decouple Dashboard**
  - **Why**: Move visualization logic to the new `baliza-site` repository.
  - **Impact**: Clearer separation of concerns; lighter repository.
  - **Effort**: M
  - **Dependencies**: Creation of `baliza-site` repo

- [ ] **Enhance `export-daily` Metadata**
  - **Why**: The consumption layer (`baliza-site`) needs rich metadata (manifests, checksums) to automate updates.
  - **Impact**: Reliable automated ingestion for the frontend.
  - **Effort**: M
  - **Dependencies**: None

- [ ] **Implement `baliza state` Management**
  - **Why**: Centralize coverage tracking and gap detection logic (currently dispersed).
  - **Impact**: More accurate `baliza verify` and `status` reporting.
  - **Effort**: M
  - **Dependencies**: Phase 1

### Phase 3: Performance & Scale (Next quarter)
Optimize the engine for handling years of historical data efficiently.

- [ ] **Async / Parallel Extraction**
  - **Why**: Sequential fetching is too slow for multi-year backfills.
  - **Impact**: 5-10x speedup in data ingestion.
  - **Effort**: L
  - **Dependencies**: Phase 2

- [ ] **Support Additional Resources**
  - **Why**: PNCP contains more than just contracts (`atas-registro-preco`, `termos-contrato`).
  - **Impact**: Complete coverage of public procurement data.
  - **Effort**: L
  - **Dependencies**: None

- [ ] **Schema Evolution Strategy**
  - **Why**: PNCP API fields may change; need a robust way to handle schema drift in Parquet exports.
  - **Impact**: Long-term data stability.
  - **Effort**: M
  - **Dependencies**: None

### Phase 4: Future Vision (6+ months)
Operational maturity and ecosystem integration.

- [ ] **Official Docker Image**
  - **Why**: Simplify deployment in containerized environments (K8s, ECS).
  - **Impact**: "Run anywhere" capability.
  - **Effort**: M
  - **Dependencies**: None

- [ ] **Orchestration Recipes**
  - **Why**: Provide standard DAGs for Airflow/Dagster/Prefect.
  - **Impact**: Enterprise-ready integration patterns.
  - **Effort**: M
  - **Dependencies**: Docker Image

---

## 💡 Quick Wins
- **Remove Unused Imports**: Run `ruff check --fix` to immediately clean up noise.
- **Update CLI Help**: Ensure all CLI flags have clear descriptions in `--help` output.
- **Add Git Pre-commit Hook**: Enforce `ruff` checks locally before commit.

## 🔧 Technical Improvements Backlog
- **Type Safety**: Increase `mypy` strictness level for core modules.
- **Error Reporting**: Integrate `sentry-sdk` (optional opt-in) for crash reporting.
- **Dependency Audit**: Review pinned versions in `pyproject.toml` for updates.

## 📝 Documentation Needs
- **Developer Guide**: How to add a new resource extractor.
- **Schema Reference**: Document the mapping between PNCP JSON and internal Arrow schema.
- **Deployment Guide**: Best practices for running `baliza` in cron/CI.

## 🧪 Testing Gaps
- **Schema Mismatch Unit Tests**: Test how the extractor behaves when API returns unexpected data types (without hitting real API).
- **Network Timeout Simulation**: More granular tests for retry logic during unstable network conditions.
