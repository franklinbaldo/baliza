# Baliza BDD Feature Set - Comprehensive Plan

## Purpose

This document outlines a comprehensive BDD feature set for the Baliza CLI project that:
1. **Documents** all existing functionality as living documentation
2. **Guides** future development with clear acceptance criteria
3. **Aligns** with project goals (reliability, preservation, accessibility)
4. **Supports** the MASTERPLAN's epics and roadmap

## Current State Analysis

### Existing Features (4 files)

| Feature File | Scenarios | Coverage | Issues |
|-------------|-----------|----------|--------|
| `extraction.feature` | 1 basic scenario | 10% | Missing incremental extraction, lookback, error handling, security |
| `export.feature` | 1 basic scenario | 15% | Missing date filtering, partitioning, IA upload, error handling |
| `state_management.feature` | 3 good scenarios | 60% | Missing gap detection details, window merging, edge cases |
| `verification.feature` | 2 basic scenarios | 20% | Missing gap reasons, statistics, suspect detection |

### Missing Features

- **backfill.feature** - Deterministic historical processing
- **resilience.feature** - API failures, retries, timeouts, network issues
- **data_quality.feature** - Data validation, integrity checks, deduplication
- **configuration.feature** - Pipeline configuration, resource settings
- **observability.feature** - Logging, metrics, progress reporting

## Comprehensive Feature Set Structure

### Epic 1: Resumable Extraction Pipeline ✅ (Mostly Complete)

#### 1.1 extraction.feature (EXPAND)
**Goal:** Reliable, incremental data extraction from PNCP API

**New Scenarios:**
- ✅ Basic extraction (exists)
- **NEW:** Incremental extraction with configurable lookback
- **NEW:** Extraction respects 10MB response size limit
- **NEW:** Extraction handles pagination correctly (500 items/page)
- **NEW:** Extraction deduplicates using numeroControlePNCP
- **NEW:** Multiple small date windows processed efficiently
- **NEW:** URL validation prevents injection attacks
- **NEW:** Query parameters are redacted in error logs

#### 1.2 state_management.feature (ENHANCE)
**Goal:** Full resumability and gap tracking

**Current:** 3 scenarios covering basics

**New Scenarios:**
- ✅ First-time extraction (exists)
- ✅ Resuming interrupted extraction (exists)
- ✅ State CLI commands (exists)
- **NEW:** Gap detection identifies incomplete windows
- **NEW:** Gap detection identifies suspect windows
- **NEW:** Gap detection applies lookback period
- **NEW:** Adjacent gaps are merged for efficiency
- **NEW:** Run history tracks timing and row counts
- **NEW:** Coverage manifesto stores page hashes
- **NEW:** State persists after each successful window

#### 1.3 backfill.feature (NEW)
**Goal:** Deterministic historical data consolidation

**Scenarios:**
- Backfill processes full months without state reuse
- Backfill for multiple months processes sequentially
- Backfill validates month format (YYYY-MM)
- Backfill creates separate pipeline instance
- Backfill updates coverage manifesto
- Backfill handles overlapping data with merge strategy

### Epic 2: Data Quality & Verification

#### 2.1 verification.feature (ENHANCE)
**Goal:** Audit coverage and detect data issues

**Current:** 2 basic scenarios

**New Scenarios:**
- ✅ Complete period verification (exists)
- ✅ Gap detection (exists)
- **NEW:** Verification shows gap reasons (incomplete, suspect, missing, lookback)
- **NEW:** Verification displays coverage statistics (% complete)
- **NEW:** Verification compares local vs API page counts
- **NEW:** Verification detects suspect windows (page count mismatches)
- **NEW:** Verification uses manifesto hashes for integrity
- **NEW:** Verification outputs human-readable summary
- **NEW:** Verification filters by date range

#### 2.2 data_quality.feature (NEW)
**Goal:** Ensure data integrity and validity

**Scenarios:**
- Primary keys are unique (numeroControlePNCP)
- Merge strategy prevents duplicates
- Date fields are properly formatted (AAAAMMDD)
- Required fields are never null
- Data types match schema expectations
- Hash digests match stored values
- Incremental cursor (dataAtualizacao) works correctly
- Page totals match between runs

### Epic 3: Data Export & Publishing

#### 3.1 export.feature (EXPAND)
**Goal:** Export data to accessible formats

**Current:** 1 basic scenario

**New Scenarios:**
- ✅ Basic Parquet export (exists)
- **NEW:** Export filters by date range
- **NEW:** Export partitions by year and month
- **NEW:** Export to specific output directory
- **NEW:** Export specific tables (contratos, etc.)
- **NEW:** Export uploads to Internet Archive with identifier
- **NEW:** Export validates data before writing
- **NEW:** Export shows progress for large datasets
- **NEW:** Export handles empty tables gracefully
- **NEW:** Export fails safely on disk space issues

### Epic 4: Pipeline Resilience

#### 4.1 resilience.feature (NEW)
**Goal:** Graceful handling of failures and edge cases

**Scenarios:**
- Extraction retries on transient network errors
- Extraction respects API rate limits
- Extraction handles timeout configuration
- Extraction records failures in state
- Extraction continues after partial window failure
- Empty API responses are handled correctly
- Malformed JSON responses are logged and skipped
- Database connection errors trigger retry
- State file corruption is detected and reported
- Concurrent executions are prevented with locking

### Epic 5: Configuration & Observability

#### 5.1 configuration.feature (NEW)
**Goal:** Flexible configuration for different scenarios

**Scenarios:**
- Pipeline uses declarative YAML config (pncp.yml)
- Custom DuckDB path can be specified
- Resource name can be changed (contratos, etc.)
- Pipeline name can be customized
- Lookback days can be configured
- Pagination size respects API limits (max 500)
- Base URL can be overridden for testing
- Dataset name can be specified

#### 5.2 observability.feature (NEW)
**Goal:** Clear visibility into pipeline operations

**Scenarios:**
- Extract command shows progress bar for windows
- Extract command logs pages processed per window
- Extract command reports total rows extracted
- Extract command shows timing statistics
- State show displays human-readable timestamps
- State show uses card layout for readability
- State history shows run status and metrics
- Verification output includes summary panel
- Error messages include actionable guidance
- Debug mode enables verbose logging

## Implementation Strategy

### Phase 1: Enhance Core Features (Week 1)
1. Expand `extraction.feature` with security and incremental scenarios
2. Enhance `state_management.feature` with gap detection details
3. Enhance `verification.feature` with statistics and reasons
4. Expand `export.feature` with all export options

### Phase 2: Add Missing Features (Week 2)
5. Create `backfill.feature` with historical processing
6. Create `resilience.feature` with error handling
7. Create `data_quality.feature` with validation rules

### Phase 3: Complete Coverage (Week 3)
8. Create `configuration.feature` for settings testing
9. Create `observability.feature` for UX testing
10. Update `feature_goal_matrix.md` with complete mapping

## Feature-Goal Alignment

| Feature File | Primary Goal | Epic |
|-------------|--------------|------|
| `extraction.feature` | Reliable Data Extraction | Epic 1 |
| `state_management.feature` | Resumability | Epic 1 |
| `backfill.feature` | Reliable Data Extraction | Epic 1 |
| `verification.feature` | Data Quality Monitoring | Epic 4 |
| `data_quality.feature` | Data Quality Monitoring | Epic 4 |
| `export.feature` | Data Accessibility | Epic 3 |
| `resilience.feature` | Reliable Data Extraction | Epic 1 |
| `configuration.feature` | Developer Experience | Epic 1 |
| `observability.feature` | Operator Experience | Epic 1 |

## Success Criteria

A comprehensive BDD feature set should:
- ✅ Cover all CLI commands (`extract`, `backfill`, `export`, `verify`, `state`)
- ✅ Document all major features in the codebase
- ✅ Include positive and negative test scenarios
- ✅ Align with project goals and epics
- ✅ Guide future development decisions
- ✅ Serve as living documentation for users and contributors
- ✅ Enable automated testing and CI/CD

## Benefits

1. **Documentation:** Features serve as executable specification
2. **Regression Prevention:** Automated tests catch breaking changes
3. **Design Guide:** Features guide implementation decisions
4. **Onboarding:** New contributors understand functionality through scenarios
5. **User Stories:** Features capture user needs and expectations
6. **Quality Assurance:** Comprehensive coverage ensures reliability
