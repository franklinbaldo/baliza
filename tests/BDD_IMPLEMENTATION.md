# BDD Implementation Plan

**Purpose:** Organize the implementation of BDD scenarios following the feature tier hierarchy.

**Last Updated:** 2026-01-19

## Overview

We have 105 BDD scenarios across 7 feature files. This document tracks implementation progress organized by tier priority.

## Tier 0: Critical Path Features

**Priority:** P0 - Must be implemented first

### extraction.feature (10 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1 | Basic extraction for a date range | ✅ Implemented | test_extraction.py |
| 2 | Incremental extraction with configurable lookback | ⏳ Partial | Missing steps |
| 3 | Extraction handles pagination correctly | ❌ Not implemented | - |
| 4 | Extraction respects 10MB response size limit | ❌ Not implemented | - |
| 5 | Extraction deduplicates using primary key | ❌ Not implemented | - |
| 6 | Multiple small date windows processed efficiently | ❌ Not implemented | - |
| 7 | URL validation prevents injection attacks | ❌ Not implemented | - |
| 8 | Query parameters are redacted in error logs | ❌ Not implemented | - |
| 9 | Extraction creates run record with metadata | ❌ Not implemented | - |
| 10 | Empty date windows are handled gracefully | ❌ Not implemented | - |

**Implementation Status:** 1/10 (10%)

### export.feature (3 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1 | Export DuckDB table to Parquet | ⏳ Partial | test_export.py |
| 2 | Export with partitioning | ❌ Not implemented | - |
| 3 | Export handles large datasets | ❌ Not implemented | - |

**Implementation Status:** 0/3 (0% - basic test exists but incomplete)

**Tier 0 Total Progress:** 1/13 scenarios (8%)

## Tier 1: Core Features

**Priority:** P1 - Implement after Tier 0 is stable

### backfill.feature (14 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1 | Backfill a single month | ❌ Not implemented | - |
| 2 | Backfill multiple months | ❌ Not implemented | - |
| 3 | Backfill with date range validation | ❌ Not implemented | - |
| ... | (11 more scenarios) | ❌ Not implemented | - |

**Implementation Status:** 0/14 (0%)

### verification.feature (20 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1 | Detect missing windows | ⏳ Partial | test_verification.py |
| 2 | Detect suspect windows | ⏳ Partial | test_verification.py |
| 3 | Detect incomplete windows | ⏳ Partial | test_verification.py |
| ... | (17 more scenarios) | ❌ Not implemented | - |

**Implementation Status:** 0/20 (0% - basic structure exists)

### state_management.feature (14 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1-14 | Various state management scenarios | ❌ Not implemented | - |

**Implementation Status:** 0/14 (0%)

### data_quality.feature (20 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1-20 | Various data quality scenarios | ❌ Not implemented | - |

**Implementation Status:** 0/20 (0%)

### resilience.feature (20 scenarios)

| # | Scenario | Status | Step Defs |
|---|----------|--------|-----------|
| 1-20 | Various resilience scenarios | ❌ Not implemented | - |

**Implementation Status:** 0/20 (0%)

**Tier 1 Total Progress:** 0/88 scenarios (0%)

## Tier 2: Operator Experience

**Priority:** P2 - Implement last (UX enhancements)

Most Tier 2 features are already implemented (progress bars, Rich formatting, etc.) but lack BDD coverage. Focus on Tier 0-1 first.

## Implementation Strategy

### Phase 1: Tier 0 Foundation (Current Priority)

**Goal:** Get all Tier 0 scenarios passing

1. **extraction.feature** (9 remaining scenarios)
   - Focus on: pagination, deduplication, error handling
   - Estimated effort: ~2-3 days

2. **export.feature** (2 remaining scenarios)
   - Focus on: partitioning, large dataset handling
   - Estimated effort: ~1 day

**Phase 1 Target:** 13/13 Tier 0 scenarios passing

### Phase 2: Tier 1 Core Features

**Goal:** Cover critical production features

1. **backfill.feature** (14 scenarios)
   - Estimated effort: ~2 days

2. **verification.feature** (20 scenarios)
   - Estimated effort: ~3 days

3. **state_management.feature** (14 scenarios)
   - Estimated effort: ~2 days

4. **data_quality.feature** (20 scenarios)
   - Estimated effort: ~3 days

5. **resilience.feature** (20 scenarios)
   - Estimated effort: ~3 days

**Phase 2 Target:** 88/88 Tier 1 scenarios passing

### Phase 3: Tier 2 UX Features

**Goal:** Document existing UX features with BDD

Most features already work, need retrospective BDD coverage for:
- Progress bars
- Rich formatting
- Gap icons/colors
- JSON output mode

## Current Priorities

**Next Steps (in order):**

1. ✅ Set up BDD infrastructure (pytest-bdd) - DONE
2. 🔄 Complete extraction.feature step definitions - IN PROGRESS
3. ⏳ Complete export.feature step definitions
4. ⏳ Move to Tier 1 features

## Step Definition Organization

### Current Structure

```
tests/
├── features/           # Gherkin feature files
│   ├── extraction.feature
│   ├── export.feature
│   ├── backfill.feature
│   ├── verification.feature
│   ├── state_management.feature
│   ├── data_quality.feature
│   └── resilience.feature
├── step_defs/         # Step definition implementations
│   ├── test_extraction.py      # ✅ Started (1/10 scenarios)
│   ├── test_export.py          # ⏳ Partial
│   ├── test_verification.py    # ⏳ Partial
│   └── test_backfill.py        # ❌ Not created yet
│   └── test_state.py           # ❌ Not created yet
│   └── test_quality.py         # ❌ Not created yet
│   └── test_resilience.py      # ❌ Not created yet
└── conftest.py        # Shared fixtures and utilities
```

### Naming Convention

- Feature file: `features/{feature_name}.feature`
- Step defs file: `step_defs/test_{feature_name}.py`
- Each step def file implements scenarios from corresponding feature file

### Common Step Patterns

**Given steps** (setup):
- Database setup
- API mocking
- Pre-existing data conditions

**When steps** (actions):
- CLI command execution
- API calls
- Data operations

**Then steps** (assertions):
- Database state verification
- Output validation
- Error checking

## Running BDD Tests

```bash
# Run all BDD tests
pytest tests/step_defs/

# Run specific feature
pytest tests/step_defs/test_extraction.py

# Run with verbose output
pytest tests/step_defs/ -v

# Run specific scenario
pytest tests/step_defs/test_extraction.py::test_extracting_data_for_a_given_period

# Run by tier (using markers, to be added)
pytest tests/step_defs/ -m tier0
pytest tests/step_defs/ -m tier1
```

## Progress Metrics

**Overall:** 1/105 scenarios (1%)

**By Tier:**
- Tier 0: 1/13 (8%)
- Tier 1: 0/88 (0%)
- Tier 2: N/A (focus on implementation first)

**Target Milestones:**
- 🎯 Tier 0 Complete: 13/13 scenarios (target: Week 1)
- 🎯 Tier 1 50%: 44/88 scenarios (target: Week 3)
- 🎯 Tier 1 Complete: 88/88 scenarios (target: Week 5)

## Notes

- Existing tests use VCR for HTTP recording/replay
- Some step definitions are reusable across multiple scenarios
- Focus on Tier 0 before expanding to Tier 1
- Tier 2 UX features mostly implemented, BDD coverage is optional
