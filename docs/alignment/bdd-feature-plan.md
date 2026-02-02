# Baliza BDD Feature Set - Comprehensive Plan

## Purpose

This document outlines the BDD feature set for the Baliza CLI project, aligning it with the 4-tier hierarchy.

## 4-Tier Feature Hierarchy

*   **Tier 0 (Critical Path):** Without these, the tool is useless. (Extraction, Basic Storage)
*   **Tier 1 (Core Features):** Essential for production use. (Resumability, Export, Resilience)
*   **Tier 2 (Operator Experience):** Quality-of-life improvements. (State Management, Verification)
*   **Tier 3 (Future Enhancements):** Aspirational features. (Advanced Data Quality, IA Upload)

## Existing Features & Alignment

| Feature File | Scenarios | Tier | Status | Goal |
|---|---|---|---|---|
| `end_to_end_extraction.feature` | 1 | Tier 1 | ⚠️ Quarantined | Reliable Data Extraction |
| `checkpoint.feature` | 4 | Tier 1 | ✅ Implemented | Reliable Data Extraction |
| `buffer_management.feature` | 4 | Tier 1 | ✅ Implemented | Reliable Data Extraction |
| `resilience.feature` | 2 | Tier 1 | ❌ Not Implemented | Reliable Data Extraction |
| `export.feature` | 1 | Tier 1 | ✅ Implemented | Accessibility for Analysis |
| `daily_export.feature` | 5 | Tier 1 | ✅ Implemented | Data Preservation |
| `state_management.feature` | 3 | Tier 2 | ⚠️ Partial | Operator Experience |
| `verification.feature` | 1 | Tier 2 | ✅ Implemented | Data Quality Monitoring |

## Missing/Planned Features

### 1. backfill.feature (Tier 1)
**Goal:** Deterministic historical data consolidation.
- Backfill processes full months sequentially.
- Backfill handles overlapping data with merge strategy.

### 2. data_quality.feature (Tier 3)
**Goal:** Ensure data integrity.
- Primary keys are unique.
- Data types match schema expectations.

## Implementation Roadmap

### Phase 1: Stability & CLI Completion (Current)
- [ ] Stabilize `end_to_end_extraction.feature` by replacing `pytest-httpx` with `monkeypatch`.
- [ ] Implement missing step definitions in `resilience.feature`.
- [ ] Implement `state` command group (show, gaps, history) in `cli_simple.py`.
- [ ] Implement `backfill` command in `cli_simple.py`.

### Phase 2: Expanded Coverage
- [ ] Add `backfill.feature` and its implementation.
- [ ] Expand `verification.feature` with more detailed gap analysis.

### Phase 3: Maturity
- [ ] Add `data_quality.feature`.
- [ ] Enhance observability and logging.
