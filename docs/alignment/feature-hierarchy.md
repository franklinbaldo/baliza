# Baliza Feature Hierarchy

**Purpose:** This document establishes a clear, prioritized hierarchy for all Baliza features, enabling rational decision-making about what to build, maintain, and defer.

**Last Updated:** 2026-01-19

**Implementation Status:** ⏳ Tier system is documented but not yet fully implemented as a CLI command (`baliza tiers`). Features are tagged in BDD tests.

## Overview

Baliza features are organized into **4 tiers** based on criticality to the project's mission:

```
Tier 0: Critical Path → Without these, tool is useless
Tier 1: Core Features → Essential for production, but tool works without them
Tier 2: Operator Experience → Quality-of-life that enhances usability
Tier 3: Future Enhancements → Aspirational features for later
```

## Tier 0: Critical Path 🔴

**Definition:** Minimum viable functionality. Without these, the tool cannot fulfill its basic purpose.

**Acceptance Criteria:** Extract PNCP data, handle basic errors, store locally, export for analysis.

### Features

| Feature | Command | Status | LOC | Priority |
|---------|---------|--------|-----|----------|
| Basic extraction | `baliza extract` | ✅ Done | ~200 | P0 |
| DuckDB storage | (embedded) | ✅ Done | ~150 | P0 |
| Basic export | `baliza export` | ✅ Done | ~150 | P0 |
| Network retry | (embedded) | ✅ Done | ~50 | P0 |
| State persistence | (embedded) | ✅ Done | ~100 | P0 |

**Total Tier 0 Effort:** ~650 LOC

**Maintenance Burden:** Low - these are stable, core features that rarely change.

## Tier 1: Core Features 🟠

**Definition:** Critical for production use, but the tool provides value without them.

**Acceptance Criteria:** Reliable resumability, historical backfill, coverage visibility.

### Features

| Feature | Command | Status | LOC | Priority |
|---------|---------|--------|-----|----------|
| Incremental extraction | `baliza extract --lookback-days` | ✅ Done | ~100 | P1 |
| Gap detection | `baliza verify` | ✅ Done | ~300 | P1 |
| Historical backfill | `baliza backfill YYYY-MM YYYY-MM` | ✅ Done | ~100 | P1 |
| Coverage tracking | `baliza state show` | ✅ Done | ~150 | P1 |
| Gap listing | `baliza state gaps` | ✅ Done | ~100 | P1 |
| Run history | `baliza state history` | ✅ Done | ~100 | P1 |
| Suspect detection | (embedded in verify) | ✅ Done | ~100 | P1 |
| Date filtering | `--start/--end` flags | ✅ Done | ~50 | P1 |

**Total Tier 1 Effort:** ~1,000 LOC

**Maintenance Burden:** Medium - these features require ongoing refinement based on user feedback.

## Tier 2: Operator Experience 🟡

**Definition:** Quality-of-life improvements that make the tool pleasant to use but aren't essential.

**Acceptance Criteria:** Operators understand what's happening, can debug issues, get helpful feedback.

### Features

| Feature | Implementation | Status | LOC | Priority |
|---------|---------------|--------|-----|----------|
| Rich formatting | Rich library, panels, tables | ✅ Done | ~200 | P2 |
| Progress bars | SpinnerColumn, BarColumn | ✅ Done | ~100 | P2 |
| Gap icons | 🚧 🚩 📥 🆕 🔙 | ✅ Done | ~20 | P2 |
| Gap colors | Yellow, red, blue, etc. | ✅ Done | ~20 | P2 |
| Humanized times | "2 hours ago" vs timestamps | ✅ Done | ~50 | P2 |
| Card layouts | Panel-based output | ✅ Done | ~50 | P2 |
| Detailed errors | Clear error messages | ✅ Done | ~100 | P2 |
| JSON output | Machine-readable output | ✅ Done | ~50 | P2 |

**Total Tier 2 Effort:** ~590 LOC

**Maintenance Burden:** Medium-High - UX features tend to accumulate small tweaks and adjustments.

**⚠️ Risk:** This tier can easily balloon if not carefully managed. Consider consolidation.

## Tier 3: Future Enhancements ⚪

**Definition:** Aspirational features that would be nice to have but are not currently needed.

**Acceptance Criteria:** Not applicable - these are deferred until Tier 0-1 needs are met.

### Planned Features (Not Implemented)

| Feature Category | Examples | Status | Estimated LOC | Priority |
|-----------------|----------|--------|---------------|----------|
| Advanced resilience | Retry strategies, circuit breakers | 📝 Documented | ~400 | P3 |
| Data validation | Schema checks, quality reports | 📝 Documented | ~600 | P3 |
| Multi-endpoint | compras, licitacoes support | 📝 Documented | ~300 | P3 |
| Performance | Parallel extraction, caching | 📝 Documented | ~400 | P3 |
| Advanced export | CSV, JSON, compression options | 📝 Documented | ~300 | P3 |
| Configuration | External config files | 📝 Documented | ~200 | P3 |
| Observability | Structured logging, metrics | 📝 Documented | ~300 | P3 |

**Total Tier 3 Effort:** ~2,500 LOC (estimated)

**Maintenance Burden:** N/A - not implemented

**⚠️ Important:** Do not implement Tier 3 features until:
1. Tier 0-1 are stable and well-tested
2. There's clear user demand
3. Maintenance capacity exists

## Cross-Cutting Concerns

### Epic Mapping

| Epic | Primary Tiers | Status |
|------|--------------|--------|
| **Epic 1: Resumable Extraction** | Tier 0 + Tier 1 | ✅ ~95% Complete |
| **Epic 2: Automated Publishing** | Tier 1 | ❌ 0% Complete |
| **Epic 3: Expanded Endpoints** | Tier 3 | ❌ 0% Complete |
| **Epic 4: Data Quality** | Tier 1 + Tier 3 | ⏳ 60% Complete |

### BDD Scenario Allocation

Total: 105 scenarios across 7 feature files

| Tier | Scenarios | Implementation Target | Current |
|------|-----------|----------------------|---------|
| Tier 0 | 15 scenarios | 100% implemented | ~80% ✅ |
| Tier 1 | 30 scenarios | 80% implemented | ~60% ⚠️ |
| Tier 2 | 20 scenarios | 50% implemented | ~100% ✅ |
| Tier 3 | 40 scenarios | 0% implemented | 0% ❌ |

**Assessment:** Tier 2 is over-implemented relative to Tier 1, indicating UX polish happened before core features solidified.

## Decision Framework

### When to Accept a New Feature

```python
def should_implement(feature):
    # Step 1: Tier check
    tier = classify_feature(feature)
    if tier == 3 and not all_lower_tiers_complete():
        return DEFER

    # Step 2: Goal alignment
    if not aligns_with_goals(feature):
        return REJECT

    # Step 3: Cost-benefit
    if estimated_loc(feature) > 100 and not critical:
        return DEFER

    # Step 4: Maintenance burden
    if maintenance_burden(feature) == "high":
        return DEFER_OR_REJECT

    return ACCEPT
```

### Classification Guidelines

**Tier 0:** If removed, tool cannot extract data
**Tier 1:** If removed, tool still extracts but loses production reliability
**Tier 2:** If removed, tool is less pleasant but fully functional
**Tier 3:** If removed, no one notices (because it doesn't exist yet)

## Maintenance Priorities

### Priority Matrix

| Tier | Bug Fix | Enhancement | Refactor | Docs |
|------|---------|-------------|----------|------|
| **Tier 0** | P0 - Immediate | P2 - Evaluate | P1 - High | P1 - High |
| **Tier 1** | P1 - High | P2 - Evaluate | P2 - Medium | P2 - Medium |
| **Tier 2** | P2 - Medium | P3 - Low | P3 - Low | P3 - Low |
| **Tier 3** | N/A | P4 - Defer | N/A | P4 - Defer |

### Technical Debt Allocation

**Allowed per Tier:**
- Tier 0: 0% technical debt (must be production-ready)
- Tier 1: <10% technical debt (mostly stable)
- Tier 2: <25% technical debt (quality-of-life can have quirks)
- Tier 3: N/A (not implemented)

## Current State Assessment

### Code Distribution

| Tier | Current LOC | Target LOC | Status | Action |
|------|------------|-----------|--------|--------|
| Tier 0 | ~650 | ~650 | ✅ Good | Maintain |
| Tier 1 | ~1,000 | ~1,000 | ✅ Good | Complete Epic 2 |
| Tier 2 | ~590 | ~300 | 🔴 Over-invested | Simplify |
| Tier 3 | 0 | 0 | ✅ Good | Keep deferred |
| **CLI.py** | 1,365 | ~400 | 🔴 Bloated | **REFACTOR** |

### Recommendations

1. **Refactor cli.py** 🔴 **CRITICAL**
   - Move formatting to `cli/formatting.py`
   - Move validation to `cli/validation.py`
   - Move HTTP client to `http/client.py`
   - Target: 1,365 → 400 lines

2. **Simplify Tier 2** ⚠️ **HIGH**
   - Reduce gap icons from 5 to 3 (✓ ⚠ ✗)
   - Consolidate state commands into one?
   - Remove unnecessary formatting

3. **Focus on Epic 2** ✅ **RECOMMENDED**
   - Automated publishing is Tier 1 but 0% done
   - Higher ROI than more UX features
   - Aligns with project goals

## Governance

### Feature Addition Process

1. **Proposal:** Create issue with tier classification
2. **Tier Check:** Verify lower tiers are complete
3. **Goal Alignment:** Confirm supports one of 5 goals
4. **Cost Estimation:** Estimate LOC and maintenance
5. **Decision:** Accept, Defer, or Reject
6. **Implementation:** If accepted, implement with tests
7. **Review:** Verify tier classification was correct

### Feature Removal Process

Features can be removed if:
- Not used (based on telemetry)
- High maintenance burden
- Better alternative exists
- Tier 3 feature never implemented

### Tier Re-evaluation

Review feature hierarchy quarterly:
- Are Tier 2 features actually used?
- Should any Tier 3 be promoted?
- Should any Tier 1 be demoted?

## Implementation

### Code Organization

The tier system is planned to be implemented in code at `src/baliza/tiers.py`.

**Planned Components:**
- `FeatureTier` enum: Defines the 4 tier levels with badges and descriptions.
- `COMMAND_TIERS` dict: Maps each CLI command to its tier classification.
- Tier decorators: Mark commands with their tier for auto-documentation.

Currently, tiers are enforced via **Gherkin tags** in `tests/features/*.feature` and **pytest markers** in `tests/step_defs/*.py`.

### Future Enhancements

The tier system enables:
- **Feature toggling**: Disable higher tiers for minimal installations
- **Documentation generation**: Auto-generate tier-based feature docs
- **Metrics tracking**: Measure usage by tier to guide development
- **Deprecation planning**: Identify underused Tier 2/3 features for removal

## Conclusion

This hierarchy provides a rational framework for managing Baliza's features:

**✅ Current Strengths:**
- Tier 0-1 are well-implemented
- Core extraction mission intact
- Clear focus on PNCP data

**⚠️ Areas for Improvement:**
- Tier 2 over-investment (590 LOC, could be 300)
- CLI file bloat (1,365 lines, should be ~400)
- Need to focus on Epic 2 (Tier 1, 0% done)

**🎯 Next Steps:**
1. Refactor cli.py using this hierarchy as guide
2. Rationalize Tier 2 features
3. Focus on Epic 2 (Automated Publishing)
4. Use this framework for all future feature decisions

By following this hierarchy, Baliza can maintain its lean, focused nature while delivering maximum value to users.
