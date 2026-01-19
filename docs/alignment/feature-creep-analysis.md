# Feature Creep Analysis & Clean Hierarchy Proposal

**Date:** 2026-01-19
**Status:** 🔴 **MODERATE FEATURE CREEP DETECTED** (Updated with scope correction)
**Confidence:** High

## 🎯 Critical Scope Clarification

**IMPORTANT:** During this analysis, we discovered a key architectural misunderstanding:

**Two-Repository Architecture:**
1. **`franklinbaldo/baliza`** (THIS REPO) = Extraction ENGINE/CLI tool
2. **`franklinbaldo/baliza-site`** (SEPARATE REPO) = Orchestration, publishing, web interface

**Impact on Feature Creep Assessment:**
- Epic 2 (Automated Publishing) does NOT belong in this repo
- It belongs in `baliza-site` which will consume this CLI
- THIS repo's responsibility: provide stable, production-ready CLI tool
- THAT repo's responsibility: GitHub Actions workflows, web UI, public data platform

**What This Changes:**
- ✅ This repo is MORE complete than initially assessed (~95% for its scope)
- ✅ Focus should be on stability, documentation, Docker packaging
- ✅ Not on implementing GitHub Actions workflows here
- ⚠️ Feature creep assessment remains valid for UX over-engineering

See `docs/ARCHITECTURE.md` for detailed two-repository design.

## Executive Summary

Baliza has experienced **moderate feature creep** in UX/observability features while maintaining focus on core extraction goals. The project needs:
1. **Feature hierarchy rationalization** - Clear categorization of core vs. nice-to-have
2. **Code consolidation** - The 1,365-line CLI file needs refactoring
3. **BDD reality check** - 105 scenarios may be aspirational rather than realistic

## Evidence of Feature Creep

### 1. Code Complexity Analysis

| Metric | Value | Assessment |
|--------|-------|------------|
| **Total Python LOC** | 3,349 | Moderate for CLI tool |
| **cli.py size** | 1,365 lines (41%) | 🔴 **TOO LARGE** |
| **State management** | 1,266 lines (38%) | Acceptable for core feature |
| **CLI + State** | 2,631 lines (79%) | System is top-heavy |
| **Number of CLI commands** | 7 | High but justified |

**Red Flags:**
- cli.py is 1,365 lines but README claims "CLI enxuta" (lean CLI)
- 41% of codebase in a single file indicates poor separation of concerns
- 79% of code dedicated to CLI and state management vs. 9% for actual pipeline

### 2. Feature Explosion Timeline

#### Core Features (Aligned with MASTERPLAN)
✅ **Planned & Implemented:**
- `extract` command - incremental extraction
- `backfill` command - historical consolidation
- `export` command - Parquet output
- `verify` command - coverage audit
- StateManager - run tracking
- GapDetector - gap identification
- CoverageTracker - manifesto storage

#### UX/Observability Features (Scope Expansion)
⚠️ **Added Beyond Original Plan:**
- Rich library integration for formatting
- Gap icons (🚧 🚩 📥 🆕 🔙) with color coding
- Humanized timestamps (naturaltime)
- Card-style layouts for CLI output
- Progress bars with spinners
- Multiple state subcommands (show, gaps, history)
- JSON caching for tests
- Dry-run modes

**Assessment:** These UX features support Goal 5 (Excellent Operator Experience) but may have been over-engineered.

### 3. BDD Feature Set Analysis

| Category | Scenarios | Reality Check |
|----------|-----------|---------------|
| **Current implementation** | ~15 | Actual tested scenarios |
| **Just created** | 105 | Aspirational documentation |
| **Gap** | 90 scenarios | 🔴 **MASSIVE GAP** |

**BDD Breakdown:**
- extraction.feature: 10 scenarios (2-3 implemented)
- state_management.feature: 14 scenarios (3-4 implemented)
- backfill.feature: 14 scenarios (1-2 implemented)
- verification.feature: 14 scenarios (2 implemented)
- export.feature: 13 scenarios (1 implemented)
- resilience.feature: 20 scenarios (0 implemented)
- data_quality.feature: 20 scenarios (0 implemented)

**Conclusion:** I created an aspirational test suite that documents desired behavior, not current reality. This could create pressure to implement everything.

### 4. Feature Hierarchy Issues

**Current State:** Flat feature list with no prioritization
- All 7 CLI commands presented as equally important
- No distinction between core and auxiliary features
- State management has 3 subcommands (show, gaps, history) - is this necessary?

**Missing Hierarchy:**
- No clear "MVP" vs "nice-to-have" distinction
- No feature flags or gradual rollout
- Everything is always-on by default

## Feature Creep Score by Category

| Category | Score | Justification |
|----------|-------|---------------|
| **Core Extraction** | ✅ 2/10 | Focused and aligned with goals |
| **State Management** | ⚠️ 5/10 | Necessary but could be simpler |
| **UX/Observability** | 🔴 8/10 | Over-engineered with icons, colors, humanization |
| **Testing/Documentation** | 🔴 9/10 | 105 aspirational scenarios created |
| **Overall** | ⚠️ 6/10 | **MODERATE CREEP** |

## Root Causes

### 1. Lack of Feature Categorization
No framework for deciding "Is this core or nice-to-have?"

### 2. UX Polish Before Product-Market Fit
Added icons, colors, card layouts before validating core use cases

### 3. Documentation-Driven Development Gone Wrong
Created 105 BDD scenarios that may never be implemented

### 4. Missing "Done" Definition
No clear criteria for when a feature is complete enough

## Proposed Clean Feature Hierarchy

### Tier 0: Critical Path (Must Have)
**Definition:** Without these, the tool is useless

```
baliza/
├── extract              # Incremental extraction with lookback
├── export               # Basic Parquet export (no frills)
├── state/
│   └── (embedded)       # Minimal state for resumability
└── verify               # Basic gap detection
```

**Acceptance Criteria:**
- Extract data from PNCP ✅
- Handle network interruptions ✅
- Export to Parquet ✅
- Show if data has gaps ✅

### Tier 1: Core Features (Should Have)
**Definition:** Critical for production use, but tool works without them

```
baliza/
├── backfill             # Historical consolidation
├── state/
│   ├── show             # Coverage summary
│   └── gaps             # Detailed gap listing
└── verify --detailed    # Gap reasons and statistics
```

**Acceptance Criteria:**
- Backfill historical data ✅
- Understand current coverage ✅
- Identify what's missing ✅

### Tier 2: Operator Experience (Nice to Have)
**Definition:** Quality-of-life improvements that enhance usability

```
UX Enhancements:
├── Rich formatting       # Colors, panels, tables
├── Progress bars         # Visual feedback
├── Icons for gap types   # 🚧 🚩 📥
├── Humanized times       # "2 hours ago"
└── state history         # Run history tracking
```

**Current Status:** ✅ Implemented (possibly over-engineered)

### Tier 3: Future Enhancements (Won't Have Yet)
**Definition:** Aspirational features for later

```
Future:
├── resilience/*          # Advanced retry logic
├── data_quality/*        # Comprehensive validation
├── Multi-endpoint        # compras, licitacoes
├── Performance tuning    # Optimization
└── Advanced exports      # CSV, JSON, compression options
```

**Current Status:** ⏳ Documented in BDD but not implemented

## Recommendations

### Immediate Actions (This Week)

1. **Refactor cli.py** 🔴 **CRITICAL**
   - Extract UX helpers to `cli/formatting.py`
   - Extract validators to `cli/validation.py`
   - Extract HTTP client logic to separate module
   - **Target:** Reduce cli.py from 1,365 → ~400 lines

2. **Update ROADMAP with Reality** ⚠️ **HIGH PRIORITY**
   - Mark Epic 1 as "~95% Complete" (not "immediate priority")
   - Clarify that resilience features are Tier 3
   - Update "Estado atual" section to reflect StateManager is done

3. **Rationalize BDD Scenarios** ⚠️ **HIGH PRIORITY**
   - Mark scenarios as `@tier0`, `@tier1`, `@tier2`, `@tier3`
   - Focus implementation on Tier 0-1 scenarios only
   - Document Tier 2-3 as "future" explicitly

4. **Create Feature Hierarchy Doc** ✅ **RECOMMENDED**
   - Document this analysis as `docs/alignment/feature-hierarchy.md`
   - Provide decision framework for future features
   - Reference in CONTRIBUTING.md for contributors

### Medium-Term Actions (Next 2-4 Weeks)

5. **Audit State Commands**
   - Do we need 3 subcommands (show, gaps, history)?
   - Could `state` show everything in one view?
   - Consider consolidation

6. **Simplify UX Tier**
   - Are 5 different gap icons necessary?
   - Could we use ✓, ⚠, ✗ only?
   - Review Rich usage for over-engineering

7. **Focus on Epic 2 Support**
   - Automated publishing belongs in baliza-site repo
   - THIS repo needs: documentation for orchestration, Docker image
   - Ensure CLI is production-ready for automated usage

### Long-Term Strategy

8. **Establish Feature Decision Framework**
   ```
   For each new feature, ask:
   1. Which tier does this belong to?
   2. Is a lower tier incomplete?
   3. Is this solving a real user problem?
   4. What's the maintenance burden?
   5. Can we defer to Tier 3?
   ```

9. **Create "Feature Freeze" Policy**
   - Once baliza-site development starts, freeze Tier 2-3 in this repo
   - No new UX features until CLI is production-ready
   - Focus on stability, documentation, Docker packaging

10. **Measure Feature Usage**
    - Add telemetry (opt-in) to understand what's actually used
    - Deprecate unused features
    - Data-driven prioritization

## Decision Framework for Future Features

### ✅ Accept if:
- Directly supports one of the 5 concrete goals
- Required for Epic 1-4 completion
- Solves documented user pain point
- Adds <100 LOC

### ⚠️ Defer if:
- "Nice to have" UX improvement
- No user request for it
- Tier 2-3 feature
- Adds >100 LOC
- Alternative simpler solution exists

### ❌ Reject if:
- Outside project scope (web UI, real-time, etc.)
- Conflicts with non-goals
- Maintenance burden > benefit
- Already solved by external tools
- Premature optimization

## Conclusion

**Feature Creep Assessment:** ⚠️ **MODERATE (6/10)**

Baliza has experienced moderate feature creep primarily in:
- UX/observability layer (icons, colors, formatting)
- Aspirational BDD documentation (105 scenarios)
- Single-file complexity (1,365-line cli.py)

However, the **core extraction mission remains intact** and the features added generally support stated goals.

**Key Issue:** Lack of clear feature hierarchy and prioritization framework.

**Recommended Path Forward:**
1. Refactor cli.py immediately (reduce from 1,365 → 400 lines)
2. Implement the proposed 4-tier feature hierarchy
3. Prepare for baliza-site (documentation, Docker, stable CLI) rather than more UX polish
4. Rationalize BDD scenarios to reflect reality vs. aspiration

**With these changes, Baliza can return to lean, focused development aligned with its North Star.**

---

## Appendix: Feature Inventory

### Implemented Features by Epic

**Epic 1: Resumable Extraction (~95% Complete)**
- ✅ StateManager with run tracking
- ✅ GapDetector with 4 gap types
- ✅ CoverageTracker with manifesto
- ✅ Integration with extract command
- ✅ State CLI commands (show, gaps, history)
- ✅ Backfill command
- ⏳ Advanced resilience (retry logic, timeouts) - partially done

**Epic 2: Automated Publishing** *(MOVED TO baliza-site REPOSITORY)*
**⚠️ SCOPE CORRECTION:** This epic belongs in franklinbaldo/baliza-site, NOT this repo.
- ✅ THIS REPO: Stable CLI commands (extract, export, verify)
- ✅ THIS REPO: Clear exit codes for CI/CD integration
- ✅ THIS REPO: JSON output mode for machine consumption
- ⏳ THIS REPO: Documentation for orchestration
- ⏳ THIS REPO: Docker container image
- ❌ BALIZA-SITE: GitHub Actions daily workflow
- ❌ BALIZA-SITE: Automated Parquet publish to Releases
- ❌ BALIZA-SITE: Release manifest generation
- ❌ BALIZA-SITE: Web interface and dashboards

**Epic 3: Expanded Endpoints (0% Complete)**
- ❌ compras endpoint
- ❌ licitacoes endpoint
- ❌ Configuration refactoring

**Epic 4: Data Quality (60% Complete)**
- ✅ Verify command with gap detection
- ✅ Coverage statistics
- ✅ Suspect window detection
- ⏳ Anomaly detection
- ❌ Public coverage report

**Tier 2 UX Enhancements (100% Complete)**
- ✅ Rich formatting
- ✅ Progress bars
- ✅ Gap icons and colors
- ✅ Humanized timestamps
- ✅ Card layouts
- ✅ State history tracking

**Tier 3 Future Features (0% Complete)**
- ❌ Comprehensive resilience testing
- ❌ Data quality validation suite
- ❌ Performance benchmarks
- ❌ Advanced export options
