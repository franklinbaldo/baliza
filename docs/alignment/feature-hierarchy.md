# Baliza Feature Hierarchy

**Purpose:** This document establishes a clear, prioritized hierarchy for all Baliza features, enabling rational decision-making about what to build, maintain, and defer.

**Last Updated:** 2024-07-24

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

| Feature | Command | Status | Priority |
|---------|---------|--------|----------|
| Basic extraction | `baliza extract` | ✅ Done | P0 |
| DuckDB storage | (embedded) | ✅ Done | P0 |
| Basic export | `baliza export` | ✅ Done | P0 |
| Network retry | (embedded) | ✅ Done | P0 |
| State persistence | (embedded) | ✅ Done | P0 |

## Tier 1: Core Features 🟠

**Definition:** Critical for production use, but the tool provides value without them.

**Acceptance Criteria:** Reliable resumability, historical backfill, coverage visibility.

### Features

| Feature | Command | Status | Priority |
|---------|---------|--------|----------|
| Incremental extraction | `baliza extract --lookback-days` | 📝 Planned | P1 |
| Gap detection | `baliza verify` | ✅ Done | P1 |
| Historical backfill | `baliza backfill` | 📝 Planned | P1 |
| Coverage tracking | `baliza state show` | 📝 Planned | P1 |
| Gap listing | `baliza state gaps` | 📝 Planned | P1 |
| Run history | `baliza state history` | 📝 Planned | P1 |
| Date filtering | `--start/--end` flags | ✅ Done | P1 |

## Tier 2: Operator Experience 🟡

**Definition:** Quality-of-life improvements that make the tool pleasant to use but aren't essential.

**Acceptance Criteria:** Operators understand what's happening, can debug issues, get helpful feedback.

### Features

| Feature | Implementation | Status | Priority |
|---------|---------------|--------|----------|
| Rich formatting | Rich library, panels, tables | ✅ Done | P2 |
| Progress bars | SpinnerColumn, BarColumn | ✅ Done | P2 |
| Buffer statistics | `baliza buffer-stats` | ✅ Done | P2 |
| Overall status | `baliza status` | ✅ Done | P2 |
| Detailed errors | Clear error messages | ✅ Done | P2 |

## Tier 3: Future Enhancements ⚪

**Definition:** Aspirational features that would be nice to have but are not currently needed.

### Planned Features (Not Implemented)

| Feature Category | Examples | Status | Priority |
|-----------------|----------|--------|----------|
| Multi-endpoint | compras, licitacoes support | 📝 Documented | P3 |
| Performance | Parallel extraction, caching | 📝 Documented | P3 |
| JSON output | Machine-readable output | 📝 Documented | P3 |
| Configuration | External config files | 📝 Documented | P3 |

## Current State Assessment

### Epic Mapping

| Epic | Primary Tiers | Status |
|------|--------------|--------|
| **Epic 1: Resumable Extraction** | Tier 0 + Tier 1 | ⏳ In Progress |
| **Epic 2: Automated Publishing** | Tier 1 | ❌ Planned |
| **Epic 3: Expanded Endpoints** | Tier 3 | ❌ Planned |
| **Epic 4: Data Quality** | Tier 1 + Tier 3 | ⏳ In Progress |

### Recommendations

1. **Implement State Management CLI** 🟠 **HIGH**
   - Consolidate fragmented observability into `baliza state` subcommands.
   - This will unify `status`, `verify`, and `buffer-stats`.

2. **Implement Backfill Command** 🟠 **HIGH**
   - Automate historical data collection.

3. **Improve Resilience Testing** 🔴 **CRITICAL**
   - Fix the skipped/placeholder BDD tests for resilience to ensure the pipeline is robust.
