# Baliza Feature Hierarchy

**Purpose:** This document establishes a clear, prioritized hierarchy for all Baliza features, enabling rational decision-making about what to build, maintain, and defer.

**Last Updated:** 2026-02-01

**Implementation Status:** ⚠️ Tier system is partially implemented via pytest markers (`@tier0`, `@tier1`, etc.). Full CLI-level integration (e.g., `baliza tiers`) is planned.

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

### Features

| Feature | Command | Status | Priority |
|---------|---------|--------|----------|
| Incremental extraction | `baliza extract --lookback-days` | ✅ Done | P1 |
| Gap detection | `baliza verify` | ✅ Done | P1 |
| Historical backfill | `baliza backfill` | ⏳ Planned | P1 |
| Coverage tracking | `baliza state show` | ⏳ Planned | P1 |
| Gap listing | `baliza state gaps` | ⏳ Planned | P1 |
| Run history | `baliza state history` | ⏳ Planned | P1 |
| Date filtering | `--start/--end` flags | ✅ Done | P1 |

## Tier 2: Operator Experience 🟡

**Definition:** Quality-of-life improvements that make the tool pleasant to use but aren't essential.

### Features

| Feature | Implementation | Status | Priority |
|---------|---------------|--------|----------|
| Rich formatting | Rich library, panels, tables | ✅ Done | P2 |
| Progress bars | SpinnerColumn, BarColumn | ✅ Done | P2 |
| Buffer statistics | `baliza buffer-stats` | ✅ Done | P2 |
| Overall status | `baliza status` | ✅ Done | P2 |

## Tier 3: Future Enhancements ⚪

**Definition:** Aspirational features that would be nice to have but are not currently needed.

### Planned Features (Not Implemented)

- Advanced resilience (circuit breakers)
- Data validation (schema checks)
- Multi-endpoint support (compras, licitacoes)
- Performance (parallel extraction)

## Current State Assessment

### Recommendations

1. **Implement Tier 1 Missing Commands** 🔴 **CRITICAL**
   - The `state` command group and `backfill` are documented but not yet implemented in `cli_simple.py`.
   - BDD tests currently use workarounds for these commands.

2. **Rationalize CLI Structure** ⚠️ **HIGH**
   - Move from `cli_simple.py` to a more structured `cli/` module as the tool grows.
   - Ensure consistent naming and grouping of commands.

3. **Complete Epic 1: Resumable Extraction**
   - Ensure the `state` commands provide full visibility into the resumable state.
