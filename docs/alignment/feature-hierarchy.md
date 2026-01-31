# Baliza Feature Hierarchy

**Purpose:** This document establishes a clear, prioritized hierarchy for all Baliza features, enabling rational decision-making about what to build, maintain, and defer.

**Last Updated:** 2024-07-24

**Implementation Status:** ✅ Tier system is implemented in code. Run `baliza tiers` to view the classification of all commands. Subcommands under `baliza state` provide observability.

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

| Feature | Command | Status | LOC | Priority |
|---------|---------|--------|-----|----------|
| Basic extraction | `baliza extract` | ✅ Done | ~200 | P0 |
| DuckDB storage | (embedded) | ✅ Done | ~150 | P0 |
| Basic export | `baliza export` | ✅ Done | ~150 | P0 |
| Network retry | (embedded) | ✅ Done | ~50 | P0 |
| State persistence | (embedded) | ✅ Done | ~100 | P0 |

## Tier 1: Core Features 🟠

**Definition:** Critical for production use, but the tool provides value without them.

### Features

| Feature | Command | Status | LOC | Priority |
|---------|---------|--------|-----|----------|
| Incremental extraction | `baliza extract --lookback-days` | ✅ Done | ~100 | P1 |
| Gap detection | `baliza state gaps` | ✅ Done | ~300 | P1 |
| Historical backfill | `baliza backfill` | ✅ Done | ~100 | P1 |
| Coverage tracking | `baliza state show` | ✅ Done | ~150 | P1 |
| Run history | `baliza state history` | ✅ Done | ~100 | P1 |

## Tier 2: Operator Experience 🟡

**Definition:** Quality-of-life improvements that make the tool pleasant to use but aren't essential.

### Features

| Feature | Implementation | Status | LOC | Priority |
|---------|---------------|--------|-----|----------|
| Rich formatting | Rich library, panels, tables | ✅ Done | ~200 | P2 |
| Progress bars | SpinnerColumn, BarColumn | ✅ Done | ~100 | P2 |
| Tier classification | `baliza tiers` | ✅ Done | ~50 | P2 |

## Tier 3: Future Enhancements ⚪

**Definition:** Aspirational features that would be nice to have but are not currently needed.

### Planned Features (Not Implemented)

| Feature Category | Examples | Status | Priority |
|-----------------|----------|--------|----------|
| Advanced resilience | Retry strategies, circuit breakers | 📝 Documented | P3 |
| Data validation | Schema checks, quality reports | 📝 Documented | P3 |
| Multi-endpoint | compras, licitacoes support | 📝 Documented | P3 |

## Conclusion

The Baliza CLI now follows a structured Tier system, with core extraction and state management features implemented and verified via BDD tests.
