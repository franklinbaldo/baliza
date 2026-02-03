# Baliza Feature Hierarchy

**Purpose:** This document establishes a clear, prioritized hierarchy for all Baliza features, enabling rational decision-making about what to build, maintain, and defer.

**Last Updated:** 2026-02-11

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

| Feature | Command | Status | LOC (est) | Priority |
|---------|---------|--------|-----------|----------|
| Basic extraction | `baliza extract` | ✅ Done | ~150 | P0 |
| DuckDB storage | (embedded) | ✅ Done | ~100 | P0 |
| Basic export | `baliza export` | ✅ Done | ~50 | P0 |
| Network retry | (embedded) | ✅ Done | ~20 | P0 |
| Resumability | (checkpointing) | ✅ Done | ~50 | P0 |

## Tier 1: Core Features 🟠

**Definition:** Critical for production use, but the tool provides value without them.

### Features

| Feature | Command | Status | LOC (est) | Priority |
|---------|---------|--------|-----------|----------|
| Gap detection | `baliza verify` | ✅ Done | ~100 | P1 |
| Status overview | `baliza status` | ✅ Done | ~50 | P1 |
| Daily export | `baliza export-daily` | ✅ Done | ~150 | P1 |
| Buffer stats | `baliza buffer-stats` | ✅ Done | ~30 | P1 |

## Tier 2: Operator Experience 🟡

**Definition:** Quality-of-life improvements that make the tool pleasant to use but aren't essential.

### Features

| Feature | Implementation | Status | LOC (est) | Priority |
|---------|---------------|--------|-----------|----------|
| Rich formatting | Rich panels, tables | ✅ Done | ~100 | P2 |
| Progress bars | Rich progress | ✅ Done | ~50 | P2 |
| Detailed errors | Console output | ✅ Done | ~50 | P2 |

## Tier 3: Future Enhancements ⚪

**Definition:** Aspirational features that would be nice to have but are not currently needed.

### Planned Features (Not Implemented)

| Feature Category | Examples | Status | Priority |
|-----------------|----------|--------|----------|
| Historical backfill | `baliza backfill` | 📝 Planned | P3 |
| Run history | `baliza status --history` | 📝 Planned | P3 |
| Data validation | Schema checks | 📝 Planned | P3 |
| Multi-endpoint | compras, licitacoes | 📝 Planned | P3 |

## Current State Assessment

The Baliza CLI follows a "Simple" architecture focusing on core extraction and preservation goals. Tier 0 and Tier 1 are mostly complete, providing a solid foundation for reliable data collection from PNCP.

**Assessment:** The project is well-aligned with its "Simple" mission.

## Recommendations

1. **Maintain Resilience:** Ensure the `tenacity` retries and `checkpoint` logic remain robust.
2. **Expand Coverage:** Monitor `baliza verify` output to ensure no data is lost.
3. **Implement Backfill:** Add a `backfill` command to simplify historical data collection when needed.
