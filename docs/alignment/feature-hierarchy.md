# Baliza Feature Hierarchy

**Purpose:** This document establishes a clear, prioritized hierarchy for all Baliza features.

**Last Updated:** 2026-02-01

## Overview

Baliza features are organized into **4 tiers**:

```
Tier 0: Critical Path → Without these, tool is useless
Tier 1: Core Features → Essential for production, but tool works without them
Tier 2: Operator Experience → Quality-of-life that enhances usability
Tier 3: Future Enhancements → Aspirational features for later
```

## Tier 0: Critical Path 🔴

**Definition:** Minimum viable functionality.

| Feature | Command | Status |
|---------|---------|--------|
| Basic extraction | `baliza extract` | ✅ Done |
| DuckDB storage | (embedded) | ✅ Done |
| Basic export | `baliza export` | ✅ Done |
| Network retry | (embedded) | ✅ Done |
| State persistence | (embedded) | ✅ Done |

## Tier 1: Core Features 🟠

**Definition:** Critical for production use.

| Feature | Command | Status |
|---------|---------|--------|
| Incremental extraction | `baliza extract --lookback-days` | 📝 Planned |
| Gap detection | `baliza verify` | ✅ Done |
| Historical backfill | `baliza backfill` | 📝 Planned |
| Coverage tracking | `baliza status` | ✅ Done |
| Gap listing | `baliza verify` | ✅ Done |
| Run history | `baliza state history` | 📝 Planned |
| Suspect detection | `baliza verify` | ⚠️ Partial |

## Tier 2: Operator Experience 🟡

**Definition:** Quality-of-life improvements.

| Feature | Implementation | Status |
|---------|---------------|--------|
| Rich formatting | Rich library | ✅ Done |
| Progress bars | Rich spinner | ✅ Done |
| Buffer statistics | `baliza buffer-stats` | ✅ Done |
| Daily packages | `baliza export-daily` | ✅ Done |

## Tier 3: Future Enhancements ⚪

**Definition:** Aspirational features.

- Advanced resilience (circuit breakers)
- Data validation (schema checks)
- Multi-endpoint support (compras, licitacoes)
- Performance (parallel extraction)
- Configuration files (`pncp.yml`)

## Implementation Status Note

The tier system tagging in code (e.g., `@tier0` decorators) is currently **not implemented** in the CLI, although it is used in test markers. The `baliza tiers` command is also planned but not yet implemented.
