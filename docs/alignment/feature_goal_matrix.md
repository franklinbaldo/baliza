# Feature -> Goal Matrix

This document tracks the alignment of BDD features with the project goals defined in `goals.md`.

| Feature Path | Primary Goal Supported | Status | Action Taken |
|--------------|------------------------|--------|--------------|
| `tests/features/end_to_end_extraction.feature` | 1. Reliably extract data | ❌ Misaligned | Rewrite to test the actual, simpler extraction logic. |
| `tests/features/checkpoint.feature` | 1. Reliably extract data | ✅ Aligned | Keep. |
| `tests/features/export.feature` | 2. Create a preserved archive | ✅ Aligned | Keep. |
| `tests/features/verification.feature`| 1. Reliably extract data | ⚠️ Partially Misaligned | Review and simplify to match current verification capabilities. |
| `tests/features/resilience.feature`| 1. Reliably extract data | ❌ Misaligned | Retire. The described resumability does not exist. |
| `tests/features/daily_export.feature`| 2. Create a preserved archive | ✅ Aligned | Keep. |
| `tests/features/buffer_management.feature`| 2. Create a preserved archive | ❌ Misaligned | Retire. This feature is not implemented. |
