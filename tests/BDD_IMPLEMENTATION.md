# BDD Implementation Plan (SIMPLIFIED)

**Purpose:** Get 5 critical scenarios to 100% passing, ship v1.0, iterate.

**Last Updated:** 2026-01-19

## Philosophy: Ship Early, Iterate

We deleted 100 scenarios and kept only 5 essential ones. Focus on getting these to 100% passing, then ship baliza v1.0 to baliza-site. Add tests for bugs we actually encounter.

## The Essential 5 Scenarios

### Tier 0: Critical Path (3 scenarios)

| # | Feature | Scenario | Status |
|---|---------|----------|--------|
| 1 | extraction.feature | Basic extraction works | ✅ PASSING |
| 2 | extraction.feature | Incremental extraction doesn't duplicate data | ⏸️ SKIPPED |
| 3 | export.feature | Export creates valid parquet | ⏳ READY TO TEST |

### Tier 1: Core Features (2 scenarios)

| # | Feature | Scenario | Status |
|---|---------|----------|--------|
| 4 | resilience.feature | Handles PNCP API errors gracefully | ⏸️ SKIPPED |
| 5 | verification.feature | Verify command detects gaps | ⏸️ SKIPPED |

## Current Progress

**Overall:** 1/5 scenarios (20%)
- Tier 0: 1/3 (33%)
- Tier 1: 0/2 (0%)

## Implementation Order

### Phase 1: Get Tier 0 to 100% ⚡ PRIORITY

1. ✅ **Basic extraction works** - DONE
2. ⏳ **Export creates valid parquet** - Test exists, needs verification
3. ⏸️ **Incremental extraction doesn't duplicate** - Needs implementation

**Target:** All 3 Tier 0 scenarios passing

### Phase 2: Add Tier 1 Safety Nets

4. ⏸️ **Handles API errors** - Needs httpx_mock setup
5. ⏸️ **Verify detects gaps** - Needs state table verification

**Target:** All 5 scenarios passing

### Phase 3: Ship v1.0

- Create PR
- Merge to main
- Tag v1.0.0
- Deploy to baliza-site
- Monitor for real bugs
- Add tests for bugs found

## Running Tests

```bash
# Run all tests
pytest tests/step_defs/ -v

# Run Tier 0 only (Critical Path)
pytest tests/step_defs/ -v -m tier0

# Run Tier 1 only (Core Features)
pytest tests/step_defs/ -v -m tier1

# Run specific scenario
pytest tests/step_defs/test_extraction.py::test_basic_extraction_works -v
```

## File Structure

```
tests/
├── features/                    # Simplified Gherkin files
│   ├── extraction.feature       # 2 scenarios (Tier 0)
│   ├── export.feature           # 1 scenario (Tier 0)
│   ├── resilience.feature       # 1 scenario (Tier 1)
│   └── verification.feature     # 1 scenario (Tier 1)
│
├── step_defs/                   # Step definition implementations
│   ├── test_extraction.py       # ✅ 1 passing, 1 skipped
│   ├── test_export.py           # ⏳ Ready to test
│   ├── test_resilience.py       # ⏸️ Needs HTTP mocking
│   └── test_verification.py     # ⏸️ Needs state setup
│
└── BDD_IMPLEMENTATION.md        # This file
```

## What We Deleted

**Removed 100 scenarios** from:
- backfill.feature (14 scenarios) - Deleted entire command
- state_management.feature (14 scenarios) - Over-engineering
- data_quality.feature (20 scenarios) - Premature
- Most of extraction.feature (8 scenarios) - Security theater
- Most of export.feature (10+ scenarios) - Nice-to-have
- Most of resilience.feature (19 scenarios) - Can add later
- Most of verification.feature (19 scenarios) - Overkill

**Why?** Analysis paralysis. Ship first, add tests when bugs appear.

## Success Criteria

### v1.0 Ready to Ship When:

- ✅ Basic extraction works
- ✅ Export creates valid parquet
- ✅ Incremental extraction doesn't duplicate
- ✅ Handles API errors gracefully
- ✅ Verify detects gaps

**All 5/5 passing = Ship it!**

## Anti-Patterns to Avoid

❌ Don't add more scenarios before v1.0 ships
❌ Don't test theoretical problems
❌ Don't test things that can't realistically break
❌ Don't write tests instead of shipping code

✅ Do ship v1.0 with 5 passing tests
✅ Do add tests when real bugs appear
✅ Do focus on actual failure modes
✅ Do get feedback from baliza-site usage

## Next Actions

1. ⏳ Fix export test if needed
2. ⏸️ Implement deduplication test
3. ⏸️ Implement error handling test
4. ⏸️ Implement verify gaps test
5. 🚀 Ship v1.0

**Target:** 5/5 passing by end of week, then SHIP.
