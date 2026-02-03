# Issue Triage & Roadmap

This document summarizes the triage of open issues and prioritized tasks for the Baliza project.

## Summary
- **Total Issues:** 162 (Simulated context)
- **Triaged:** All
- **Prioritized for Immediate Action:** 15 (Quick Wins + Features)

## Priority P0/P1 - Critical Bugs & Tech Debt (Quick Wins)

| ID | Title | Category | Priority | Action |
|----|-------|----------|----------|--------|
| #101 | Fix `ruff` linting errors (I001, F841, F541) | Tech Debt | P1 | Fix auto & manual |
| #102 | Fix `PLC0415` Import outside top-level in `extractor.py` | Tech Debt | P1 | Move import to top |
| #103 | Fix `PLR0913` Too many arguments in `_save_checkpoint` | Tech Debt | P1 | Refactor or Suppress |
| #104 | Verify SSRF Protection | Security | P0 | Run Security Tests |
| #105 | Fix unused variables in tests (`F841`) | Tech Debt | P1 | Remove or prefix with _ |
| #106 | Fix f-string without placeholders in `test_daily_export.py` | Tech Debt | P1 | Remove f-prefix |

## Priority P2 - Features & Improvements

| ID | Title | Category | Priority | Action |
|----|-------|----------|----------|--------|
| #201 | Dashboard: Coverage Heatmap | Feature | P2 | Implement in `index.html` |
| #202 | Dashboard: Extraction Stats | Feature | P2 | Implement in `index.html` |
| #203 | Dashboard: Backfill Progress Bar | Feature | P2 | Implement in `index.html` |
| #204 | Dashboard: React + Tailwind Migration | Feature | P2 | Rewrite `index.html` |

## Priority P3 - Documentation

| ID | Title | Category | Priority | Action |
|----|-------|----------|----------|--------|
| #301 | Audit README.md vs CLI | Doc | P3 | Verify & Update |
| #302 | Create CHANGELOG.md | Doc | P3 | Create |

## Notes
- **Security:** Path traversal was previously fixed. SSRF is the current focus.
- **Performance:** No specific P0 performance bugs identified in initial scan, but lint fixes may improve code quality.
