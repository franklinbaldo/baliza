# Alignment Actions Log

This document records significant alignment decisions and actions taken by the Baliza BDD Alignment Agent.

## 2026-02-11: BDD Alignment & Health Pass

### Action: Implement `state` subcommands in CLI
- **Reason:** Misalignment between `state_management.feature` and `cli_simple.py`. Feature expected `state show/gaps/history` while CLI had top-level `status/verify`.
- **Decision:** Added `state` Typer app group to `cli_simple.py`. Mapped `state show` to `status`, `state gaps` to `verify`, and added `state buffer` for `buffer-stats`.
- **Outcome:** Features and CLI are now aligned. Tests for these commands are now "honest" as they call the actual subcommands.

### Action: Implement Resilience Tests
- **Reason:** `resilience.feature` scenarios were entirely skipped with "Not implemented" messages, leaving a gap in core feature verification.
- **Decision:** Implemented `tests/step_defs/test_resilience.py` using `monkeypatch` to mock `httpx.Client.get`.
- **Outcome:** Both scenarios ("recovers from transient error" and "fails after multiple retries") are now passing, providing verified confidence in the pipeline's resilience.

### Action: English Translation & Legacy Cleanup
- **Reason:** Project standard is shifting to English (as per Masterplan), and legacy `dlt` references were still present in `README.md` and `ROADMAP.md`.
- **Decision:** Translated `README.md` and `ROADMAP.md` to English. Removed all `dlt` references.
- **Outcome:** Documentation is now accurate and consistent with the "Simple" (httpx + DuckDB) architecture.

### Action: Feature Categorization (Tiers & Smoke)
- **Reason:** Need for prioritized testing and clear feature hierarchy as per `feature-hierarchy.md`.
- **Decision:** Added `@tierN` tags to all feature files and `@smoke` tags to critical scenarios.
- **Outcome:** Enables targeted test runs (e.g., `pytest -m smoke`).

### Action: Honest Quarantine Update
- **Reason:** Previous `quarantine.md` was incomplete and outdated.
- **Decision:** Fully updated `quarantine.md` with all current skips and xfails, including metadata (Reason, Reference, Expiry). Moved `state history` to `xfail(strict=True)`.
- **Outcome:** Transparent and traceable test suite health.
