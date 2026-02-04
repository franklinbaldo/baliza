# Test Quarantine Log

This document tracks all tests marked as SKIP or XFAIL, in accordance with the project's quarantine policy.

## Tier 0: Critical Path 🔴

### Quarantine Entry 1
- **Identifier:** `tests/step_defs/test_end_to_end_extraction.py::test_pipeline_is_resumable_and_idempotent`
- **Status:** SKIP
- **Reason:** Persistent timeout issue with `pytest-httpx` in the test environment.
- **Reference:** `docs/MASTERPLAN.md` (Technical Debt)
- **Added On:** 2024-07-24
- **Expiry Date:** 2026-06-01
- **Owner:** Baliza BDD Agent

## Tier 1: Core Features 🟠

### Quarantine Entry 2
- **Identifier:** `tests/step_defs/test_state_management.py::test_show_history`
- **Status:** XFAIL (strict=True)
- **Reason:** The `state history` command is not yet implemented in the CLI.
- **Reference:** Feature Brief 002
- **Added On:** 2026-02-11
- **Expiry Date:** 2026-04-11
- **Owner:** Baliza BDD Agent

## Integration & Environment 🌍

### Quarantine Entry 3
- **Identifier:** `tests/integration/test_pncp_real_api.py` (All tests)
- **Status:** SKIP
- **Reason:** Requires live PNCP API access, which is flaky and often blocked in CI.
- **Reference:** `docs/alignment/quarantine.md`
- **Added On:** 2024-07-24
- **Expiry Date:** 2026-12-31
- **Owner:** Baliza BDD Agent

### Quarantine Entry 4
- **Identifier:** `tests/integration/test_pncp_api_simple.py::test_pncp_api_response_fields`
- **Status:** XFAIL (strict=False)
- **Reason:** PNCP API is unstable and occasionally returns 400 for valid requests.
- **Reference:** In-code comment in test file.
- **Added On:** 2026-02-11
- **Expiry Date:** 2026-05-11
- **Owner:** Baliza BDD Agent

### Quarantine Entry 5
- **Identifier:** `tests/integration/test_pncp_api_simple.py::test_pncp_api_date_format`
- **Status:** XFAIL (strict=False)
- **Reason:** PNCP API is unstable and occasionally returns 400 for valid requests.
- **Reference:** In-code comment in test file.
- **Added On:** 2026-02-11
- **Expiry Date:** 2026-05-11
- **Owner:** Baliza BDD Agent
