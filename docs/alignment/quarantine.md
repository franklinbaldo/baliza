# Test Quarantine Log

This document tracks all tests marked as SKIP or XFAIL.

## Active Quarantines

| Identifier | Status | Reason | Added | Expiry |
|---|---|---|---|---|
| `tests/step_defs/test_end_to_end_extraction.py::test_pipeline_is_resumable_and_idempotent` | SKIP | Timeout issue with `pytest-httpx` in test environment. | 2024-07-24 | 2026-05-01 |
| `tests/step_defs/test_resilience.py` (all scenarios) | SKIP | Feature logic (complex retries/error recovery) not yet fully implemented in `extractor.py`. | 2026-02-01 | 2026-05-01 |
| `tests/step_defs/test_state_management.py::test_show_history` | SKIP | `state history` command not yet implemented. | 2026-02-01 | 2026-05-01 |
| `tests/integration/test_pncp_real_api.py` (all tests) | SKIP | Requires live PNCP API access, often blocked in CI or unstable. | 2024-07-24 | 2026-05-01 |
| `tests/integration/test_pncp_api_simple.py::test_pncp_api_response_fields` | XFAIL | PNCP API instability (returns 400 for valid requests sometimes). | 2026-02-01 | 2026-05-01 |
| `tests/integration/test_pncp_api_simple.py::test_pncp_api_date_format` | XFAIL | PNCP API instability (returns 400 for valid requests sometimes). | 2026-02-01 | 2026-05-01 |

## Principles
- **Skip** = Not applicable/cannot run now.
- **XFail** = Known bug or missing feature; failure expected.
- All entries must have an expiry date. When reached, they must be re-evaluated or fixed.
