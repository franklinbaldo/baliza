# Test Quarantine Registry

This file tracks all tests that are temporarily disabled using `skip` or `xfail`.

| Test / Module | Reason for Quarantine | Reference | Added On | Expiry Date | Owner |
|---------------|-----------------------|-----------|----------|-------------|-------|
| `tests/integration/test_pncp_real_api.py` | **Timeout:** This test appears to make live API calls to the external PNCP service, causing the entire test suite to time out and fail in CI. It violates test isolation principles and must be rewritten to use mocks (`pytest-httpx`) before being re-enabled. | `docs/alignment/actions.md` | 2024-07-15 | 2024-08-15 | Baliza BDD Agent |
