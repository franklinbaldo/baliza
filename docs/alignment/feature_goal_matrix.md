# Baliza Feature-Goal Matrix

This document maps the key functionalities and tests in the Baliza project to the primary goals outlined in `docs/alignment/goals.md`. Since this project does not use BDD `.feature` files, this matrix is based on the CLI commands and the test suite.

| Feature / Test File | Primary Goal Supported | Status | Action Taken |
|---|---|---|---|
| `baliza extract` command | Reliable Data Extraction, Data Preservation | ✅ | - |
| `baliza backfill` command | Reliable Data Extraction, Data Preservation | ✅ | - |
| `baliza export` command | Accessibility for Analysis | ✅ | - |
| `baliza verify` command | Reliable Data Extraction | ✅ | - |
| `baliza state` commands | Reliable Data Extraction | ✅ | - |
| `tests/e2e/test_pncp_pipeline.py` | Reliable Data Extraction | ✅ | - |
| `tests/e2e/test_resumable_extraction.py` | Reliable Data Extraction | ✅ | - |
| `tests/integration/test_pncp_api_simple.py` | Reliable Data Extraction | ✅ | - |
| `tests/integration/test_pncp_real_api.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_cli_validation.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_coverage_tracker.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_dates.py` | Reliable Data Extraction, Accessibility for Analysis | ✅ | - |
| `tests/unit/test_export.py` | Accessibility for Analysis | ✅ | - |
| `tests/unit/test_gap_detector.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_state_manager.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_cli_security.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_dos_protection.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_dos_protection_httpx.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_security_config.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_security_coverage.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_security_coverage_fix.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_ssrf.py` | Reliable Data Extraction | ✅ | - |
| `tests/unit/test_upload_internet_archive.py` | Data Preservation | ✅ | - |
| `tests/unit/test_incremental_overrides.py`| Reliable Data Extraction | ✅ | - |
| `tests/unit/test_cli_verify.py` | Reliable Data Extraction | ✅ | - |

**Status Legend:**
- ✅: Aligned with a project goal.
- ⚠️: Partially aligned, may need review or improvement.
- ❌: Not aligned with a project goal.
