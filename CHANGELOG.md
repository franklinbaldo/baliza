# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- **Dashboard:** Enhanced `docs/dashboard/index.html` with React + TailwindCSS, providing a modern interface for monitoring extraction status, backfill progress, and coverage heatmap.
- **Security:** Implemented SSRF protection in `PNCPExtractor` by validating `base_url` against private IP ranges (`baliza.utils.validate_url`).
- **Tests:** Added integration tests for SSRF protection (`tests/integration/test_security_ssrf.py`).

### Changed
- **Documentation:** Updated `README.md` to accurately reflect available CLI commands (`extract`, `verify`, `export-daily`, `buffer-stats`, `status`).
- **Tech Debt:** Fixed multiple linting errors (`ruff` rules I001, F841, F541, PLR0913, PLC0415) to improve code quality and maintainability.
- **Extractor:** Refactored `src/baliza/extractor.py` to fix import placement and suppress specific lint warnings where appropriate.

### Fixed
- Fixed unused variables in test files.
- Fixed f-string usage in `test_daily_export.py`.
- Fixed potential SQL injection risks by ensuring strict validation of identifiers and URLs.
