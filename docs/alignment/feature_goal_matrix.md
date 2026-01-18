# Feature-Goal Alignment Matrix

This document tracks the alignment of BDD features with the project's primary goals.

| Feature File                        | Primary Goal Supported       | Status | Action Taken / Notes                                                                                               |
| ----------------------------------- | ---------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------ |
| `tests/features/extraction.feature` | Reliable Data Extraction     | ✅     | **Expanded.** Now covers core CLI features from README: resumability, lookback, and gap detection.                   |
| `tests/features/export.feature`     | Accessibility for Analysis   | ✅     | **Expanded.** Now verifies creation of partitioned Parquet files (`ano=YYYY/mes=MM`), as documented in the README. |
| `tests/features/state_management.feature` | Reliable Data Extraction   | ✅     | Aligned. Crucial for ensuring resumability and reliability. No changes made.                                       |
| `tests/features/verification.feature` | Reliable Data Extraction   | ✅     | **Added.** Aligned. Covers the `verify` command for auditing data coverage. Previously missing from this matrix.    |
