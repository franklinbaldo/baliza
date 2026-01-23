# Feature → Goal Matrix

This document maps each BDD feature to its primary project goal, providing a clear line of sight from testable specifications to business value.

| Feature File                  | Primary Goal Supported      | Status | Action Taken / Notes                                                                                                                              |
| ----------------------------- | --------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `end_to_end_extraction.feature` | Reliable Data Extraction    | ✅     | Aligned. This feature directly tests the core extraction pipeline's ability to run, resume, and avoid duplicating data, which is central to reliability. |
| `export.feature`              | Data Accessibility          | ✅     | Aligned. This feature ensures the extracted data can be successfully exported to Parquet, making it accessible for downstream analytical use.      |
| `resilience.feature`          | Reliable Data Extraction    | ✅     | Aligned. This feature verifies that the extractor can gracefully handle API errors, a key component of a reliable pipeline.                         |
| `verification.feature`        | Reliable Data Extraction    | ✅     | Aligned. Although the `verify` command is not present in the simplified CLI, the *intent* of this feature (ensuring data integrity) is still valid.  |
