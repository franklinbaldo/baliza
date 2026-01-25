# Feature-Goal Alignment Matrix

| Feature File | Primary Goal Supported | Status | Action Taken |
|---|---|---|---|
| `buffer_management.feature` | Data Preservation | ✅ | Kept, aligned with current architecture. |
| `checkpoint.feature` | Reliable Extraction | ✅ | **Rewritten** to match the current `PNCPExtractor` checkpoint implementation. |
| `daily_export.feature` | Accessible Data Format | ✅ | Aligned with the current `export-daily` command. |
| `end_to_end_extraction.feature`| Reliable Extraction | ✅ | Aligned with the current `extract` command. |
| `export.feature` | Accessible Data Format | ✅ | Aligned with the current `export` command. |
| `resilience.feature` | Reliable Extraction | ❌ | **Retired**. This feature tested obsolete error handling from the old `dlt` pipeline. |
| `verification.feature` | Reliable Extraction | ✅ | Aligned with the current `verify` command. |
