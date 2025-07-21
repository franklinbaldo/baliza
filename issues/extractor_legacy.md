## `extractor_legacy.py`: Architectural Mix and Legacy Code

### Problem

The `src/baliza/extractor_legacy.py` file is a collection of multiple, overlapping extraction implementations, which makes the codebase confusing and hard to maintain.

1.  **Multiple Architectures**: It contains at least three different extraction architectures:
    *   A "V2" task-driven architecture using a stateful control table.
    *   An older implementation of a dbt-driven architecture.
    *   A modern implementation of the dbt-driven architecture that correctly delegates to the `ExtractionCoordinator`.

2.  **Code Duplication**: There is significant code duplication with other modules, such as `cli.py` (the `stats` command) and `extraction_coordinator.py`.

3.  **Lack of Clarity**: It is not clear which parts of this file are still in use and which are purely legacy. This increases the cognitive load for developers trying to understand the system.

### Potential Solutions

1.  **Refactor and Remove Legacy Code**:
    *   The primary goal should be to remove the outdated extraction implementations from this file.
    *   The "V2" task-driven architecture and the older dbt-driven implementation should be removed.
    *   The `typer` CLI application in this file should be removed, as the main CLI is in `src/baliza/cli.py`.

2.  **Consolidate and Clarify**:
    *   The `AsyncPNCPExtractor` class in this file should be reviewed. If it's still needed, it should be stripped down to only the essential, non-legacy functionality.
    *   If the `extract_dbt_driven` method is the only part that's still relevant, consider moving it to a more appropriate location or refactoring the `AsyncPNCPExtractor` in `extractor.py` to include it.

3.  **Delete the File**:
    *   The best-case scenario is that this entire file can be deleted after ensuring that any of its essential functionality has been migrated to other, more modern parts of the application.

### Recommendation

Perform a careful refactoring to remove all the legacy code from this file. The ultimate goal should be to delete `extractor_legacy.py` entirely. This will significantly simplify the codebase, reduce confusion, and make the application easier to maintain. This process will involve:

1.  Identifying any essential logic in this file that is not present elsewhere.
2.  Migrating that logic to the appropriate modern modules (`extractor.py`, `extraction_coordinator.py`, `cli.py`).
3.  Deleting this file.
