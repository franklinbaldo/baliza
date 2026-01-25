# Alignment Actions Log

### `README.md` Update
- **Decision:** The `README.md` was critically outdated and described a `dlt`-based pipeline that was removed from the project. This created a major disconnect for any user or contributor.
- **Action:** The entire `README.md` was rewritten to reflect the current, simplified architecture based on `httpx` and `DuckDB`. All command examples, feature descriptions, and installation instructions were updated to be accurate.

### BDD Feature Retirement
- **Decision:** The `resilience.feature` file was testing an error handling mechanism specific to the non-existent `dlt` pipeline. It was no longer relevant.
- **Action:** Deleted `resilience.feature` and its corresponding step definition file, `tests/step_defs/test_resilience_simple.py`, to remove obsolete tests.

### BDD Scenario Rewrite
- **Decision:** The existing `checkpoint.feature` did not accurately test the page-level checkpointing and resume-on-failure logic of the current `PNCPExtractor`.
- **Action:** Rewrote the scenario in `checkpoint.feature` to simulate a mid-extraction failure, verify the creation of a checkpoint, and confirm that a subsequent run resumes correctly. The step definitions in `tests/step_defs/test_checkpoint.py` were updated to implement this new, more accurate test.

### Project Goal Inference
- **Decision:** The project's goals needed to be explicitly stated to guide future alignment work.
- **Action:** Inferred the project's goals, non-goals, and primary users from the current codebase (`cli_simple.py`, `extractor.py`) and the simplified dependencies in `pyproject.toml`. Documented these in `docs/alignment/goals.md`.
