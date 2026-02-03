# BDD Conversion Journal - 2026-02-12

## Tasks
- [ ] Refactor `db_path` fixture to `tests/conftest.py` for reuse across BDD tests.
- [ ] Replace `pytest-httpx` with `monkeypatch` in `tests/step_defs/test_end_to_end_extraction.py`.
- [ ] Implement `resilience.feature` step definitions in `tests/step_defs/test_resilience.py`.
- [ ] Implement run history tracking in `PNCPExtractor` to support resilience verification.
- [ ] Enable and verify end-to-end and resilience tests.

## Progress

### 2026-02-12
- Initializing the task.
- Analyzed `resilience.feature` and its unimplemented step definitions.
- Identified that `PNCPExtractor` has the `baliza_state.runs` table but does not populate it, which is required by the resilience tests.
- Found that `test_end_to_end_extraction.py` is skipped due to `pytest-httpx` issues.
