"""
Modern E2E test suite for DLT-based PNCP pipeline.
Tests the complete extraction pipeline using real DLT components.

FIXME: This entire test suite has been removed as it was disabled, outdated,
and relied on a legacy architecture. It needs to be completely rewritten from
scratch to test the current DLT-based pipeline effectively.

The new test suite should focus on:
1.  Testing the `dlt.sources.rest_api.rest_api_source` based pipeline.
2.  Using modern mocking techniques (e.g., `pytest-httpx`) for API responses.
3.  Validating data transformations and the behavior of the gap detector.
4.  Ensuring all tests are enabled and pass in CI.
"""
