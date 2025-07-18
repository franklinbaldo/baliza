# ADR-004: End-to-End (E2E) Only Testing Strategy

**Status:** Accepted

**Context:**
BALIZA is an integration system. Its primary value lies in its ability to correctly interact with external systems (the PNCP API, the file system) and ensure the data pipeline works from end to end. Unit tests with mocks could provide a false sense of security, as they would not validate the actual integration.

**Decision:**
The project's testing strategy will focus **exclusively on End-to-End (E2E) tests**. There will be no unit or integration tests that use mocks to validate business logic. Validation will be performed by running the pipeline against the real PNCP API on a controlled scope (e.g., a single day of data).

**Consequences:**
*   **Positive:**
    *   **Maximum Confidence:** A successful E2E test proves that the system *actually* works under production-like conditions.
    *   **Detects Integration Issues:** API contract errors, unexpected data format changes, and authentication issues are caught immediately.
    *   **Simplified Maintenance:** Less test code to write and maintain, without the complexity of managing mocks.
    *   **Focus on Value:** Tests validate user workflows and business outcomes, not implementation details.
*   **Negative:**
    *   **Slowness:** E2E tests are inherently slower than unit tests.
    *   **Flakiness:** Tests depend on the availability and consistency of the external API and can fail for reasons outside our control (e.g., network instability). This will be mitigated with retry logic (`tenacity`).
    *   **Harder Debugging:** A failure in an E2E test can be more difficult to trace back to the exact root cause in the code.