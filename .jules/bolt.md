# Bolt's Journal

## 2026-01-10 - httpx/requests caching behavior
**Learning:** `httpx.Response.json()` and `requests.Response.json()` do NOT cache their results by default. In pipelines where multiple components (like `dlt` and custom wrappers) both need to parse the same JSON response, this leads to double parsing which can be significant for large payloads (1MB+).
**Action:** Use a context manager to monkey-patch `Response.json` with a caching wrapper during the execution of such pipelines to ensure O(1) parsing cost per response.
