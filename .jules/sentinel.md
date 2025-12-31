## 2024-05-23 - Prevent RCE in Dynamic Imports
**Vulnerability:** Arbitrary Code Execution via `load_pncp_config` and `_import_from_string`. The application allowed importing any python module specified in the YAML configuration file (e.g., `os.system`), which could lead to RCE if a malicious config file is loaded.
**Learning:** Dynamic imports based on user-controlled input (even indirectly via config files) are dangerous if not strictly validated.
**Prevention:** Implemented a strict whitelist in `_import_from_string` to only allow imports from the `baliza.*` namespace.

## 2024-05-24 - Prevent SQL Injection in CoverageTracker
**Vulnerability:** SQL Injection in `CoverageTracker.derive_window_candidates`. The `date_field` parameter was interpolated directly into a SQL query string without quoting or validation, allowing arbitrary SQL execution if exposed to user input.
**Learning:** Always use identifier quoting (or parameterized queries where applicable) for dynamic column/table names in SQL construction, even if the input currently comes from a trusted source (config defaults), as future changes might expose it.
**Prevention:** Applied `_quote_identifier` to `date_field` before interpolation in `src/baliza/state/coverage.py`.

## 2025-12-31 - Prevent SSRF in Fallback Client
**Vulnerability:** SSRF (Server-Side Request Forgery) in `_FallbackClient`. The fallback client (used when `httpx` is missing) used `urllib.request.urlopen` which automatically follows HTTP redirects. This could allow an attacker to access internal resources (like metadata services or local ports) by providing a URL that redirects to them.
**Learning:** Python's `urllib.request` follows redirects by default, whereas modern clients like `httpx` often do not. When implementing security controls on URLs (like scheme validation), you must also control redirect behavior to prevent bypasses.
**Prevention:** Implemented a `NoRedirectHandler` in `src/baliza/cli.py` to disable automatic redirects in the fallback client, aligning its behavior with the secure default of `httpx`.
