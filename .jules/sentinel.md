## 2024-05-23 - Prevent RCE in Dynamic Imports
**Vulnerability:** Arbitrary Code Execution via `load_pncp_config` and `_import_from_string`. The application allowed importing any python module specified in the YAML configuration file (e.g., `os.system`), which could lead to RCE if a malicious config file is loaded.
**Learning:** Dynamic imports based on user-controlled input (even indirectly via config files) are dangerous if not strictly validated.
**Prevention:** Implemented a strict whitelist in `_import_from_string` to only allow imports from the `baliza.*` namespace.

## 2024-05-24 - Prevent SQL Injection in CoverageTracker
**Vulnerability:** SQL Injection in `CoverageTracker.derive_window_candidates`. The `date_field` parameter was interpolated directly into a SQL query string without quoting or validation, allowing arbitrary SQL execution if exposed to user input.
**Learning:** Always use identifier quoting (or parameterized queries where applicable) for dynamic column/table names in SQL construction, even if the input currently comes from a trusted source (config defaults), as future changes might expose it.
**Prevention:** Applied `_quote_identifier` to `date_field` before interpolation in `src/baliza/state/coverage.py`.

## 2024-05-25 - Prevent SSRF in HTTP Clients
**Vulnerability:** Server-Side Request Forgery (SSRF) in `_SecureClient` and `_FallbackClient`. The application did not validate target IPs, allowing access to private networks (localhost, 127.0.0.1, LAN) via CLI arguments or configuration.
**Learning:** Checking URL schemes (`http`/`https`) is insufficient. DNS resolution must be performed to check the actual destination IP against private ranges.
**Prevention:** Implemented `_is_safe_url` helper in `src/baliza/cli.py` to resolve and validate hostnames before making requests, blocking private/loopback IPs by default.
