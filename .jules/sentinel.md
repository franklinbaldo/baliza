## 2024-05-23 - Prevent RCE in Dynamic Imports
**Vulnerability:** Arbitrary Code Execution via `load_pncp_config` and `_import_from_string`. The application allowed importing any python module specified in the YAML configuration file (e.g., `os.system`), which could lead to RCE if a malicious config file is loaded.
**Learning:** Dynamic imports based on user-controlled input (even indirectly via config files) are dangerous if not strictly validated.
**Prevention:** Implemented a strict whitelist in `_import_from_string` to only allow imports from the `baliza.*` namespace.

## 2024-05-24 - Prevent SQL Injection in CoverageTracker
**Vulnerability:** SQL Injection in `CoverageTracker.derive_window_candidates`. The `date_field` parameter was interpolated directly into a SQL query string without quoting or validation, allowing arbitrary SQL execution if exposed to user input.
**Learning:** Always use identifier quoting (or parameterized queries where applicable) for dynamic column/table names in SQL construction, even if the input currently comes from a trusted source (config defaults), as future changes might expose it.
**Prevention:** Applied `_quote_identifier` to `date_field` before interpolation in `src/baliza/state/coverage.py`.

## 2024-05-25 - Prevent Path Traversal in Resource Extraction
**Vulnerability:** Path Traversal and Command Injection via `resource` parameter. The application used the `resource` string directly in URL construction (`base_url + "/" + resource`) and CLI suggestion strings. This could allow attackers to traverse paths (e.g., `../etc/passwd`) or inject shell commands if the suggestion was executed.
**Learning:** Even when consuming external APIs, input used to construct URLs or CLI commands must be strictly validated against an allowlist to prevent traversal and injection.
**Prevention:** Implemented `validate_resource_path` in `src/baliza/utils.py` enforcing `^[a-zA-Z0-9_\-/]+$` and rejecting `..`, and applied it in `PNCPExtractor` and CLI commands.

## 2024-05-26 - Prevent SSRF in Extractor Configuration
**Vulnerability:** Server-Side Request Forgery (SSRF) via `base_url`. The `PNCPExtractor` initialized with a user-supplied `base_url` did not validate the target, allowing potential access to internal network services (e.g., metadata services, localhost) if the configuration was compromised or user-controlled.
**Learning:** Logic checking for IP properties (e.g., `is_private`) inside a `try/except ValueError` block (intended to catch non-IPs) can mistakenly suppress security exceptions if the validation logic itself raises `ValueError`. Validation logic must be careful not to swallow its own security alerts.
**Prevention:** Implemented `validate_url` in `src/baliza/utils.py` which strictly validates URL schemes and blocks private/loopback IPs, with a strictly scoped exception handler and an environment variable bypass for testing.
