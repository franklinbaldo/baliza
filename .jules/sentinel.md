## 2024-05-23 - Prevent RCE in Dynamic Imports
**Vulnerability:** Arbitrary Code Execution via `load_pncp_config` and `_import_from_string`. The application allowed importing any python module specified in the YAML configuration file (e.g., `os.system`), which could lead to RCE if a malicious config file is loaded.
**Learning:** Dynamic imports based on user-controlled input (even indirectly via config files) are dangerous if not strictly validated.
**Prevention:** Implemented a strict whitelist in `_import_from_string` to only allow imports from the `baliza.*` namespace.

## 2024-05-24 - Prevent SQL Injection in CoverageTracker
**Vulnerability:** SQL Injection in `CoverageTracker.derive_window_candidates`. The `date_field` parameter was interpolated directly into a SQL query string without quoting or validation, allowing arbitrary SQL execution if exposed to user input.
**Learning:** Always use identifier quoting (or parameterized queries where applicable) for dynamic column/table names in SQL construction, even if the input currently comes from a trusted source (config defaults), as future changes might expose it.
**Prevention:** Applied `_quote_identifier` to `date_field` before interpolation in `src/baliza/state/coverage.py`.

## 2024-05-25 - Prevent Path Traversal in Resource URLs
**Vulnerability:** Path Traversal and SSRF risk in `PNCPExtractor.extract`. The `resource` parameter was used to construct URLs (`base_url + "/" + resource`) without validation, allowing traversal (`../`) or absolute paths (`http://evil.com`).
**Learning:** CLI arguments that are part of URL construction must be strictly validated, even if they look like simple identifiers.
**Prevention:** Implemented `validate_resource_path` in `src/baliza/utils.py` allowing only alphanumeric, `_`, `-`, `/`, and blocking traversal/absolute paths.
