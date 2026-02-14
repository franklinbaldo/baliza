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

## 2024-05-26 - Prevent SQL Injection in DuckDB Commands
**Vulnerability:** SQL Injection in `baliza export` (file path) and `baliza status` (schema identifier). The `export` command used user-supplied file paths directly in a `COPY ... TO '...'` SQL string, allowing escape via single quotes. The `status` command used user-supplied dataset names directly in SQL without validation.
**Learning:** Even when using "local" databases like DuckDB, file paths and identifiers used in SQL strings must be escaped or validated. Standard parameterized queries (`?`) are not supported for identifiers or utility command arguments like filenames in `COPY`.
**Prevention:**
1. Use `validate_identifier` for all schema/table names.
2. Use `escape_sql_literal` (doubling single quotes) for string literals that cannot be parameterized (like file paths in `COPY`).

## 2024-05-27 - Prevent Credential Leakage in Logs
**Vulnerability:** Sensitive Credential Exposure in Logs via Exception Messages. The `scrub_url_params` utility only scrubbed query parameters for `http/https` URLs. Connection strings (e.g., `postgres://user:pass@host/db`, `s3://bucket/file?token=...`) in exception messages could leak credentials in logs if a connection error occurred.
**Learning:** Security scrubbing must be robust against diverse URL schemes and authentication methods (authority-based credentials vs query params), especially in a tool that supports multiple backends (DuckDB, S3, etc.).
**Prevention:** Enhanced `scrub_url_params` in `src/baliza/utils.py` to support generic schemes (`[a-z][a-z0-9+.-]*://`) and scrub both authority credentials (`user:pass@`) and query parameters.

## 2026-02-09 - Enhance SSRF Protection with is_global
**Vulnerability:** Incomplete SSRF protection in `validate_url` due to reliance on a block-list (`is_private`, `is_loopback`, `is_link_local`). This allowed potential access to Reserved IPs (e.g., `240.0.0.0/4`) and Multicast IPs (`224.0.0.0/4`), which are not technically private but unsafe for SSRF contexts.
**Learning:** Security controls based on "allow-lists" (what is explicitly safe, e.g., `is_global`) are safer than "block-lists" (what is known bad), as they handle edge cases and future protocol changes more robustly.
**Prevention:** Replaced manual checks with strict `is_global` enforcement in `src/baliza/utils.py` and blocked multicast addresses explicitly.

## 2026-02-10 - Prevent DNS Rebinding in HTTP Requests
**Vulnerability:** TOCTOU vulnerability in SSRF protection where `validate_url` checked an IP, but `httpx` re-resolved the hostname, allowing DNS Rebinding attacks against internal HTTP services.
**Learning:** Validating a hostname's IP is insufficient if the HTTP client performs its own resolution later. The validated IP must be the one used for the connection.
**Prevention:** Implemented `secure_url_connection_params` in `src/baliza/utils.py` which resolves the IP, validates it, and rewrites the URL to use the IP directly (setting the Host header) for HTTP requests.

## 2026-02-11 - Prevent Mixed-IP DNS Rebinding in SSRF Protection
**Vulnerability:** The SSRF protection mechanism `_resolve_safe_ip` only validated the first IP address returned by `getaddrinfo`. This allowed a DNS Rebinding attack where a domain resolves to both a public (safe) IP and a private (unsafe) IP. If the public IP was returned first, the validation passed, but the subsequent HTTP request could connect to the private IP.
**Learning:** Checking only the first result of a DNS resolution is insufficient. Attackers can control the order or provide multiple records to bypass checks.
**Prevention:** Always iterate through **ALL** resolved IP addresses for a hostname. If **ANY** of them are unsafe (private/loopback/multicast), reject the entire request immediately.
