## 2024-05-23 - Prevent RCE in Dynamic Imports
**Vulnerability:** Arbitrary Code Execution via `load_pncp_config` and `_import_from_string`. The application allowed importing any python module specified in the YAML configuration file (e.g., `os.system`), which could lead to RCE if a malicious config file is loaded.
**Learning:** Dynamic imports based on user-controlled input (even indirectly via config files) are dangerous if not strictly validated.
**Prevention:** Implemented a strict whitelist in `_import_from_string` to only allow imports from the `baliza.*` namespace.

## 2024-05-24 - Prevent SQL Injection in CoverageTracker
**Vulnerability:** SQL Injection in `CoverageTracker.derive_window_candidates`. The `date_field` parameter was interpolated directly into a SQL query string without quoting or validation, allowing arbitrary SQL execution if exposed to user input.
**Learning:** Always use identifier quoting (or parameterized queries where applicable) for dynamic column/table names in SQL construction, even if the input currently comes from a trusted source (config defaults), as future changes might expose it.
**Prevention:** Applied `_quote_identifier` to `date_field` before interpolation in `src/baliza/state/coverage.py`.

## 2026-01-21 - Prevent SQL Injection in Export Command
**Vulnerability:** SQL Injection in `baliza export` command via the `--output` path. The file path was interpolated directly into the `COPY ... TO '...'` SQL statement using an f-string. A malicious path containing a single quote could break out of the string literal and execute arbitrary SQL.
**Learning:** File paths and other "trusted" local inputs must still be treated as untrusted values in SQL statements. DuckDB's Relation API (`con.table(...).to_parquet(...)`) avoids raw SQL entirely and is safer and cleaner than manual quoting or parameterization for this use case.
**Prevention:** Switched from `COPY` SQL statement to DuckDB Relation API in `src/baliza/cli_simple.py`.
