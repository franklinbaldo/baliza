## Hardcoded Database Path

### Problem

Multiple modules in the application have a hardcoded path to the DuckDB database file. This includes:

*   `src/baliza/loader.py`
*   `src/baliza/pncp_writer.py`

This makes the application less flexible and harder to configure.

### Potential Solutions

1.  **Centralize Database Path in `config.py`**:
    *   Add a `db_path` attribute to the `Settings` class in `src/baliza/config.py`.
    *   Update all modules that access the database to get the path from `settings.db_path`.

### Recommendation

Centralize the database path in the `Settings` class. This will make the application more configurable and easier to maintain. All hardcoded instances of `DATA_DIR / "baliza.duckdb"` or similar should be replaced with a reference to the settings object.
