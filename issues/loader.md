## `loader.py`: Hardcoded Database Path

### Problem

The `export_to_parquet` function in `src/baliza/loader.py` has a hardcoded path to the DuckDB database file:

```python
db_path = DATA_DIR / "baliza.duckdb"
```

This makes the loader less flexible, as it can't be easily configured to use a database in a different location. This is inconsistent with the best practice of managing configuration through the `settings` object.

### Potential Solutions

1.  **Add `db_path` to Settings**:
    *   Add a `db_path` attribute to the `Settings` class in `src/baliza/config.py`.
    *   Update the `export_to_parquet` function to get the database path from `settings.db_path`.
    *   This would centralize the database path configuration and make the loader more flexible.

### Recommendation

Add the `db_path` to the `Settings` class and use it in the `export_to_parquet` function. This is a simple change that will improve the configuration management of the application.
