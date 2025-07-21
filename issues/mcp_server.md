## `mcp_server.py`: Hardcoded Parquet Table Mapping

### Problem

The `PARQUET_TABLE_MAPPING` dictionary in `src/baliza/mcp_server.py` is hardcoded. This makes it difficult to configure the server to use different Parquet files or a different directory structure without modifying the source code.

```python
# TODO: Move this to a configuration file (see ISSUES.md)
PARQUET_TABLE_MAPPING = {
    "contratos": "contratos/*.parquet",
    "atas": "atas/*.parquet",
    "contratacoes": "contratacoes/*.parquet",
    "pca": "pca/*.parquet",
    "instrumentos": "instrumentos/*.parquet",
    # Add other datasets as needed
}
```

### Potential Solutions

1.  **Move to a Configuration File**:
    *   Create a new configuration file (e.g., `mcp_config.yaml`) that defines the table mapping.
    *   Modify the `get_table_mapping` function to load this file.
    *   This would allow users to easily customize the table mapping without touching the code.

2.  **Add to `settings`**:
    *   Add a `parquet_table_mapping` attribute to the `Settings` class in `src/baliza/config.py`.
    *   The `get_table_mapping` function would then get the mapping from `settings.parquet_table_mapping`.
    *   This would keep all configuration in one place, but it might be less user-friendly than a dedicated YAML file if the mapping is complex.

### Recommendation

Move the Parquet table mapping to a separate `mcp_config.yaml` file. This provides the most flexibility and is the most user-friendly approach. The server should be updated to load this configuration file at startup.
