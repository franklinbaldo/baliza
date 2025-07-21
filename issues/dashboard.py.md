## `dashboard.py`: Simulated Data in Dashboard

### Problem

The `Dashboard` class in `src/baliza/ui/dashboard.py` uses simulated or hardcoded data in some of its panels. This includes:

*   `_get_quick_insights`: The `total_value`, `top_agency`, and `top_agency_contracts` are hardcoded.
*   `_get_pipeline_health`: All the data in this panel is hardcoded.
*   `_create_data_sources_health`: Some of the API performance metrics are hardcoded.

This means that the dashboard does not currently provide a fully accurate picture of the system's status.

### Potential Solutions

1.  **Implement Real Data Fetching**:
    *   The simulated data needs to be replaced with real data fetched from the database or other sources.
    *   For example, the `_get_quick_insights` method should be updated to run queries against the dbt models (e.g., the gold-level tables) to get the total value of contracts, the top agency, etc.
    *   The `_get_pipeline_health` method should be updated to get information about the last dbt run, the number of models, etc. This could be done by reading dbt's `run_results.json` file or by storing metadata about dbt runs in the database.
    *   The API performance metrics could be calculated from the `psa.pncp_requests` table.

### Recommendation

Replace all simulated data in the dashboard with real, data-driven insights. This will make the dashboard much more useful and will provide a true reflection of the system's status. This will likely require adding new queries to the dashboard's data-fetching methods and potentially storing more metadata about the ETL process in the database.
