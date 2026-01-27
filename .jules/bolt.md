## 2024-05-23 - Date Parsing Optimization
**Learning:** `datetime.fromisoformat()` in Python 3.11+ is significantly faster (up to ~75x) than iterating through `datetime.strptime()` with multiple format strings for ISO 8601 dates. It also natively handles "Z" and timezone offsets, which older versions or manual parsing often struggle with.
**Action:** When working with Python 3.11+ projects, always prioritize `datetime.fromisoformat()` for parsing ISO dates. Ensure fallbacks are kept only for non-standard formats.

## 2025-05-24 - PyArrow Bulk Insertion
**Learning:** Inserting data into DuckDB using `pyarrow.Table.from_pylist` and `con.register` + `INSERT INTO ... SELECT` is significantly faster (~250x) than iterating through Python lists and using `con.executemany`. The explicit schema in PyArrow handles nested JSON structures efficiently, and DuckDB can query the registered view using dot notation for structs.
**Action:** Always prefer PyArrow for bulk data ingestion into DuckDB from Python objects. Define an explicit schema to handle nested data and ensure type safety, but keep a fallback path for robustness against dirty data.
