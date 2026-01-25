## 2024-05-23 - Date Parsing Optimization
**Learning:** `datetime.fromisoformat()` in Python 3.11+ is significantly faster (up to ~75x) than iterating through `datetime.strptime()` with multiple format strings for ISO 8601 dates. It also natively handles "Z" and timezone offsets, which older versions or manual parsing often struggle with.
**Action:** When working with Python 3.11+ projects, always prioritize `datetime.fromisoformat()` for parsing ISO dates. Ensure fallbacks are kept only for non-standard formats.

## 2024-05-24 - PyArrow for Bulk Insert
**Learning:** Replacing manual list-of-tuples construction + `executemany` with `pyarrow.Table.from_pylist(rows, schema=...)` + DuckDB `INSERT INTO ... SELECT ... FROM arrow_table` eliminates Python iteration overhead and object allocation for thousands of rows. Explicit PyArrow schemas are crucial to handle missing keys/structs safely without crashes or type inference errors.
**Action:** When inserting JSON-like data into DuckDB, prefer `pyarrow` over manual row iteration, but always define an explicit schema to handle sparse data.
