## 2024-05-23 - Date Parsing Optimization
**Learning:** `datetime.fromisoformat()` in Python 3.11+ is significantly faster (up to ~75x) than iterating through `datetime.strptime()` with multiple format strings for ISO 8601 dates. It also natively handles "Z" and timezone offsets, which older versions or manual parsing often struggle with.
**Action:** When working with Python 3.11+ projects, always prioritize `datetime.fromisoformat()` for parsing ISO dates. Ensure fallbacks are kept only for non-standard formats.

## 2024-05-23 - Date Formatting Optimization
**Learning:** Manual f-string construction (e.g., `f"{dt.year:04d}-{dt.month:02d}..."`) is significantly faster (~1.5x to ~3x) than `dt.strftime()` for fixed date formats.
**Action:** Use manual f-strings instead of `strftime` for fixed formats in hot paths or frequently called utility functions.
