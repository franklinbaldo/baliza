## 2024-05-23 - Date Parsing Optimization
**Learning:** `datetime.fromisoformat()` in Python 3.11+ is significantly faster (up to ~75x) than iterating through `datetime.strptime()` with multiple format strings for ISO 8601 dates. It also natively handles "Z" and timezone offsets, which older versions or manual parsing often struggle with.
**Action:** When working with Python 3.11+ projects, always prioritize `datetime.fromisoformat()` for parsing ISO dates. Ensure fallbacks are kept only for non-standard formats.

## 2024-05-24 - Dictionary setdefault Performance
**Learning:** `dict.setdefault(key, default_value)` evaluates `default_value` eagerly, even if the key exists. When the default value is expensive to create (like a large dictionary or list), this can cause a massive performance hit (observed 7.4x slowdown) in tight loops compared to `try/except KeyError` or explicit checks.
**Action:** Avoid `setdefault` with expensive default values in hot loops. Use `try/except KeyError` or `if key not in dict` pattern instead.
