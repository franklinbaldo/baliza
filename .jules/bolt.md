## 2024-05-23 - Date Parsing Optimization
**Learning:** `datetime.fromisoformat()` in Python 3.11+ is significantly faster (up to ~75x) than iterating through `datetime.strptime()` with multiple format strings for ISO 8601 dates. It also natively handles "Z" and timezone offsets, which older versions or manual parsing often struggle with.
**Action:** When working with Python 3.11+ projects, always prioritize `datetime.fromisoformat()` for parsing ISO dates. Ensure fallbacks are kept only for non-standard formats.

## 2024-05-24 - Optimization Lifecycle
**Learning:** Received feedback that `CoverageTracker` optimization was "implemented or obsolete" ("Limpeza massiva..."). This indicates that even active-looking code might be deprecated or subject to pending refactors/cleanups.
**Action:** When optimizing state management code (`baliza.state`), check for open PRs or architecture tasks that might obsolete the current implementation.
