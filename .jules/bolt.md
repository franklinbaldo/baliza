# Bolt's Journal

## 2024-05-23 - Dataclass Memory Optimization
**Learning:** Dataclasses without `slots=True` create a `__dict__` for every instance, consuming significant memory when thousands of objects are created (like `WindowPage` or `Window`).
**Action:** Always use `@dataclass(slots=True)` for high-cardinality data objects in Python 3.10+ to reduce memory footprint and improve access speed.
