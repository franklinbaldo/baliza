## `extractor.py` - HIGH PRIORITY REFACTORING NEEDED

**Status:** 🚨 NEEDS SIGNIFICANT REFACTORING - Complex methods and tight coupling

### 🚨 High Priority Issues:
*   **Complex Methods:** The `extract_data` and `extract_dbt_driven` methods are extremely long (300+ lines) and handle multiple phases. This violates the Single Responsibility Principle and makes testing/maintenance difficult.
*   **Tight Coupling:** Strong dependency on `PNCPWriter` and `PNCPTaskPlanner` makes the class hard to test and extend. Needs dependency injection.
*   **Mixed Concerns:** The class handles orchestration, data fetching, progress display, and database operations. Should be separated into focused components.

### 🔧 Medium Priority Issues:
*   **String-based State Management:** Task status uses strings instead of proper enums/state machine. Error-prone and unclear state transitions.
*   **UI Logic in Business Layer:** `_create_matrix_progress_table` belongs in a UI/presentation module, not the core extractor.
*   **Hardcoded SQL:** Many SQL queries embedded in the code. Should use a data access layer.

### 💾 Low Priority Issues:
*   **In-memory State:** Statistics held in memory instead of persisted. Affects resumability accuracy.

### 📈 Priority: HIGH  
This class is central to the application and its complexity makes it a maintenance bottleneck.
