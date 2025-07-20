# Analysis of `src/baliza/extractor_legacy.py`

This file contains the main logic for the data extraction process. It's a complex file with a lot of responsibilities.

## Architectural Issues

1.  **Monolithic Class:** The `AsyncPNCPExtractor` class is a "God object" that does too many things. It handles task planning, discovery, execution, reconciliation, and even dbt integration. This makes the class difficult to understand, maintain, and test.
2.  **Tight Coupling:** The `AsyncPNCPExtractor` class is tightly coupled to the `PNCPClient`, `PNCPTaskPlanner`, and `PNCPWriter` classes. This makes it difficult to reuse or replace any of these components.
3.  **Mixing Concerns:** The class mixes high-level orchestration logic with low-level implementation details. For example, the `_discover_tasks` method contains both the logic for fetching task metadata and for updating the database.
4.  **Inconsistent dbt Integration:** The dbt integration is not well-defined. The `extract_dbt_driven` method seems to be a separate workflow that duplicates some of the logic from the main `extract_data` method. The `_dbt_plan_tasks` and `_dbt_execute_tasks` methods are private, but they are called from `extract_dbt_driven`. This suggests that the dbt integration is not a first-class citizen in the architecture.
5.  **Lack of a Clear Data Model:** The code uses dictionaries to pass data between methods. This makes it difficult to understand the data flow and to ensure data consistency. A clear data model, using Pydantic or dataclasses, would improve the code's readability and robustness.

## Code Quality Issues

1.  **Hardcoded SQL Queries:** The SQL queries are hardcoded as strings. This makes them difficult to maintain and test. They should be moved to separate `.sql` files or a dedicated query module.
2.  **Complex Methods:** Many of the methods in the `AsyncPNCPExtractor` class are too long and complex. They should be broken down into smaller, more focused methods.
3.  **Lack of Comments:** The code lacks comments, especially in the more complex parts. This makes it difficult to understand the logic and the intent of the code.
4.  **Inconsistent Naming:** The naming of variables and methods is not always consistent. For example, some variables are in `snake_case` and others are in `camelCase`.
5.  **Noisy `setup_signal_handlers` method:** The `setup_signal_handlers` method does not have a docstring that explains what it does.

## Suggestions for Improvement

*   **Refactor `AsyncPNCPExtractor`:** Break down the `AsyncPNCPExtractor` class into smaller, more focused classes, each with a single responsibility. For example, you could have a `TaskPlanner`, a `TaskExecutor`, and a `TaskReconciler` class.
*   **Use Dependency Injection:** Use dependency injection to decouple the components of the system. This will make the code more modular, reusable, and testable.
*   **Define a Clear Data Model:** Use Pydantic or dataclasses to define a clear data model for the data that is passed between the components of the system.
*   **Improve dbt Integration:** Make the dbt integration a first-class citizen in the architecture. This could be done by creating a separate `dbt` module that encapsulates all the dbt-related logic.
*   **Externalize SQL Queries:** Move the SQL queries to separate `.sql` files or a dedicated query module.
*   **Refactor Complex Methods:** Break down the complex methods into smaller, more focused methods.
*   **Add Comments:** Add comments to the code to explain the logic and the intent of the code.
*   **Use Consistent Naming:** Use a consistent naming convention for variables and methods.
*   **Add docstrings:** Add docstrings to all methods to explain their purpose.

Overall, the `extractor_legacy.py` file is a good starting point, but it needs a significant refactoring to improve its architecture and code quality. The suggestions above will help to make the code more modular, maintainable, and easier to understand.

## Proposed Solutions

*   **Create a `Coordinator` class:**
    *   This class will be responsible for orchestrating the entire extraction process.
    *   It will be composed of a `TaskPlanner`, a `TaskExecutor`, and a `TaskReconciler`.
    *   It will use dependency injection to get the components it needs.
*   **Create a `TaskPlanner` class:**
    *   This class will be responsible for planning the extraction tasks.
    *   It will use a `PNCPTaskPlanner` to generate the tasks.
    *   It will store the tasks in the database.
*   **Create a `TaskExecutor` class:**
    *   This class will be responsible for executing the extraction tasks.
    *   It will use a `PNCPClient` to make requests to the PNCP API.
    *   It will use a `PNCPWriter` to store the results in the database.
*   **Create a `TaskReconciler` class:**
    *   This class will be responsible for reconciling the extraction tasks.
    *   It will update the status of the tasks in the database.
*   **Use Pydantic models for data:**
    *   Define Pydantic models for the tasks, responses, and other data structures.
    *   Use these models to pass data between the components of the system.
*   **Create a `dbt` module:**
    *   Create a `dbt` module that encapsulates all the dbt-related logic.
    *   Use this module to run dbt commands.
*   **Externalize SQL queries:**
    *   Move the SQL queries to separate `.sql` files.
    *   Use a query builder or an ORM to build the queries.
