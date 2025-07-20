# Analysis of `src/baliza/extraction_coordinator.py`

This file contains the `ExtractionCoordinator` class, which orchestrates the dbt-driven extraction process.

## Architectural Issues

1.  **Hardcoded SQL Queries:** The SQL queries are hardcoded as strings. This makes them difficult to maintain and test. They should be moved to separate `.sql` files or a dedicated query module.
2.  **Mixing Concerns:** The `ExtractionCoordinator` class mixes the logic for coordinating the extraction process with the logic for handling signals and generating reports. This violates the Single Responsibility Principle.
3.  **Noisy `logger` variable:** The `logger` variable is created but it is not used in the whole file.
4.  **Noisy `console` variable:** The `console` variable is created but it is not used in the whole file.
5.  **Noisy `WriterProtocol` protocol:** The `WriterProtocol` protocol does not have a docstring that explains what it does.
6.  **Noisy `ExtractionPhase` class:** The `ExtractionPhase` class does not have a docstring that explains what it does.
7.  **Noisy `PlanningPhase` class:** The `PlanningPhase` class does not have a docstring that explains what it does.
8.  **Noisy `ExecutionPhase` class:** The `ExecutionPhase` class does not have a docstring that explains what it does.

## Code Quality Issues

1.  **Bare `except Exception`:** The `_execute_task_batch` and `_execute_single_task` methods use a bare `except Exception`, which is too broad. It's better to catch more specific exceptions.
2.  **Long Methods:** Some of the methods in the `ExtractionCoordinator` class are too long and complex, such as `extract_dbt_driven`. They should be broken down into smaller, more focused methods.
3.  **Placeholder Implementation:** The `_execute_single_task` method is a placeholder implementation. It should be replaced with the actual task execution logic.
4.  **Inconsistent Naming:** The naming of variables and methods is not always consistent. For example, some variables are in `snake_case` and others are in `camelCase`.

## Suggestions for Improvement

*   **Externalize SQL Queries:** Move the SQL queries to separate `.sql` files or a dedicated query module.
*   **Separate Concerns:** Create separate classes for handling signals and generating reports. The `ExtractionCoordinator` class should then use these classes to perform its work.
*   **Remove unused variables:** The `logger` and `console` variables are not used anywhere in the file and should be removed.
*   **Add docstrings:** Add docstrings to all protocols and classes to explain their purpose.
*   **Specific Exception Handling:** Use more specific exception handling to provide better error messages and make the script more robust.
*   **Refactor Long Methods:** Break down the long methods into smaller, more focused methods.
*   **Implement `_execute_single_task`:** Implement the actual task execution logic in the `_execute_single_task` method.
*   **Use Consistent Naming:** Use a consistent naming convention for variables and methods.

Overall, the `extraction_coordinator.py` file is a good starting point, but it needs a significant refactoring to improve its architecture and code quality. The suggestions above will help to make the code more modular, maintainable, and easier to understand.

## Proposed Solutions

*   **Create a `database` module:**
    *   This module will be responsible for all database interactions.
    *   It will have a `Database` class with methods for connecting to the database, executing queries, and closing the connection.
    *   It will also have a `QueryBuilder` class for building SQL queries.
*   **Refactor `ExtractionCoordinator`:**
    *   The `ExtractionCoordinator` class will be responsible for orchestrating the extraction process.
    *   It will use the `PlanningPhase`, `ExecutionPhase`, and `ReportingPhase` classes to perform its work.
*   **Create a `PlanningPhase` class:**
    *   This class will be responsible for planning the extraction.
    *   It will use the `DbtRunner` to generate the task plan.
*   **Create an `ExecutionPhase` class:**
    *   This class will be responsible for executing the extraction tasks.
    *   It will use the `TaskClaimer` to claim tasks.
    *   It will use the `PNCPClient` to make requests to the PNCP API.
    *   It will use the `PNCPWriter` to store the results in the database.
*   **Create a `ReportingPhase` class:**
    *   This class will be responsible for generating the final report.
*   **Implement `_execute_single_task`:**
    *   Implement the actual task execution logic in the `_execute_single_task` method.
*   **Remove unused variables:**
    *   Remove the `logger` and `console` variables.
*   **Add docstrings and type hints to all classes and methods.**
