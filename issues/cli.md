## `cli.py`: Legacy `stats` Command

### Problem

The `stats` command in `src/baliza/cli.py` is marked as a legacy command and has some technical debt:

1.  **Direct Database Access**: It directly connects to the database using a hardcoded path (`BALIZA_DB_PATH`). This bypasses the service layer and makes the command brittle to changes in configuration or database schema.
2.  **Hardcoded Lock File Logic**: It manually handles the database lock file, which is inconsistent with the more modern, centralized locking mechanisms that should be handled by a database service.
3.  **Inconsistent with Modern Commands**: Newer commands like `extract` and `transform` use a service-oriented architecture (`get_cli_services`), which makes them more modular and maintainable. The `stats` command does not follow this pattern.

### Potential Solutions

1.  **Refactor `stats` Command**:
    *   Create a `get_stats()` method in a relevant service (e.g., a new `StatsService` or within the `DatabaseService`).
    *   This service method would encapsulate the database queries.
    *   The `stats` command would then call this service method, similar to how other commands operate.
    *   This would remove the direct database connection and lock file handling from the CLI layer.

2.  **Deprecate and Remove `stats` Command**:
    *   If the functionality of the `stats` command is covered by the `status` command or other parts of the dashboard, consider deprecating it.
    *   If it's still needed, the refactoring option is preferred.

### Recommendation

Refactor the `stats` command to use the service layer. This will improve maintainability, reduce code duplication, and make the CLI more consistent.
