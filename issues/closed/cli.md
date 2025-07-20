## `cli.py`

*   **Mixing of UI and Business Logic:** The `extract`, `transform`, and `load` commands contain a lot of UI-related code (e.g., `rich` progress bars, headers, panels) mixed with the core business logic. This makes the code harder to test and reuse.
*   **Monolithic `run` Command:** The `run` command is a large, monolithic function that orchestrates the entire ETL pipeline. This could be broken down into smaller, more manageable functions.
*   **Lack of Dependency Injection:** The `AsyncPNCPExtractor`, `transformer`, and `loader` are directly imported and used. This makes it difficult to swap out implementations for testing or other purposes.
*   **Global Shutdown Flag:** The `_shutdown_requested` flag is a global variable. This can make the code harder to reason about and test. It would be better to pass the shutdown signal through the application in a more explicit way.
*   **Async in a Sync-Looking CLI:** The use of `asyncio.run(main())` inside the `extract` command and other commands hides the asynchronous nature of the application from the top-level CLI. This can make it harder to compose and reason about the application's concurrency.
