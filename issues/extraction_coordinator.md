## `extraction_coordinator.py`: Placeholder Implementation in `ExecutionPhase`

### Problem

The `_execute_single_task` method in the `ExecutionPhase` class of `src/baliza/extraction_coordinator.py` is currently a placeholder. It simulates task execution with an `asyncio.sleep(0.1)` and does not perform any real work.

This means that the core logic of the extraction process—actually fetching data from the PNCP API based on the task plan—is not implemented in this module.

### Potential Solutions

1.  **Implement the Task Execution Logic**:
    *   The `_execute_single_task` method needs to be implemented to perform the actual data extraction. This would likely involve:
        *   Getting the task details (e.g., endpoint, date range, page number) from the `task` dictionary.
        *   Making an HTTP request to the PNCP API using an HTTP client (like `httpx`).
        *   Putting the raw response data onto the `page_queue` to be processed by the writer worker.
        *   Handling API errors, retries, and rate limiting.
    *   This logic could be encapsulated in a new class or function to keep the `ExecutionPhase` class clean.

2.  **Inject a Task Executor**:
    *   A more modular approach would be to define a `TaskExecutor` protocol and inject an implementation into the `ExecutionPhase`.
    *   This would further separate the orchestration of task execution from the implementation of a single task's execution.

### Recommendation

Implement the task execution logic within the `_execute_single_task` method, possibly by calling out to a new, dedicated function or class that handles the details of the API request. This will complete the implementation of the dbt-driven extraction process.

Also, the hardcoded dates in `_generate_new_plan_from_existing` should be replaced with the actual `start_date` and `end_date` from the context.
