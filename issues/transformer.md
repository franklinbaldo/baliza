## `transformer.py`: Hardcoded dbt Project Directory and Inconsistent dbt Execution

### Problem

1.  **Hardcoded Project Directory**: The `transform` function in `src/baliza/transformer.py` has a hardcoded path to the dbt project directory:

    ```python
    process = subprocess.Popen(
        ["dbt", "run", "--project-dir", "dbt_baliza"],
        ...
    )
    ```

    This makes the transformer less flexible.

2.  **Inconsistent dbt Execution**: The application runs dbt in two different ways:
    *   In `task_claimer.py`, it uses `uv run dbt ...`.
    *   In `transformer.py`, it uses `dbt ...` directly.

    This inconsistency can lead to different behavior in different environments, especially if the user has multiple versions of dbt installed.

### Potential Solutions

1.  **Centralize dbt Project Path**:
    *   Add a `dbt_project_dir` attribute to the `Settings` class in `src/baliza/config.py`.
    *   Update all dbt-related commands to use this setting.

2.  **Unify dbt Execution**:
    *   Create a dedicated "dbt runner" utility that is responsible for running all dbt commands.
    *   This runner would encapsulate the logic for how to run dbt (e.g., using `uv run` or directly) and would get the project directory from the settings.
    *   All parts of the application that need to run dbt would use this runner.

### Recommendation

Create a `dbt_runner.py` module that provides a consistent way to run dbt commands. This runner should get the dbt project directory from the application settings. This will solve both the hardcoded path issue and the inconsistent execution issue.
