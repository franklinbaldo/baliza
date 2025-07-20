## `baliza/dbt_baliza/macros/generate_task_plan.sql` - ARCHITECTURAL ISSUE

**Status:** 🚨 CRITICAL ARCHITECTURAL FLAW - Hardcoded configuration duplication

### 🚨 Critical Issue:
*   **Hardcoded Endpoint Configuration:** ❌ CRITICAL - The `endpoints_config` CTE hardcodes endpoint information that duplicates `baliza/config/endpoints.yaml`. This creates a maintenance nightmare and violates DRY principle. 
   - **Impact:** Changes to endpoints require updates in multiple places
   - **Solution:** Implement dbt-external-tables or similar mechanism to read from endpoints.yaml
   - **Status:** Acknowledged in code comments but not yet fixed
*   **Hardcoded `modalidades` Values:** Similar to `endpoints.yaml`, the `modalidades` arrays within the hardcoded endpoint definitions contain hardcoded integer values. These should be dynamically loaded from the `ModalidadeContratacao` enum or a symbolic representation.
*   **Limited Date Granularity:** The `date_spine` CTE only generates monthly chunks. While this might be sufficient for the current use case, a more flexible macro would allow for different granularities (e.g., daily, weekly, yearly) to be specified as an argument.
*   **Implicit Dependency on `md5` Function:** The macro uses the `md5` function for generating `task_id`. While `md5` is generally available in DuckDB, relying on specific database functions within a macro can reduce its portability to other data warehouses.
*   **Lack of Error Handling for `start_date` and `end_date`:** The macro assumes that `start_date` and `end_date` are valid date strings. It does not include any error handling for invalid date formats.
*   **Redundant `plan_fingerprint` Default:** The `plan_fingerprint` column defaults to "unknown" if the variable is not provided. This should ideally be a `not_null` field, and the macro should ensure that a valid fingerprint is always provided.
*   **Mixing of Concerns:** The macro is responsible for both generating the task plan and defining the structure of the `task_plan` table. These concerns should be separated. The table structure should be defined in `planning_schema.yml`, and the macro should only focus on generating the data.
