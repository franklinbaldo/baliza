## `pyproject.toml`: Pin dbt-duckdb Version

### Problem

The `dbt-duckdb` dependency in `pyproject.toml` is specified as `>=1.7.0`. While this is not strictly an error, it can lead to problems in the future if a new version of `dbt-duckdb` is released with breaking changes.

### Potential Solutions

1.  **Pin to a Specific Version**:
    *   Change the dependency to `==1.7.0` or a similar specific version. This will ensure that the project always uses a known-good version of the library.
    *   This is the safest approach, but it requires manually updating the version number when a new version is released.

2.  **Use a Compatible Release Specifier**:
    *   Change the dependency to `~=1.7.0`. This will allow updates to patch releases (e.g., `1.7.1`) but not to minor or major releases (e.g., `1.8.0` or `2.0.0`).
    *   This provides a good balance between stability and getting bug fixes.

### Recommendation

Use a compatible release specifier (`~=1.7.0`) for the `dbt-duckdb` dependency. This will ensure that the project remains compatible with future patch releases of `dbt-duckdb` while protecting it from potentially breaking changes in minor or major releases. The same should be applied to `dbt-core`.
