from __future__ import annotations

import string
from pathlib import Path
from typing import Any


class SQLLoader:
    """Load and parameterize SQL files from disk."""

    def __init__(self, sql_root: Path | str = Path("sql")) -> None:
        self.sql_root = Path(sql_root)
        self._cache: dict[str, str] = {}

    def load(self, query_path: str, **params: Any) -> str:
        """
        Load a SQL file and substitute its parameters.

        This method reads a SQL file from the configured `sql_root` directory,
        caches its content, and then uses `string.Template` to safely substitute
        placeholders with the provided parameters.

        Args:
            query_path: The path to the SQL file, relative to `sql_root`.
            **params: Keyword arguments mapping placeholder names in the SQL
                      template to their substitution values.

        Returns:
            The SQL query with all placeholders substituted.

        Raises:
            FileNotFoundError: If the specified `query_path` does not exist.
            ValueError: If a parameter required by the template is not provided
                        in `params`.
        """
        if query_path not in self._cache:
            full_path = self.sql_root / query_path
            if not full_path.exists():
                raise FileNotFoundError(f"SQL file not found: {full_path}")
            self._cache[query_path] = full_path.read_text(encoding="utf-8")

        template = string.Template(self._cache[query_path])
        try:
            return template.substitute(**params)
        except KeyError as exc:
            missing_param = exc.args[0]
            raise ValueError(
                f"Missing required parameter '{missing_param}' for query '{query_path}'"
            ) from None
