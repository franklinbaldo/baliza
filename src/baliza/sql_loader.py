from __future__ import annotations

import string
from pathlib import Path


class SQLLoader:
    """Load and parameterize SQL files from disk."""

    def __init__(self, sql_root: Path | str = Path("sql")) -> None:
        self.sql_root = Path(sql_root)
        self._cache: dict[str, str] = {}

    def load(self, query_path: str, **params: str) -> str:
        """Load SQL file and substitute parameters with validation."""
        if query_path not in self._cache:
            full_path = self.sql_root / query_path
            if not full_path.exists():
                raise FileNotFoundError(f"SQL file not found: {query_path}")
            self._cache[query_path] = full_path.read_text(encoding="utf-8")

        template = string.Template(self._cache[query_path])
        try:
            return template.substitute(**params)
        except KeyError as exc:
            missing = exc.args[0]
            raise ValueError(f"Missing parameter: {missing}") from None
