#!/usr/bin/env python3
"""Generate a manifest.json summary for exported Parquet datasets."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Tuple

import duckdb


def _format_datetime(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[return-value]
    return str(value)


def _detect_date_columns(con: duckdb.DuckDBPyConnection, glob: str) -> List[str]:
    description = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?);",
        [glob],
    ).fetchall()
    columns = {row[0]: row[1] for row in description}

    preferred = [
        column
        for column in ("dataPublicacaoPncp", "dataAtualizacao")
        if column in columns
    ]
    if preferred:
        return preferred

    fallbacks = [
        name
        for name, dtype in columns.items()
        if "DATE" in dtype.upper() or "TIMESTAMP" in dtype.upper()
    ]
    return sorted(fallbacks)


def _compute_sha256(files: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for file_path in files:
        with file_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _validate_directory(path: Path) -> Tuple[Path, List[Path]]:
    if not path.exists():
        raise FileNotFoundError(f"Parquet directory not found: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Expected a directory with Parquet files: {path}")

    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {path}")
    return path, files


def build_manifest(parquet_dir: Path, dataset_version: str) -> Path:
    directory, files = _validate_directory(parquet_dir)
    glob = str(directory / "**/*.parquet")
    con = duckdb.connect()

    row_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?);",
        [glob],
    ).fetchone()[0]

    date_columns = _detect_date_columns(con, glob)
    min_data: str | None = None
    max_data: str | None = None
    if date_columns:
        if len(date_columns) == 1:
            expression = date_columns[0]
        else:
            expression = "COALESCE(" + ", ".join(date_columns) + ")"

        min_value, max_value = con.execute(
            f"SELECT MIN({expression}), MAX({expression}) FROM read_parquet(?);",
            [glob],
        ).fetchone()
        min_data = _format_datetime(min_value)
        max_data = _format_datetime(max_value)

    checksum = _compute_sha256(files)

    manifest = {
        "dataset_version": dataset_version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(row_count),
        "min_data": min_data,
        "max_data": max_data,
        "sha256": checksum,
        "parquet_files": [str(path.relative_to(directory)) for path in files],
    }

    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a manifest for Parquet exports")
    parser.add_argument(
        "--parquet-dir",
        required=True,
        type=Path,
        help="Directory containing Parquet files",
    )
    parser.add_argument(
        "--dataset-version",
        required=True,
        help="Semantic version or tag for the export batch",
    )
    args = parser.parse_args()

    manifest_path = build_manifest(args.parquet_dir, args.dataset_version)
    print(f"Manifest written to {manifest_path}")


if __name__ == "__main__":
    main()
