#!/usr/bin/env python3
"""Helper utility to standardize Baliza Parquet exports."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


def _build_command(args: argparse.Namespace) -> list[str]:
    command = [
        "baliza",
        "export",
        args.table,
        "--duckdb",
        str(args.duckdb),
        "--dataset",
        args.dataset,
        "--out",
        str(args.output_dir),
    ]

    if args.date_col:
        command.extend(["--date-col", args.date_col])
    if args.fallback_date_col:
        command.extend(["--fallback-date-col", args.fallback_date_col])

    if args.current_month:
        command.append("--current-month")
    if args.target_month:
        command.extend(["--target-month", args.target_month])

    return command


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Wrapper around `baliza export` ensuring consistent options "
            "across automation workflows."
        )
    )
    parser.add_argument("table", help="Target table name for the export command")
    parser.add_argument(
        "--duckdb",
        default=Path("baliza.duckdb"),
        type=Path,
        help="Path to the DuckDB database file",
    )
    parser.add_argument(
        "--dataset",
        default="baliza_raw",
        help="Dataset (schema) name inside DuckDB",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data") / "contratos",
        type=Path,
        help="Directory that will receive the exported Parquet files",
    )
    parser.add_argument(
        "--date-col",
        default="dataPublicacaoPncp",
        help="Primary date column used to partition exports",
    )
    parser.add_argument(
        "--fallback-date-col",
        default="dataAtualizacao",
        help="Secondary date column used when the primary date is missing",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--current-month",
        action="store_true",
        help="Export only the files for the current UTC month",
    )
    group.add_argument(
        "--target-month",
        help="Export data for a specific YYYY-MM month",
    )

    parsed = parser.parse_args()
    command = _build_command(parsed)

    print("Executing:", " ".join(shlex.quote(part) for part in command))
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as exc:
        return exc.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
