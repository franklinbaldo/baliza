from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

import duckdb


@dataclass
class ExportMetadata:
    """Metadata returned by :func:`export_parquet`."""

    duckdb_path: str
    dataset: str
    table: str
    output_dir: str
    date_column: str
    rows_exported: int
    partitions: list[dict[str, int]]
    partition_count: int
    start_date: Optional[str]
    end_date: Optional[str]

    def asdict(self) -> dict[str, object]:
        return {
            "duckdb_path": self.duckdb_path,
            "dataset": self.dataset,
            "table": self.table,
            "output_dir": self.output_dir,
            "date_column": self.date_column,
            "rows_exported": self.rows_exported,
            "partitions": self.partitions,
            "partition_count": self.partition_count,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }


def _normalize_date(value: Optional[date | datetime | str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _build_where_clause(
    column_identifier: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[str, list[str]]:
    conditions: list[str] = []
    params: list[str] = []

    if start_date and end_date:
        conditions.append(f"{column_identifier} BETWEEN ? AND ?")
        params.extend([start_date, end_date])
    else:
        if start_date:
            conditions.append(f"{column_identifier} >= ?")
            params.append(start_date)
        if end_date:
            conditions.append(f"{column_identifier} <= ?")
            params.append(end_date)

    if not conditions:
        return "", params

    where_clause = " WHERE " + " AND ".join(conditions)
    return where_clause, params


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def _resolve_date_column(
    con: duckdb.DuckDBPyConnection,
    dataset: str,
    table: str,
    primary: str,
    fallbacks: Iterable[str],
) -> str:
    table_exists = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)
        LIMIT 1
        """,
        [dataset, table],
    ).fetchone()
    if not table_exists:
        raise ValueError(f"Table '{dataset}.{table}' does not exist in DuckDB database")

    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)
        """,
        [dataset, table],
    ).fetchall()

    available = {row[0].lower(): row[0] for row in rows}
    candidates = [primary, *fallbacks]

    for candidate in candidates:
        normalized = candidate.lower()
        if normalized in available:
            return available[normalized]

    fallback_list = ", ".join(candidates)
    raise ValueError(
        f"None of the candidate date columns ({fallback_list}) exist in table '{dataset}.{table}'"
    )


def export_parquet(
    duckdb_path: Path | str,
    dataset: str,
    table: str,
    out_dir: Path | str,
    date_col: str,
    fallback_date_cols: Optional[Sequence[str]] = None,
    start_date: Optional[date | datetime | str] = None,
    end_date: Optional[date | datetime | str] = None,
) -> ExportMetadata:
    """Export a DuckDB table to a partitioned Parquet dataset."""

    duckdb_path_str = str(duckdb_path)
    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)
    destination = str(out_dir_path).replace("'", "''")

    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)

    candidates = list(fallback_date_cols or ["dataAtualizacao"])
    # Ensure the primary date column comes first and remove duplicates keeping order.
    seen = set()
    ordered_candidates: list[str] = []
    for candidate in [date_col, *candidates]:
        lower = candidate.lower()
        if lower in seen:
            continue
        seen.add(lower)
        ordered_candidates.append(candidate)

    with duckdb.connect(str(duckdb_path_str)) as con:
        con.execute(f"USE {_quote_identifier(dataset)}")
        resolved_date_col = _resolve_date_column(
            con=con,
            dataset=dataset,
            table=table,
            primary=ordered_candidates[0],
            fallbacks=ordered_candidates[1:],
        )

        table_identifier = _quote_identifier(table)
        date_identifier = _quote_identifier(resolved_date_col)

        where_clause, params = _build_where_clause(
            column_identifier=date_identifier,
            start_date=normalized_start,
            end_date=normalized_end,
        )
        params_tuple = tuple(params)

        copy_sql = f"""
        COPY (
            SELECT *,
                   EXTRACT(YEAR FROM {date_identifier}) AS ano,
                   EXTRACT(MONTH FROM {date_identifier}) AS mes
            FROM {table_identifier}
            {where_clause}
        ) TO '{destination}'
        (FORMAT PARQUET, PARTITION_BY (ano, mes), OVERWRITE_OR_IGNORE TRUE)
        """
        con.execute(copy_sql, params_tuple)

        count_query = f"SELECT COUNT(*) FROM {table_identifier}{where_clause}"
        rows_exported = int(con.execute(count_query, params_tuple).fetchone()[0])

        partitions_query = f"""
        SELECT DISTINCT
            CAST(EXTRACT(YEAR FROM {date_identifier}) AS INTEGER) AS ano,
            CAST(EXTRACT(MONTH FROM {date_identifier}) AS INTEGER) AS mes
        FROM {table_identifier}
        {where_clause}
        ORDER BY 1, 2
        """
        partition_rows = con.execute(partitions_query, params_tuple).fetchall()

    partitions = [{"ano": int(row[0]), "mes": int(row[1])} for row in partition_rows]

    return ExportMetadata(
        duckdb_path=duckdb_path_str,
        dataset=dataset,
        table=table,
        output_dir=str(out_dir_path),
        date_column=resolved_date_col,
        rows_exported=rows_exported,
        partitions=partitions,
        partition_count=len(partitions),
        start_date=normalized_start,
        end_date=normalized_end,
    )
