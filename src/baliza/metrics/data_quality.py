"""
Data quality metrics and checks for the Baliza pipeline.
"""

from typing import List, Any, Optional
from datetime import date
import ibis
from ibis import _


def check_null_values(
    con: ibis.backends.duckdb.Backend, table_name: str, column_name: str
) -> int:
    """Check for null values in a specified column of a table."""
    table = con.table(table_name)
    return table.filter(table[column_name].isnull()).count().execute()


def check_duplicate_values(
    con: ibis.backends.duckdb.Backend, table_name: str, column_name: str
) -> int:
    """Check for duplicate values in a specified column of a table."""
    table = con.table(table_name)
    return table.group_by(column_name).count().filter(_["count"] > 1).count().execute()


def check_accepted_values(
    con: ibis.backends.duckdb.Backend,
    table_name: str,
    column_name: str,
    accepted_values: List[Any],
) -> int:
    """Check if the values in a specified column are within a set of accepted values."""
    table = con.table(table_name)
    return table.filter(~table[column_name].isin(accepted_values)).count().execute()


def check_date_range(
    con: ibis.backends.duckdb.Backend,
    table_name: str,
    column_name: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> int:
    """Check if the dates in a specified column are within a given range."""
    table = con.table(table_name)
    if start_date:
        table = table.filter(table[column_name] >= start_date)
    if end_date:
        table = table.filter(table[column_name] <= end_date)
    return table.count().execute()
