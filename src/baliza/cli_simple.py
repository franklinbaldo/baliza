"""Simplified CLI using direct extraction (no dlt)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console

from .extractor import PNCPExtractor
from .tiers import tier0

app = typer.Typer(help="Baliza - Simple PNCP extraction tool")
console = Console()


@app.command("extract")
@tier0
def extract(
    start: str = typer.Option(
        ...,
        "--start",
        help="Start date (YYYY-MM-DD)",
    ),
    end: str = typer.Option(
        ...,
        "--end",
        help="End date (YYYY-MM-DD)",
    ),
    duckdb: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="Path to DuckDB database file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name in DuckDB",
    ),
    resource: str = typer.Option(
        "contratos",
        "--resource",
        "-r",
        help="Resource to extract (contratos, etc.)",
    ),
) -> None:
    """Extract data from PNCP API to DuckDB.

    Simple extraction command without complex gap detection or resumability.
    Just fetches data from start to end date and saves to DuckDB.
    """
    try:
        # Parse dates
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

        # Extract data
        with PNCPExtractor(duckdb, dataset) as extractor:
            result = extractor.extract(start_date, end_date, resource)

        console.print(f"\n[green]✓ Extraction complete!")
        console.print(f"  Rows: {result['rows_extracted']}")
        console.print(f"  Pages: {result['pages']}")

    except Exception as e:
        console.print(f"[red]✗ Extraction failed: {e}")
        raise typer.Exit(1)


@app.command("export")
@tier0
def export(
    table: str = typer.Option(..., "--table", help="Table name to export"),
    output: Path = typer.Option(..., "--output", "-o", help="Output directory"),
    duckdb: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name"),
    date_col: str = typer.Option("dataPublicacao", "--date-col", help="Date column for partitioning"),
) -> None:
    """Export DuckDB table to Parquet files."""
    import duckdb

    try:
        output.mkdir(parents=True, exist_ok=True)

        with duckdb.connect(str(duckdb)) as con:
            # Simple export - dump everything to parquet
            parquet_file = output / f"{table}.parquet"
            con.execute(f"""
                COPY {dataset}.{table} TO '{parquet_file}' (FORMAT PARQUET)
            """)

        console.print(f"[green]✓ Exported {dataset}.{table} to {parquet_file}")

    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
