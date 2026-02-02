from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import duckdb
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..daily_exporter import DailyExporter
from ..extractor import PNCPExtractor
from ..tiers import FeatureTier, tier0, tier1, tier2
from ..utils import validate_identifier, validate_resource_path
from .commands.state import state_app

app = typer.Typer(help="Baliza - Simple PNCP extraction tool")
app.add_typer(state_app, name="state")
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
    db_path: Path = typer.Option(
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
        # Validate resource
        validate_resource_path(resource)

        # Parse dates
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

        # Extract data
        start_time = time.time()
        with PNCPExtractor(db_path, dataset) as extractor:
            result = extractor.extract(start_date, end_date, resource)
        duration = time.time() - start_time

        # Create summary
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="bold")
        grid.add_column(justify="right")

        grid.add_row("Rows Extracted:", f"{result['rows_extracted']:,}")
        grid.add_row("Pages:", f"{result['pages']:,}")
        grid.add_row("Date Range:", f"{start} to {end}")
        grid.add_row("Duration:", f"{duration:.1f}s")

        console.print(
            Panel(
                grid,
                title="[green]✓ Extraction Complete[/green]",
                border_style="green",
                expand=False,
            )
        )

    except Exception as e:
        console.print(f"[red]✗ Extraction failed: {e}")
        raise typer.Exit(1) from None


@app.command("export")
@tier1
def export(
    table: str = typer.Option(..., "--table", help="Table name to export"),
    output: Path = typer.Option(..., "--output", "-o", help="Output directory"),
    db_path: Path = typer.Option(Path("baliza.duckdb"), "--duckdb", "-d", help="DuckDB file"),
    dataset: str = typer.Option("baliza_raw", "--dataset", "-s", help="Dataset name"),
    date_col: str = typer.Option(
        "dataPublicacao", "--date-col", help="Date column for partitioning"
    ),
) -> None:
    """Export DuckDB table to Parquet files."""
    try:
        # Validate inputs used in SQL construction
        validate_identifier(table)
        validate_identifier(dataset)

        output.mkdir(parents=True, exist_ok=True)

        with console.status(
            f"[bold green]Exporting {dataset}.{table} to parquet...[/bold green]",
            spinner="dots",
        ):
            with duckdb.connect(str(db_path)) as con:
                # Simple export - dump everything to parquet
                parquet_file = output / f"{table}.parquet"
                con.execute(f"""
                    COPY {dataset}.{table} TO '{parquet_file}' (FORMAT PARQUET)
                """)

        console.print(f"[green]✓ Exported {dataset}.{table} to {parquet_file}")

    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}")
        raise typer.Exit(1) from None


@app.command("export-daily")
@tier1
def export_daily(
    date_str: str = typer.Option(
        ...,
        "--date",
        "-d",
        help="Date to export (YYYY-MM-DD)",
    ),
    output: Path = typer.Option(
        Path("data/daily"),
        "--output",
        "-o",
        help="Output directory (date subdirectory will be created)",
    ),
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        help="DuckDB file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name",
    ),
) -> None:
    """Export daily self-contained parquet package.

    Creates a date-specific directory with:
    - contratos.parquet (main contracts table)
    - orgaos.parquet (deduplicated organizations)
    - unidades.parquet (organizational units)
    - _metadata.json (schema version and stats)
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        exporter = DailyExporter(db_path, dataset)

        with console.status(
            f"[bold green]Exporting daily package for {date_str}...[/bold green]",
            spinner="dots",
        ):
            stats = exporter.export(target_date, output)

        # Show summary table
        table = Table(title=f"Daily Export: {date_str}")
        table.add_column("Table", style="cyan")
        table.add_column("Rows", justify="right")
        table.add_column("Size", justify="right")

        for name, info in stats["tables"].items():
            size_kb = info["file_size_bytes"] / 1024
            table.add_row(name, str(info["row_count"]), f"{size_kb:.1f} KB")

        console.print(table)
        console.print(f"\n[green]✓ Output: {output / date_str}/")

    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}")
        raise typer.Exit(1) from None


@app.command("buffer-stats")
@tier2
def buffer_stats(
    db_path: Path = typer.Option(
        Path("baliza.duckdb"),
        "--duckdb",
        "-d",
        help="DuckDB file",
    ),
    dataset: str = typer.Option(
        "baliza_raw",
        "--dataset",
        "-s",
        help="Dataset name",
    ),
) -> None:
    """Show buffer statistics for monitoring."""
    try:
        with PNCPExtractor(db_path, dataset) as extractor:
            stats = extractor.get_buffer_stats()

        console.print(Panel("[bold]Buffer Statistics[/bold]"))
        console.print(f"  Total rows in buffer: [cyan]{stats['total_rows']:,}[/cyan]")
        console.print(f"  Dates in buffer: [cyan]{stats['dates_in_buffer']}[/cyan]")
        console.print(f"  Dates uploaded to IA: [cyan]{stats['dates_uploaded_to_ia']}[/cyan]")
        console.print(f"  Pending checkpoints: [yellow]{stats['pending_checkpoints']}[/yellow]")

        if stats["rows_by_date"]:
            console.print("\n[bold]Rows by Date:[/bold]")
            table = Table()
            table.add_column("Date", style="cyan")
            table.add_column("Rows", justify="right")

            for dt, count in sorted(stats["rows_by_date"].items()):
                table.add_row(dt, f"{count:,}")

            console.print(table)

    except Exception as e:
        console.print(f"[red]✗ Failed to get stats: {e}")
        raise typer.Exit(1) from None


@app.command("tiers")
@tier2
def show_tiers() -> None:
    """Show available commands organized by their Feature Tier."""
    table = Table(title="Baliza Feature Tiers", box=None)
    table.add_column("Tier", justify="center")
    table.add_column("Command", style="cyan")
    table.add_column("Description")

    # Get all commands from app and state_app
    all_commands = []

    # Main app commands
    for command in app.registered_commands:
        tier = getattr(command.callback, "_tier", FeatureTier.TIER_3)
        all_commands.append((tier, command.name or command.callback.__name__, command.help or ""))

    # State app commands
    for command in state_app.registered_commands:
        tier = getattr(command.callback, "_tier", FeatureTier.TIER_3)
        all_commands.append((tier, f"state {command.name or command.callback.__name__}", command.help or ""))

    # Sort by tier value
    all_commands.sort(key=lambda x: x[0].name)

    current_tier = None
    for tier, name, help_text in all_commands:
        if tier != current_tier:
            if current_tier is not None:
                table.add_row("", "", "")
            table.add_row(f"{tier.icon} [bold]{tier.name}[/bold]", "", f"[dim]{tier.description}[/dim]")
            current_tier = tier

        table.add_row("", name, help_text)

    console.print(table)


if __name__ == "__main__":
    app()
