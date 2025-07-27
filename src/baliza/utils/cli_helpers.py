"""
CLI helper functions for baliza commands.
Extracted from cli.py to improve modularity and separation of concerns.
"""

from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from rich.console import Console
from rich.table import Table

console = Console()


def _normalize_date_format(date_str: str) -> str:
    """Normalize date string to YYYYMMDD format.

    Supports:
    - YYYYMMDD (already normalized)
    - YYYY-MM-DD (ISO format)
    - YYYY-MM (month format, uses first day)
    """
    date_str = date_str.strip()

    if len(date_str) == 8 and date_str.isdigit():
        return date_str  # Already YYYYMMDD

    if len(date_str) == 10 and date_str.count("-") == 2:
        # YYYY-MM-DD format
        return date_str.replace("-", "")

    if len(date_str) == 7 and date_str.count("-") == 1:
        # YYYY-MM format
        return date_str.replace("-", "") + "01"

    raise ValueError(f"Unsupported date format: {date_str}")


def parse_date_options(
    backfill_all: bool,
    days: Optional[int],
    date_input: Optional[str],
    date_range: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Parse date options into start_date, end_date."""
    # If any specific option is provided, disable backfill
    if days or date_input or date_range:
        backfill_all = False

    if backfill_all:
        return None, None

    if date_range:
        try:
            start_str, end_str = date_range.split(":")
            start_str = _normalize_date_format(start_str)
            end_str = _normalize_date_format(end_str)
            return start_str, end_str
        except ValueError as e:
            import typer

            raise typer.BadParameter(
                f"Date range must be in format YYYYMMDD:YYYYMMDD, YYYY-MM-DD:YYYY-MM-DD, or YYYY-MM:YYYY-MM. Error: {e}"
            )

    if date_input:
        # Handle month format (YYYY-MM) vs day format (YYYY-MM-DD)
        if len(date_input.strip()) == 7 and date_input.count("-") == 1:
            # Month format - extract entire month
            from calendar import monthrange

            year, month = date_input.split("-")
            start_date = f"{year}{month.zfill(2)}01"
            last_day = monthrange(int(year), int(month))[1]
            end_date = f"{year}{month.zfill(2)}{last_day:02d}"
            return start_date, end_date
        else:
            # Day format or YYYYMMDD
            normalized_date = _normalize_date_format(date_input)
            return normalized_date, normalized_date

    if days:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

    # Default: last 30 days
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def parse_data_types(data_types: Optional[List[str]]) -> Dict[str, List[int]]:
    """Parse data types into endpoint configuration."""
    type_mapping = {
        "all": [
            "contratacoes_publicacao",
            "contratos",
            "atas",
            "contratacoes_atualizacao",
            "contratos_atualizacao",
            "atas_atualizacao",
            "contratacoes_proposta",
            "instrumentoscobranca_inclusao",
            "pca",
            "pca_usuario",
            "pca_atualizacao",
        ],
        "compras": ["contratacoes_publicacao"],
        "contratos": ["contratos"],
        "atas": ["atas"],
        "atualizacoes": [
            "contratacoes_atualizacao",
            "contratos_atualizacao",
            "atas_atualizacao",
        ],
        "propostas": ["contratacoes_proposta"],
        "instrumentos": ["instrumentoscobranca_inclusao"],
        "pca": ["pca", "pca_usuario", "pca_atualizacao"],
        # "especifica": ["contratacao_especifica"]  # Disabled: requires specific params
    }

    if not data_types:
        return {"endpoints": ["contratacoes_publicacao", "contratos", "atas"]}

    endpoints = []
    for data_type in data_types:
        if data_type in type_mapping:
            endpoints.extend(type_mapping[data_type])
        else:
            import typer

            available_types = ", ".join(type_mapping.keys())
            raise typer.BadParameter(
                f"Unknown data type '{data_type}'. Available types: {available_types}"
            )

    return {"endpoints": endpoints}


def show_extraction_plan(
    start_date: Optional[str],
    end_date: Optional[str],
    endpoints: List[str],
    gaps_found: int = None,
    output_dir: Optional[str] = None,
):
    """Display extraction plan to user."""
    table = Table(title="📋 Extraction Plan")
    table.add_column("Parameter", style="cyan")
    table.add_column("Value", style="green")

    if start_date and end_date:
        table.add_row("Date Range", f"{start_date} to {end_date}")
    else:
        table.add_row("Mode", "Complete Historical Backfill")

    table.add_row("Endpoints", ", ".join(endpoints))

    if gaps_found is not None:
        table.add_row("Data Gaps Found", str(gaps_found))

    # Display output directory in the plan
    if output_dir:
        table.add_row("Output Directory", output_dir)

    console.print(table)


def show_extraction_results(result: Any, output_dir: str = None):
    """Display extraction results to user."""
    if result is None:
        console.print(
            "✅ [green]No extraction needed - all data already exists[/green]"
        )
        return

    # Basic result display
    console.print("🎉 [green]Extraction completed successfully![/green]")

    if hasattr(result, "loads_ids"):
        console.print(f"   Load IDs: {len(result.loads_ids)}")

    if output_dir:
        console.print(f"   Output directory: {output_dir}")

    # TODO: Leverage DLT's `LoadInfo` object (the `result` parameter) to provide more detailed
    #       metrics and insights from the extraction. This includes information about
    #       loaded rows, file sizes, and any errors encountered during the load phase.
    #       Refer to DLT documentation on `LoadInfo` for available attributes.


def get_version_info() -> str:
    """Get baliza version information."""
    try:
        from importlib.metadata import version

        return version("baliza")
    except ImportError:
        return "2.0.0-dev"


def format_endpoint_list(endpoints: List[str]) -> str:
    """Format endpoint list for display."""
    # TODO: This function provides a basic formatting for endpoint lists.
    #       Consider using a more robust or configurable method for formatting
    #       lists, especially if the number of endpoints grows or if different
    #       display styles are required (e.g., bullet points, numbered lists).
    if len(endpoints) <= 3:
        return ", ".join(endpoints)
    else:
        return f"{', '.join(endpoints[:3])} and {len(endpoints) - 3} more"
