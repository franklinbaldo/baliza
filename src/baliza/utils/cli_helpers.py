"""
CLI helper functions for baliza commands.
Extracted from cli.py to improve modularity and separation of concerns.
"""

from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from rich.console import Console
from rich.table import Table

console = Console()


def parse_date_options(
    backfill_all: bool, 
    days: Optional[int], 
    date_input: Optional[str], 
    date_range: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """Parse date options into start_date, end_date."""
    if backfill_all:
        return None, None
    
    if date_range:
        try:
            start_str, end_str = date_range.split(":")
            # TODO: The `date_range` parsing currently assumes YYYYMMDD:YYYYMMDD.
            #       It should be more robust to handle YYYY-MM-DD:YYYY-MM-DD or
            #       YYYY-MM:YYYY-MM formats, similar to how `date_input` is handled.
            #       Also, the `ValueError` is caught, but the function should ideally
            #       raise a `typer.BadParameter` for consistency with `cli.py`.
            return start_str, end_str
        except ValueError:
            raise ValueError("Date range must be in format YYYYMMDD:YYYYMMDD")
    
    if date_input:
        return date_input, date_input
    
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
        "compras": ["contratacoes_publicacao"],
        "contratos": ["contratos"],
        "atas": ["atas"],
        "atualizacoes": ["contratacoes_atualizacao", "contratos_atualizacao", "atas_atualizacao"],
        "propostas": ["contratacoes_proposta"],
        "instrumentos": ["instrumentoscobranca_inclusao"],
        "pca": ["pca", "pca_usuario", "pca_atualizacao"],
        "especifica": ["contratacao_especifica"]
    }
    
    if not data_types:
        return {"endpoints": ["contratacoes_publicacao", "contratos", "atas"]}
    
    endpoints = []
    for data_type in data_types:
        if data_type in type_mapping:
            endpoints.extend(type_mapping[data_type])
        else:
            console.print(f"[yellow]Warning: Unknown data type '{data_type}' ignored[/yellow]")
    
    return {"endpoints": endpoints}


def show_extraction_plan(
    start_date: Optional[str], 
    end_date: Optional[str], 
    endpoints: List[str], 
    gaps_found: int = None
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
    
    console.print(table)


def show_extraction_results(result: Any, output_dir: str = None):
    """Display extraction results to user."""
    if result is None:
        console.print("✅ [green]No extraction needed - all data already exists[/green]")
        return
    
    # Basic result display
    console.print("🎉 [green]Extraction completed successfully![/green]")
    
    if hasattr(result, 'loads_ids'):
        console.print(f"   Load IDs: {len(result.loads_ids)}")
    
    if output_dir:
        console.print(f"   Output directory: {output_dir}")
    
    # Note: Enhanced metrics parsing could be added here when DLT result structure is better understood


def get_version_info() -> str:
    """Get baliza version information."""
    try:
        from importlib.metadata import version
        return version("baliza")
    except ImportError:
        return "2.0.0-dev"


def format_endpoint_list(endpoints: List[str]) -> str:
    """Format endpoint list for display."""
    if len(endpoints) <= 3:
        return ", ".join(endpoints)
    else:
        return f"{', '.join(endpoints[:3])} and {len(endpoints) - 3} more"