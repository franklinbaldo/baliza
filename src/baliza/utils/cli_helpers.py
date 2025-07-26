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
    
    if len(date_str) == 10 and date_str.count('-') == 2:
        # YYYY-MM-DD format
        return date_str.replace('-', '')
    
    if len(date_str) == 7 and date_str.count('-') == 1:
        # YYYY-MM format
        return date_str.replace('-', '') + '01'
    
    raise ValueError(f"Unsupported date format: {date_str}")


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
            # Support multiple date formats: YYYYMMDD, YYYY-MM-DD, YYYY-MM
            start_str = _normalize_date_format(start_str)
            end_str = _normalize_date_format(end_str)
            return start_str, end_str
        except ValueError as e:
            import typer
            raise typer.BadParameter(f"Date range must be in format YYYYMMDD:YYYYMMDD, YYYY-MM-DD:YYYY-MM-DD, or YYYY-MM:YYYY-MM. Error: {e}")
    
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
            import typer
            available_types = ", ".join(type_mapping.keys())
            raise typer.BadParameter(f"Unknown data type '{data_type}'. Available types: {available_types}")
    
    return {"endpoints": endpoints}


def show_extraction_plan(
    start_date: Optional[str], 
    end_date: Optional[str], 
    endpoints: List[str], 
    gaps_found: int = None,
    output_dir: Optional[str] = None
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
    # Basic endpoint list formatting for CLI display
    # Truncates long lists to keep output readable
    if len(endpoints) <= 3:
        return ", ".join(endpoints)
    else:
        return f"{', '.join(endpoints[:3])} and {len(endpoints) - 3} more"