"""
Baliza CLI - PNCP Data Extraction to Parquet

Simplified CLI focused solely on data extraction with smart defaults.
Default behavior: Extract ALL historical PNCP data (backfill everything).
"""

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

from .extraction.pipeline import (
    pncp_source, 
    run_priority_extraction,
    run_structured_extraction,
    create_default_pipeline,
    get_completed_extractions
)
from .extraction.gap_detector import find_extraction_gaps
from .settings import settings

app = typer.Typer(
    name="baliza",
    help="🚀 PNCP Data Extraction to Parquet Files",
    rich_markup_mode="rich"
)
console = Console()


@app.command()
def extract(
    # Smart date options (pick one) - DEFAULT: backfill everything
    backfill_all: bool = typer.Option(
        True, 
        "--backfill/--no-backfill", 
        help="Extract all available historical data (default)"
    ),
    days: Optional[int] = typer.Option(
        None, 
        "--days", "-d", 
        help="Extract last N days (overrides backfill)"
    ),
    date_input: Optional[str] = typer.Option(
        None, 
        "--date", 
        help="Date (YYYY-MM-DD, YYYY-MM) (overrides backfill)"
    ),
    date_range: Optional[str] = typer.Option(
        None, 
        "--range", "-r", 
        help="Date range (YYYY-MM:YYYY-MM) (overrides backfill)"
    ),
    
    # Data selection
    types: str = typer.Option(
        "all", 
        "--types", "-t", 
        help="Data types: all,contracts,publications,agreements"
    ),
    
    # Output options  
    output: Path = typer.Option(
        "data/", 
        "--output", "-o", 
        help="Output directory"
    ),
    
    # Utility flags
    verbose: bool = typer.Option(
        False, 
        "--verbose", "-v", 
        help="Verbose output"
    ),
    dry_run: bool = typer.Option(
        False, 
        "--dry-run", 
        help="Show what would be extracted"
    )
):
    """
    Extract PNCP data to Parquet files. 
    
    [bold green]By default, backfills all available historical data.[/bold green]
    
    Examples:
      baliza extract                    # Extract ALL historical data (default)
      baliza extract --days 30         # Last 30 days only  
      baliza extract --date 2025-01    # January 2025 only
      baliza extract --types contracts # All historical contracts
      baliza extract --dry-run         # See what would be extracted
    """
    
    # Determine date range based on options
    start_date, end_date = _parse_date_options(
        backfill_all, days, date_input, date_range
    )
    
    # Parse data types
    endpoints = _parse_data_types(types)
    
    # Show extraction plan
    _show_extraction_plan(start_date, end_date, endpoints, output, dry_run)
    
    if dry_run:
        console.print("✅ Dry run completed - no data extracted")
        return
    
    # Create output directory
    output.mkdir(parents=True, exist_ok=True)
    
    # Run extraction
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        task = progress.add_task("🔄 Extracting PNCP data...", total=None)
        
        try:
            # Run structured extraction with completion tracking
            result = run_structured_extraction(
                start_date=start_date,
                end_date=end_date,
                endpoints=endpoints,
                output_dir=str(output),
                skip_completed=True
            )
            
            progress.update(task, description="✅ Extraction completed!")
            
            # Show results
            _show_extraction_results(result, output)
            
        except Exception as e:
            progress.update(task, description="❌ Extraction failed!")
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)


@app.command()
def info():
    """Show information about available data types and configuration."""
    
    console.print("📊 [bold]PNCP Data Extraction Information[/bold]")
    console.print()
    
    # Data types table
    table = Table(title="Available Data Types")
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Description", style="white")
    table.add_column("PNCP Endpoint", style="dim")
    
    table.add_row("contracts", "Contract data", "contratos")
    table.add_row("publications", "Contract publications", "contratacoes_publicacao") 
    table.add_row("agreements", "Agreement records", "atas")
    table.add_row("updates", "Update sync endpoints", "*_atualizacao")
    table.add_row("proposals", "Proposal data", "contratacoes_proposta")
    table.add_row("charges", "Collection instruments", "instrumentoscobranca_inclusao")
    table.add_row("pca", "Annual contracting plans", "pca*")
    table.add_row("details", "Specific contract details", "contratacao_especifica")
    table.add_row("all", "ALL 12 data types (default)", "All PNCP endpoints")
    
    console.print(table)
    console.print()
    
    # Configuration info
    console.print("⚙️  [bold]Configuration[/bold]")
    console.print(f"  PNCP API Base URL: {settings.pncp_api_base_url}")
    console.print(f"  Default Date Range: {settings.default_date_range_days} days")
    console.print()
    
    # Usage examples
    console.print("💡 [bold]Quick Start Examples[/bold]")
    console.print("  baliza extract                    # Extract ALL historical data")
    console.print("  baliza extract --days 7           # Last week only")  
    console.print("  baliza extract --types contracts  # All historical contracts")
    console.print("  baliza extract --dry-run          # Preview what would be extracted")


@app.command()
def version():
    """Show version information."""
    # TODO: Get version from pyproject.toml dynamically instead of hardcoding.
@app.command()
def status(
    output: Path = typer.Option(
        "data/", 
        "--output", "-o", 
        help="Output directory to check"
    )
):
    """Show status of completed extractions."""
    
    console.print("📊 [bold]Extraction Status[/bold]")
    console.print()
    
    completed = get_completed_extractions(str(output))
    
    if not completed:
        console.print("❌ No completed extractions found")
        console.print(f"   Check output directory: {output}")
        return
    
    # Create status table
    table = Table(title="Completed Extractions")
    table.add_column("Endpoint", style="cyan", no_wrap=True)
    table.add_column("Months Completed", style="green")
    table.add_column("Total", style="white", justify="center")
    
    total_months = 0
    for endpoint, months in completed.items():
        months_str = ", ".join(sorted(months)[:3])  # Show first 3 months
        if len(months) > 3:
            months_str += f" +{len(months) - 3} more"
        
        table.add_row(endpoint, months_str, str(len(months)))
        total_months += len(months)
    
    console.print(table)
    console.print()
    console.print(f"✅ [bold green]{len(completed)} endpoints[/bold green] with [bold green]{total_months} months[/bold green] completed")
    console.print(f"📁 Output directory: {output}")


    #       This can be done using `importlib.metadata` or by reading the file.
    console.print("🚀 [bold]Baliza PNCP Data Extractor[/bold]")
    try:
        from importlib.metadata import version
        baliza_version = version("baliza")
    except ImportError:
        baliza_version = "2.0.0-dev"
    console.print(f"   Version: {baliza_version}")
    console.print("   DLT-powered extraction pipeline")


# TODO: Consider moving these helper functions (_parse_date_options,
#       _parse_data_types, _show_extraction_plan, _show_extraction_results)
#       to a separate `baliza.utils.cli_helpers` module to keep `cli.py`
#       focused solely on command definitions and high-level logic.

def _parse_date_options(
    backfill_all: bool, 
    days: Optional[int], 
    date_input: Optional[str], 
    date_range: Optional[str]
) -> tuple[Optional[str], Optional[str]]:
    """Parse date options into start_date, end_date."""
    
    # If any specific option is provided, disable backfill
    if days or date_input or date_range:
        backfill_all = False
    
    if backfill_all:
        # Backfill everything - let gap detector determine range
        return None, None
    
    if days:
        # Last N days
        end_dt = date.today()
        start_dt = end_dt - timedelta(days=days)
        return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    
    if date_input:
        # Single date or month
        if len(date_input) == 7:  # YYYY-MM
            year, month = date_input.split("-")
            start_dt = date(int(year), int(month), 1)
            # Last day of month
            if int(month) == 12:
                end_dt = date(int(year) + 1, 1, 1) - timedelta(days=1)
            else:
                end_dt = date(int(year), int(month) + 1, 1) - timedelta(days=1)
        elif len(date_input) == 10:  # YYYY-MM-DD
            start_dt = end_dt = date.fromisoformat(date_input)
        else:
            raise typer.BadParameter(f"Invalid date format: {date_input}")
        
        return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    
    if date_range:
        # Date range YYYY-MM:YYYY-MM
        start_str, end_str = date_range.split(":")
        start_year, start_month = start_str.split("-")
        end_year, end_month = end_str.split("-")
        
        start_dt = date(int(start_year), int(start_month), 1)
        # Last day of end month
        if int(end_month) == 12:
            end_dt = date(int(end_year) + 1, 1, 1) - timedelta(days=1)
        else:
            end_dt = date(int(end_year), int(end_month) + 1, 1) - timedelta(days=1)
        
        return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    
    # Default: last 7 days
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=7)
    return start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")


def _parse_data_types(types: str) -> list[str]:
    """Parse data types string into endpoint list."""
    if types == "all":
        # Return ALL 12 endpoints - no phase restrictions!
        return settings.all_pncp_endpoints
    
    type_mapping = {
        "contracts": "contratos",
        "publications": "contratacoes_publicacao", 
        "agreements": "atas",
        "updates": ["contratacoes_atualizacao", "contratos_atualizacao", "atas_atualizacao"],
        "proposals": "contratacoes_proposta",
        "charges": "instrumentoscobranca_inclusao",
        "pca": ["pca", "pca_usuario", "pca_atualizacao"],
        "details": "contratacao_especifica"
    }
    
    endpoints = []
    for type_name in types.split(","):
        type_name = type_name.strip()
        if type_name in type_mapping:
            mapped = type_mapping[type_name]
            if isinstance(mapped, list):
                endpoints.extend(mapped)
            else:
                endpoints.append(mapped)
        else:
            raise typer.BadParameter(f"Unknown data type: {type_name}. Available: {', '.join(type_mapping.keys())}")
    
    return endpoints


def _show_extraction_plan(
    start_date: Optional[str], 
    end_date: Optional[str], 
    endpoints: list[str], 
    output: Path,
    dry_run: bool
):
    """Show extraction plan to user."""
    
    console.print("📋 [bold]Extraction Plan[/bold]")
    
    # Date range
    if start_date and end_date:
        console.print(f"  📅 Date Range: {start_date} to {end_date}")
    else:
        console.print("  📅 Date Range: [bold green]ALL HISTORICAL DATA (backfill)[/bold green]")
    
    # Endpoints
    endpoint_names = {
        "contratos": "Contracts",
        "contratacoes_publicacao": "Publications", 
        "atas": "Agreements"
    }
    
    endpoint_list = [endpoint_names.get(ep, ep) for ep in endpoints]
    console.print(f"  📊 Data Types: {', '.join(endpoint_list)}")
    
    # Output
    console.print(f"  📁 Output Directory: {output}")
    
    if dry_run:
        console.print("  🔍 [yellow]DRY RUN - No data will be extracted[/yellow]")
    
    console.print()


def _show_extraction_results(result, output: Path):
    """Show extraction results."""
    
    console.print("✅ [bold green]Extraction Completed![/bold green]")
    console.print()
    
    # TODO: Parse DLT's result object to show detailed metrics,
    #       like number of rows extracted, file sizes, etc.
    #       The current output is too generic.
    console.print(f"📁 Data saved to: {output}")
    console.print("📊 Run metrics available in DLT logs")
    
    # Show next steps
    console.print()
    console.print("💡 [bold]Next Steps[/bold]")
    console.print("  • Data is saved in Parquet format")
    console.print("  • Use pandas, polars, or DuckDB to analyze")
    console.print("  • Re-run with same parameters to get only new data (incremental)")


if __name__ == "__main__":
    app()