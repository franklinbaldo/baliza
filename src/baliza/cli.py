"""
Baliza CLI - PNCP Data Extraction to Parquet

Simplified CLI focused solely on data extraction with smart defaults.
Default behavior: Extract ALL historical PNCP data (backfill everything).
"""

import typer
import re
import warnings
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

# Suppress common DLT warnings for cleaner output
warnings.filterwarnings("ignore", message=".*psutil dependency is not installed.*")
# Note: DLT filesystem destination doesn't support merge disposition natively (only append/replace)
# We've removed primary_key from config to prevent DLT from trying to use merge mode
warnings.filterwarnings("ignore", message=".*merge.*write disposition.*cannot be met.*")

# Configure logging to suppress DLT warnings
import logging
logging.getLogger("dlt").setLevel(logging.ERROR)

from .extraction.pipeline import create_default_pipeline, pncp_source, pncp_monthly_sources
from .utils.completion_tracking import (
    get_completed_extractions,
    mark_extraction_completed,
)
from .settings import settings
from .utils.cli_helpers import (
    parse_date_options,
    parse_data_types,
    show_extraction_plan,
    show_extraction_results,
)

app = typer.Typer(
    name="baliza",
    help="🚀 PNCP Data Extraction to Parquet Files",
    rich_markup_mode="rich",
)
console = Console()


@app.command()
def extract(
    # Smart date options (pick one) - DEFAULT: backfill everything
    backfill_all: bool = typer.Option(
        True,
        "--backfill/--no-backfill",
        help="Extract monthly data from 2021-01 to present (default)",
    ),
    days: Optional[int] = typer.Option(
        None, "--days", "-d", help="Extract last N days (overrides backfill)"
    ),
    date_input: Optional[str] = typer.Option(
        None, "--date", help="Date (YYYY-MM-DD, YYYY-MM) (overrides backfill)"
    ),
    date_range: Optional[str] = typer.Option(
        None, "--range", "-r", help="Date range (YYYY-MM:YYYY-MM) (overrides backfill)"
    ),
    # Data selection
    types: str = typer.Option(
        "all", "--types", "-t", help="Data types: all,contracts,publications,agreements"
    ),
    # Output options
    output: Path = typer.Option("data/", "--output", "-o", help="Output directory"),
    # Progress tracking
    progress: str = typer.Option(
        "enlighten", "--progress", "-p", help="Progress tracking: enlighten, tqdm, alive_progress, log"
    ),
    # Utility flags
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be extracted"
    ),
):
    """
    Extract PNCP data to Parquet files.

    [bold green]By default, extracts monthly data from 2021-01 to present.[/bold green]

    Examples:
      baliza extract                           # Extract monthly from 2021-01 to present (default)
      baliza extract --days 30                # Last 30 days only
      baliza extract --date 2025-01           # January 2025 only
      baliza extract --types contracts        # All historical contracts
      baliza extract --progress tqdm          # Use tqdm progress bars
      baliza extract --progress log           # Log progress to console
      baliza extract --dry-run                # See what would be extracted
    """

    # Validate progress option
    valid_progress_options = ["enlighten", "tqdm", "alive_progress", "log"]
    if progress not in valid_progress_options:
        console.print(f"[red]Invalid progress option: {progress}[/red]")
        console.print(f"Valid options: {', '.join(valid_progress_options)}")
        raise typer.Exit(1)

    # Determine date range based on options
    start_date, end_date = parse_date_options(
        backfill_all, days, date_input, date_range
    )

    # Parse data types - split comma-separated string into list
    types_list = [t.strip() for t in types.split(",")] if types != "all" else ["all"]
    endpoints_config = parse_data_types(types_list)
    endpoints = endpoints_config["endpoints"]

    # Show extraction plan
    show_extraction_plan(start_date, end_date, endpoints)

    if dry_run:
        console.print("✅ Dry run completed - no data extracted")
        return

    # Create output directory
    output.mkdir(parents=True, exist_ok=True)

    # Run extraction
    try:
        console.print("🔄 Starting PNCP data extraction...")
        
        # Create DLT pipeline with built-in progress tracking
        pipeline = create_default_pipeline("parquet", str(output), progress)

        # Check if this is a backfill operation (use monthly sources for better isolation)
        if start_date is None and end_date is None:
            # Monthly backfill - each month is processed independently
            # TODO: Explore using DLT's incremental loading features (e.g., @dlt.incremental)
            # within pncp_monthly_sources to manage state and simplify date range generation,
            # potentially reducing the need for manual month iteration here.
            monthly_sources = pncp_monthly_sources(
                start_date=start_date, 
                end_date=end_date, 
                endpoints=endpoints,
                backfill_all=True
            )
            
            total_results = []
            failed_months = []
            
            # Count unique months vs total sources (month×modalidade combinations)
            unique_months = set()
            for source in monthly_sources:
                # Extract month from source name (e.g., "pncp_contratacoes_publicacao_20210101_mod1" -> "202101")
                if hasattr(source, 'name'):
                    source_name = source.name
                    # Extract YYYYMMDD and convert to YYYYMM
                    match = re.search(r'_(\d{8})_', source_name)
                    if match:
                        date_str = match.group(1)
                        month_str = date_str[:6]  # YYYYMM
                        unique_months.add(month_str)
            
            console.print(f"📊 Processing {len(unique_months)} months ({len(monthly_sources)} sources total)")
            
            # Use Rich progress bar for source processing
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress_bar:
                task = progress_bar.add_task("Processing sources", total=len(monthly_sources))
                
                for i, source in enumerate(monthly_sources, 1):
                    source_name = getattr(source, 'name', f'source_{i}')
                    try:
                        result = pipeline.run(source)
                        total_results.append(result)
                        # 204 responses are handled by DLT as success with no data - this is correct behavior
                        progress_bar.update(task, advance=1, description=f"✅ {source_name}")
                    except Exception as e:
                        failed_months.append((source_name, str(e)))
                        progress_bar.update(task, advance=1, description=f"❌ {source_name} failed")
                        console.print(f"[red]Error in {source_name}: {e}[/red]")
                        # Continue with next source instead of failing entire process
            
            # Report results
            if failed_months:
                console.print(f"⚠️  {len(failed_months)} sources failed:")
                for source_name, error in failed_months[:5]:  # Show first 5 failures with details
                    console.print(f"   {source_name}: {error}")
                if len(failed_months) > 5:
                    console.print(f"   ... and {len(failed_months) - 5} more failures")
            
            # Use the last successful result for display
            result = total_results[-1] if total_results else None
        else:
            # Single date range - use standard source
            source = pncp_source(
                start_date=start_date, end_date=end_date, endpoints=endpoints
            )
            result = pipeline.run(source)

        # Mark extractions as completed
        if start_date and end_date:
            mark_extraction_completed(str(output), start_date, end_date, endpoints)

        console.print("✅ Extraction completed!")

        # Show results
        show_extraction_results(result, str(output))

    except Exception as e:
        console.print(f"❌ Extraction failed!")
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
    # table.add_row("details", "Specific contract details", "contratacao_especifica")  # Disabled
    table.add_row("all", "ALL 11 data types (default)", "All PNCP endpoints")

    console.print(table)
    console.print()

    # Configuration info
    console.print("⚙️  [bold]Configuration[/bold]")
    console.print(f"  PNCP API Base URL: {settings.pncp_api_base_url}")
    console.print(f"  Default Date Range: {settings.default_date_range_days} days")
    console.print()

    # Progress tracking options
    console.print("🎯 [bold]Progress Tracking Options[/bold]")
    console.print("  --progress enlighten      # Visual progress bars with logging (default)")
    console.print("  --progress tqdm           # Popular Python progress bars") 
    console.print("  --progress alive_progress # Animated progress bars")
    console.print("  --progress log            # Console/log output only")
    console.print()

    # Usage examples
    console.print("💡 [bold]Quick Start Examples[/bold]")
    console.print("  baliza extract                       # Extract monthly from 2021-01 to present")
    console.print("  baliza extract --days 7              # Last week only")
    console.print("  baliza extract --types contracts     # All historical contracts")
    console.print("  baliza extract --progress tqdm       # Use tqdm progress bars")
    console.print("  baliza extract --dry-run             # Preview what would be extracted")


@app.command()
def version():
    """Show version information."""
    # Note: Version retrieved from package metadata or dev fallback
    try:
        from importlib.metadata import version

        baliza_version = version("baliza")
    except ImportError:
        baliza_version = "2.0.0-dev"

    console.print(f"🚀 **Baliza** v{baliza_version}")
    console.print("   DLT-powered PNCP extraction pipeline")


@app.command()
def status(
    output: Path = typer.Option(
        "data/", "--output", "-o", help="Output directory to check"
    ),
):
    """Show status of completed extractions."""

    console.print("📊 [bold]Extraction Status[/bold]")
    console.print()

    completed = get_completed_extractions(str(output))

    if not completed:
        console.print("❌ No completed extractions found")
        console.print(f"   Check output directory: {output}")
        return

    # Note: Enhanced status reporting could show data quality metrics, file sizes, etc.
    #       For each completed endpoint, consider displaying:
    #       - The full date range covered (min_date to max_date).
    #       - The total number of records extracted (if available from metadata).
    #       - The total size of the extracted data.
    #       This would provide a more comprehensive overview of the extracted data.

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
    console.print(
        f"✅ [bold green]{len(completed)} endpoints[/bold green] with [bold green]{total_months} months[/bold green] completed"
    )
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


def _parse_date_options(
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
        "updates": [
            "contratacoes_atualizacao",
            "contratos_atualizacao",
            "atas_atualizacao",
        ],
        "proposals": "contratacoes_proposta",
        "charges": "instrumentoscobranca_inclusao",
        "pca": ["pca", "pca_usuario", "pca_atualizacao"],
        # "details": "contratacao_especifica"  # Disabled: requires specific params
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
            raise typer.BadParameter(
                f"Unknown data type: {type_name}. Available: {', '.join(type_mapping.keys())}"
            )

    return endpoints


def _show_extraction_plan(
    start_date: Optional[str],
    end_date: Optional[str],
    endpoints: list[str],
    output: Path,
    dry_run: bool,
):
    """Show extraction plan to user."""

    console.print("📋 [bold]Extraction Plan[/bold]")

    # Date range
    if start_date and end_date:
        console.print(f"  📅 Date Range: {start_date} to {end_date}")
    else:
        console.print(
            "  📅 Date Range: [bold green]ALL HISTORICAL DATA (backfill)[/bold green]"
        )

    # Endpoints
    endpoint_names = {
        "contratos": "Contracts",
        "contratacoes_publicacao": "Publications",
        "atas": "Agreements",
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

    # Display basic result information from DLT pipeline
    # Detailed metrics are available in DLT logs and state
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
