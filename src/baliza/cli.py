import asyncio
import json
import sys
from datetime import date
from pathlib import Path

import typer
from rich.console import Console

# Late imports for optional dependencies
from . import loader, mcp_server, transformer
from .config import settings
from .enums import ENUM_REGISTRY, get_enum_choices
from .extractor import (
    BALIZA_DB_PATH,
    DATA_DIR,
    AsyncPNCPExtractor,
    connect_utf8,
)

app = typer.Typer()
console = Console(force_terminal=True, legacy_windows=False, stderr=False)


@app.command()
def extract(
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
):
    """Extract data using true async architecture."""
    start_dt = date(2021, 1, 1)
    end_dt = date.today()

    async def main():
        async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
            results = await extractor.extract_data(start_dt, end_dt, force)

            # Save results
            results_file = (
                DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
            )
            with Path(results_file).open("w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, default=str)

            console.print(f"Results saved to: {results_file}")

    asyncio.run(main())


@app.command()
def transform():
    """
    Run dbt transformations.
    """
    transformer.transform()


@app.command()
def load():
    """
    Load data to Internet Archive.
    """
    loader.load()


@app.command()
def run(
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
    skip_transform: bool = typer.Option(
        False, "--skip-transform", help="Skip dbt transformation step"
    ),
    skip_load: bool = typer.Option(
        False, "--skip-load", help="Skip Internet Archive upload step"
    ),
):
    """
    Run complete ETL pipeline: Extract → Transform → Load.
    
    This command executes the full BALIZA pipeline:
    1. Extract data from PNCP API
    2. Transform data with dbt models 
    3. Load data to Internet Archive
    """
    console.print("🚀 [bold blue]Starting BALIZA ETL Pipeline[/bold blue]")
    console.print("")
    
    # Step 1: Extract
    console.print("📥 [bold green]Step 1: Extracting data from PNCP API[/bold green]")
    start_dt = date(2021, 1, 1)
    end_dt = date.today()

    async def extract_data():
        async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
            results = await extractor.extract_data(start_dt, end_dt, force)
            console.print(f"✅ Extraction completed: {results['total_records_extracted']:,} records")
            return results

    extraction_results = asyncio.run(extract_data())
    
    # Step 2: Transform  
    if not skip_transform:
        console.print("")
        console.print("🔄 [bold yellow]Step 2: Transforming data with dbt[/bold yellow]")
        try:
            transformer.transform()
            console.print("✅ Transformation completed successfully")
        except Exception as e:
            console.print(f"⚠️ [yellow]Transform step failed: {e}[/yellow]")
            console.print("Continuing to load step...")
    else:
        console.print("⏭️ [dim]Skipping transformation step[/dim]")
    
    # Step 3: Load
    if not skip_load:
        console.print("")
        console.print("📤 [bold cyan]Step 3: Loading data to Internet Archive[/bold cyan]")
        try:
            loader.load()
            console.print("✅ Load completed successfully")
        except Exception as e:
            console.print(f"⚠️ [yellow]Load step failed: {e}[/yellow]")
    else:
        console.print("⏭️ [dim]Skipping load step[/dim]")
    
    console.print("")
    console.print("🎉 [bold green]ETL Pipeline completed![/bold green]")
    console.print(f"📊 Total records processed: {extraction_results['total_records_extracted']:,}")


@app.command()
def mcp(
    host: str = typer.Option("127.0.0.1", help="The host to bind the MCP server to."),
    port: int = typer.Option(8000, help="The port to run the MCP server on."),
):
    """
    Starts the Model Context Protocol (MCP) server to allow language models
    to interact with the Baliza dataset.
    """
    console.print(f"🚀 Starting Baliza MCP Server at http://{host}:{port}")
    console.print("Press Ctrl+C to stop the server.")

    # This is a simplified call. We might need to adapt it based on
    # how fastmcp's `app.run()` is implemented, potentially passing host/port.
    mcp_server.run_server()


@app.command()
def stats():
    """Show extraction statistics."""
    conn = connect_utf8(str(BALIZA_DB_PATH))

    # Overall stats
    total_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses"
    ).fetchone()[0]
    success_responses = conn.execute(
        "SELECT COUNT(*) FROM psa.pncp_raw_responses WHERE response_code = 200"
    ).fetchone()[0]

    console.print(f"=== Total Responses: {total_responses:,} ===")
    console.print(f"Successful: {success_responses:,}")
    console.print(f"❌ Failed: {total_responses - success_responses:,}")

    if total_responses > 0:
        console.print(f"Success Rate: {success_responses / total_responses * 100:.1f}%")

    # Endpoint breakdown
    endpoint_stats = conn.execute(
        """
        SELECT endpoint_name, COUNT(*) as responses, SUM(total_records) as total_records
        FROM psa.pncp_raw_responses
        WHERE response_code = 200
        GROUP BY endpoint_name
        ORDER BY total_records DESC
    """
    ).fetchall()

    console.print("\n=== Endpoint Statistics ===")
    for name, responses, records in endpoint_stats:
        console.print(f"  {name}: {responses:,} responses, {records:,} records")

    conn.close()


@app.command()
def enums(
    enum_name: str = typer.Argument(None, help="Specific enum to display (optional)"),
):
    """Show available enum values for PNCP data types."""
    if enum_name:
        if enum_name not in ENUM_REGISTRY:
            console.print(f"❌ [red]Enum '{enum_name}' not found.[/red]")
            console.print("Available enums:", list(ENUM_REGISTRY.keys()))
            return

        enum_class = ENUM_REGISTRY[enum_name]
        choices = get_enum_choices(enum_class)

        console.print(f"\n📋 [bold blue]{enum_name}[/bold blue] values:")
        for value, name in choices.items():
            console.print(f"  {value}: {name.replace('_', ' ').title()}")
    else:
        console.print("📋 [bold blue]Available PNCP Enums[/bold blue]\n")
        for name, enum_class in ENUM_REGISTRY.items():
            choices = get_enum_choices(enum_class)
            console.print(f"• [cyan]{name}[/cyan] ({len(choices)} values)")

        console.print(
            "\n💡 Use [green]baliza enums <enum_name>[/green] to see specific values"
        )


if __name__ == "__main__":
    # Configure streams for UTF-8
    for std in (sys.stdin, sys.stdout, sys.stderr):
        if std and hasattr(std, "reconfigure"):
            std.reconfigure(encoding="utf-8", errors="surrogateescape")
    app()
