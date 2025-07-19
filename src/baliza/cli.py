import asyncio
import json
import sys
from datetime import date
from pathlib import Path

import typer

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
from .ui import Dashboard, ErrorHandler, get_console

app = typer.Typer(no_args_is_help=False)  # Allow running without args
console = get_console()  # Use themed console
dashboard = Dashboard()
error_handler = ErrorHandler()


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """BALIZA - Brazilian Public Procurement Data Platform

    Analyze contracts, procurements, and spending with ease.
    """
    if ctx.invoked_subcommand is None:
        # Show the beautiful dashboard when no command is provided
        dashboard.show_welcome_dashboard()


@app.command()
def extract(
    endpoint: str = typer.Argument(
        "all", help="Endpoint to extract (all, contratos, atas, contratacoes)"
    ),
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
):
    """Extract data from PNCP API with beautiful progress tracking."""
    from .ui import create_header, format_number, get_theme

    theme = get_theme()
    start_dt = date(2021, 1, 1)
    end_dt = date.today()

    # Show beautiful header
    if endpoint == "all":
        endpoint_display = "All Endpoints"
        subtitle = "Complete data extraction from PNCP API"
    else:
        endpoint_display = endpoint.title()
        subtitle = f"Targeted extraction from {endpoint} endpoint"

    header = create_header(
        f"Extracting {endpoint_display} Data", subtitle, theme.ICONS["extract"]
    )
    console.print(header)

    # Show configuration
    config_content = [
        "⚙️  Configuration",
        f"   📅 Date range: {start_dt} to {end_dt}",
        f"   🔀 Concurrency: {concurrency} parallel requests",
        "   💾 Storage: Split-table architecture with deduplication",
        "   🔄 Rate limiting: Optimized for PNCP (0ms delay)",
        "",
        "🚀 Starting extraction...",
    ]

    from .ui.components import create_info_panel

    config_panel = create_info_panel(
        "\n".join(config_content),
        title=f"{theme.ICONS['settings']} Extraction Setup",
        style="primary",
    )
    console.print(config_panel)

    async def main():
        try:
            async with AsyncPNCPExtractor(concurrency=concurrency) as extractor:
                results = await extractor.extract_data(start_dt, end_dt, force)

                # Show completion summary
                completion_content = [
                    "✨ Extraction Complete",
                    "",
                    "📊 Results:",
                    f"   New records: {format_number(results.get('total_records_extracted', 0))}",
                    f"   API calls: {format_number(results.get('total_requests', 0))}",
                    f"   Success rate: {results.get('success_rate', 0):.1f}%",
                    f"   Run ID: {results.get('run_id', 'Unknown')[:8]}...",
                ]

                completion_panel = create_info_panel(
                    "\n".join(completion_content),
                    title=f"{theme.ICONS['success']} Extraction Summary",
                    style="success",
                )
                console.print(completion_panel)

                # Save results file
                results_file = (
                    DATA_DIR / f"async_extraction_results_{results['run_id']}.json"
                )
                with Path(results_file).open("w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2, default=str)

                # Show next steps
                next_steps_content = [
                    "💡 Next steps:",
                    "   baliza transform → Process data with dbt models",
                    "   baliza status    → View detailed system status",
                    "   baliza explore   → Interactive data browser",
                ]

                next_steps_panel = create_info_panel(
                    "\n".join(next_steps_content),
                    title=f"{theme.ICONS['info']} What's Next?",
                    style="info",
                )
                console.print(next_steps_panel)

        except Exception as e:
            error_handler.handle_api_error(
                e, "PNCP API", {"endpoint": endpoint, "concurrency": concurrency}
            )
            raise

    asyncio.run(main())


@app.command()
def transform():
    """Transform raw data into analytics-ready tables with dbt."""
    from .ui import create_header, get_theme
    from .ui.components import create_info_panel

    theme = get_theme()

    # Show beautiful header
    header = create_header(
        "Transforming Data with dbt",
        "Raw data → Analytics-ready tables",
        theme.ICONS["transform"],
    )
    console.print(header)

    # Show transformation info
    transform_content = [
        "🏗️  Building Data Models",
        "",
        "This will create analytics-ready tables from your raw PNCP data:",
        "   📋 bronze_pncp_raw      → Clean, validated contract data",
        "   📊 bronze_content_analysis → Deduplication metrics",
        "   🏆 mart_compras_beneficios → High-value contract analysis",
        "   💰 mart_spending_analysis  → Spending trends and patterns",
        "",
        "⏱️  Estimated time: 10-30 seconds",
    ]

    transform_panel = create_info_panel(
        "\n".join(transform_content),
        title=f"{theme.ICONS['info']} Transform Overview",
        style="primary",
    )
    console.print(transform_panel)

    try:
        # Run transformation
        console.print(
            f"\n{theme.ICONS['process']} [primary]Running dbt transformations...[/primary]"
        )
        transformer.transform()

        # Show success message
        success_content = [
            "✨ Transform Complete",
            "",
            "🎯 Data is now ready for analysis:",
            "   📊 All models built successfully",
            "   🔍 Data quality checks passed",
            "   💾 Tables optimized and indexed",
            "",
            "💡 Next steps:",
            "   baliza explore   → Browse your data interactively",
            "   baliza load      → Publish to Internet Archive",
            "   baliza status    → View system health",
        ]

        success_panel = create_info_panel(
            "\n".join(success_content),
            title=f"{theme.ICONS['success']} Transform Success",
            style="success",
        )
        console.print(success_panel)

    except Exception as e:
        error_handler.handle_database_error(e, "transform")
        console.print(
            "\n[warning]💡 Tip: Check that your database has data with 'baliza extract' first[/warning]"
        )
        raise


@app.command()
def load():
    """Load data to Internet Archive for public access."""
    from .ui import create_header, get_theme
    from .ui.components import create_info_panel

    theme = get_theme()

    # Show beautiful header
    header = create_header(
        "Loading Data to Internet Archive",
        "Publishing your data for public access and preservation",
        theme.ICONS["load"],
    )
    console.print(header)

    # Show load info
    load_content = [
        "📤 Publishing to Internet Archive",
        "",
        "This will upload your processed data to the Internet Archive:",
        "   🌐 Public access for researchers and developers",
        "   📚 Permanent preservation of Brazilian procurement data",
        "   🔄 Automated versioning and metadata",
        "   🏛️  Open government data initiative",
        "",
        "⏱️  Estimated time: 2-5 minutes (depending on data size)",
    ]

    load_panel = create_info_panel(
        "\n".join(load_content),
        title=f"{theme.ICONS['info']} Publication Overview",
        style="primary",
    )
    console.print(load_panel)

    try:
        # Run load
        console.print(
            f"\n{theme.ICONS['process']} [primary]Uploading to Internet Archive...[/primary]"
        )
        loader.load()

        # Show success message
        success_content = [
            "✨ Load Complete",
            "",
            "🎯 Your data is now publicly available:",
            "   🌐 Accessible via Internet Archive",
            "   📊 Searchable and downloadable",
            "   🔗 Permanent archival links generated",
            "   📈 Contributing to open government data",
            "",
            "💡 What's next:",
            "   Share the archive links with researchers",
            "   Schedule regular updates with 'baliza run'",
            "   Monitor usage and impact",
        ]

        success_panel = create_info_panel(
            "\n".join(success_content),
            title=f"{theme.ICONS['success']} Publication Success",
            style="success",
        )
        console.print(success_panel)

    except Exception as e:
        error_handler.handle_api_error(e, "Internet Archive", {"operation": "upload"})
        console.print(
            "\n[warning]💡 Tip: Check your Internet Archive credentials and network connection[/warning]"
        )
        raise


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
            console.print(
                f"✅ Extraction completed: {results['total_records_extracted']:,} records"
            )
            return results

    extraction_results = asyncio.run(extract_data())

    # Step 2: Transform
    if not skip_transform:
        console.print("")
        console.print(
            "🔄 [bold yellow]Step 2: Transforming data with dbt[/bold yellow]"
        )
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
        console.print(
            "📤 [bold cyan]Step 3: Loading data to Internet Archive[/bold cyan]"
        )
        try:
            loader.load()
            console.print("✅ Load completed successfully")
        except Exception as e:
            console.print(f"⚠️ [yellow]Load step failed: {e}[/yellow]")
    else:
        console.print("⏭️ [dim]Skipping load step[/dim]")

    console.print("")
    console.print("🎉 [bold green]ETL Pipeline completed![/bold green]")
    console.print(
        f"📊 Total records processed: {extraction_results['total_records_extracted']:,}"
    )


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
def status():
    """Show detailed system status and health monitoring."""
    dashboard.show_detailed_status()


@app.command()
def stats():
    """Show extraction statistics (legacy command)."""
    # Keep the original stats command for backward compatibility
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


@app.command()
def explore():
    """Interactive data explorer for browsing procurement data."""
    from .ui.explorer import DataExplorer

    explorer = DataExplorer()
    explorer.start_interactive_session()


@app.command()
def tutorial():
    """Interactive tutorial for learning BALIZA."""
    from .ui import create_header, get_theme
    from .ui.components import create_info_panel

    theme = get_theme()

    header = create_header(
        "Welcome to BALIZA Tutorial",
        "Learn to analyze Brazilian procurement data in 5 minutes",
        theme.ICONS["tutorial"],
    )
    console.print(header)

    tutorial_content = [
        "🎓 Quick Start Guide",
        "",
        "Step 1: Extract Some Data (2 minutes)",
        "   Let's grab today's contract data from the government API.",
        "   ",
        "   $ [primary]baliza extract contratos[/primary]",
        "   ",
        "   This will:",
        "   ✅ Connect to PNCP (Portal Nacional de Contratações Públicas)",
        "   ✅ Download contract publications from today",
        "   ✅ Store efficiently with deduplication",
        "",
        "Step 2: Transform for Analysis (30 seconds)",
        "   $ [primary]baliza transform[/primary]",
        "   ",
        "   This creates analytics-ready tables using dbt, like:",
        "   📊 Contract summaries by agency and category",
        "   💰 Spending analysis and trends",
        "   🔍 High-value contract identification",
        "",
        "Step 3: Explore Your Data (ongoing)",
        "   $ [primary]baliza explore[/primary]",
        "   ",
        "   Interactive browser to discover insights:",
        "   🎯 Filter by category, agency, or value",
        "   📈 See trends and patterns",
        "   🏆 Find the biggest contracts",
        "",
        "Ready to start? Try: [primary]baliza extract contratos[/primary]",
    ]

    tutorial_panel = create_info_panel(
        "\n".join(tutorial_content),
        title=f"{theme.ICONS['data']} Learn BALIZA",
        style="primary",
    )
    console.print(tutorial_panel)

    # Next steps
    next_steps_content = [
        "💡 More Commands:",
        "   baliza status    → System health and statistics",
        "   baliza run       → Complete ETL pipeline",
        "   baliza enums     → Available data categories",
        "   baliza load      → Publish to Internet Archive",
        "",
        "🏛️  About the Data:",
        "   PNCP contains all Brazilian federal procurement data",
        "   Updated daily with new contracts and amendments",
        "   Includes spending, suppliers, and agency information",
        "",
        "🤝 Need Help?",
        "   Each command has detailed help: baliza <command> --help",
        "   Rich dashboard shows next steps: just run 'baliza'",
        "   Data explorer guides you through analysis",
    ]

    next_steps_panel = create_info_panel(
        "\n".join(next_steps_content),
        title=f"{theme.ICONS['info']} Learn More",
        style="info",
    )
    console.print(next_steps_panel)


if __name__ == "__main__":
    # Configure streams for UTF-8
    for std in (sys.stdin, sys.stdout, sys.stderr):
        if std and hasattr(std, "reconfigure"):
            std.reconfigure(encoding="utf-8", errors="surrogateescape")
    app()
