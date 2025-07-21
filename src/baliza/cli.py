import asyncio
import json
import signal
import sys
from datetime import date
from pathlib import Path

import typer

# Late imports for optional dependencies
from . import mcp_server
from .config import settings
from .dependencies import get_cli_services
from .enums import ENUM_REGISTRY, get_enum_choices
from .extractor import (
    BALIZA_DB_PATH,
    DATA_DIR,
    connect_utf8,
)
from .ui import Dashboard, ErrorHandler, get_console

app = typer.Typer(no_args_is_help=False)  # Allow running without args
console = get_console()  # Use themed console
dashboard = Dashboard()
error_handler = ErrorHandler()

# Global shutdown flag
_shutdown_requested = False


def setup_signal_handlers():
    """Setup graceful shutdown signal handlers."""

    def signal_handler(signum, frame):
        """Handle shutdown signals gracefully."""
        global _shutdown_requested
        # Handle the signal gracefully (using frame for context information)
        signal_name = (
            signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        )
        frame_info = (
            f" (in {frame.f_code.co_filename}:{frame.f_lineno})" if frame else ""
        )

        if not _shutdown_requested:
            _shutdown_requested = True
            console.print(
                f"\n🛑 [yellow]Shutdown requested ({signal_name}){frame_info}... cleaning up[/yellow]"
            )
            console.print(
                "⏳ Please wait for graceful shutdown (Press Ctrl+C again to force)"
            )
        else:
            console.print(
                f"\n💥 [red]Force shutdown ({signal_name}) - may leave locks![/red]"
            )
            sys.exit(1)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """BALIZA - Brazilian Acquisition Ledger Intelligence Zone Archive

    Navigate contracts, procurements, and spending data with ease.
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
    force_db: bool = typer.Option(
        False, "--force-db", help="Force database connection by removing locks"
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
        # Setup signal handlers for graceful shutdown
        setup_signal_handlers()

        try:
            # Use dependency injection for extractor
            services = get_cli_services()
            extractor = services.get_extractor(
                concurrency=concurrency, force_db=force_db
            )

            async with extractor as ext:
                # Check for shutdown before starting
                if _shutdown_requested:
                    console.print(
                        "🛑 [yellow]Shutdown requested before extraction started[/yellow]"
                    )
                    return {"total_records_extracted": 0, "run_id": "cancelled"}

                results = await ext.extract_data(start_dt, end_dt, force)

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

        except KeyboardInterrupt:
            console.print("\n🛑 [yellow]Graceful shutdown completed[/yellow]")
            return {"total_records_extracted": 0, "run_id": "interrupted"}
        except Exception as e:
            if _shutdown_requested:
                console.print(
                    "🛑 [yellow]Shutdown during extraction - cleaning up[/yellow]"
                )
                return {"total_records_extracted": 0, "run_id": "shutdown"}
            else:
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
        # Run transformation using dependency injection
        console.print(
            f"\n{theme.ICONS['process']} [primary]Running dbt transformations...[/primary]"
        )
        services = get_cli_services()
        transformer = services.get_transformer()
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
        # Run load using dependency injection
        console.print(
            f"\n{theme.ICONS['process']} [primary]Uploading to Internet Archive...[/primary]"
        )
        services = get_cli_services()
        loader = services.get_loader()
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


# ETL Pipeline Components - Refactored for better maintainability and testability


class ETLPipelineStep:
    """Base class for ETL pipeline steps."""

    def __init__(self, name: str, emoji: str, color: str):
        self.name = name
        self.emoji = emoji
        self.color = color

    def should_skip(self, skip_flag: bool) -> bool:
        """Check if step should be skipped."""
        if _shutdown_requested:
            console.print(f"🛑 [yellow]Skipping {self.name} due to shutdown[/yellow]")
            return True
        if skip_flag:
            console.print(f"⏭️ [dim]Skipping {self.name} step[/dim]")
            return True
        return False

    def print_step_header(self, step_number: int):
        """Print standardized step header."""
        console.print("")
        console.print(
            f"{self.emoji} [bold {self.color}]Step {step_number}: {self.name}[/bold {self.color}]"
        )


async def run_extraction_step(concurrency: int, force_db: bool, force: bool) -> dict:
    """Execute the data extraction step with proper error handling."""
    step = ETLPipelineStep("Extracting data from PNCP API", "📥", "green")
    step.print_step_header(1)

    start_dt = date(2021, 1, 1)
    end_dt = date.today()

    async def extract_data():
        try:
            if _shutdown_requested:
                console.print(
                    "🛑 [yellow]Shutdown requested before extraction[/yellow]"
                )
                return {"total_records_extracted": 0, "run_id": "cancelled"}

            # Use dependency injection for extractor in pipeline
            services = get_cli_services()
            extractor = services.get_extractor(
                concurrency=concurrency, force_db=force_db
            )

            async with extractor as ext:
                results = await ext.extract_data(start_dt, end_dt, force)
                console.print(
                    f"✅ Extraction completed: {results['total_records_extracted']:,} records"
                )
                return results
        except KeyboardInterrupt:
            console.print("🛑 [yellow]Extraction interrupted gracefully[/yellow]")
            return {"total_records_extracted": 0, "run_id": "interrupted"}
        except Exception as e:
            if _shutdown_requested:
                console.print(
                    "🛑 [yellow]Extraction shutdown during operation[/yellow]"
                )
                return {"total_records_extracted": 0, "run_id": "shutdown"}
            else:
                console.print(f"❌ [red]Extraction failed: {e}[/red]")
                raise

    try:
        extraction_results = await extract_data()

        # Check for shutdown after extraction
        if _shutdown_requested:
            console.print("🛑 [yellow]Pipeline stopped after extraction[/yellow]")

        return extraction_results

    except KeyboardInterrupt:
        console.print("🛑 [yellow]Pipeline interrupted during extraction[/yellow]")
        return {"total_records_extracted": 0, "run_id": "interrupted"}


def run_transformation_step(skip_transform: bool) -> bool:
    """Execute the data transformation step with proper error handling.

    Returns:
        bool: True if step completed successfully, False if failed or skipped
    """
    step = ETLPipelineStep("Transforming data with dbt", "🔄", "yellow")

    if step.should_skip(skip_transform):
        return True

    step.print_step_header(2)

    try:
        # Use dependency injection for transformer in pipeline
        services = get_cli_services()
        transformer = services.get_transformer()
        transformer.transform()
        console.print("✅ Transformation completed successfully")
        return True
    except KeyboardInterrupt:
        console.print("🛑 [yellow]Transform interrupted[/yellow]")
        return False
    except Exception as e:
        if _shutdown_requested:
            console.print("🛑 [yellow]Transform stopped due to shutdown[/yellow]")
            return False
        else:
            console.print(f"⚠️ [yellow]Transform step failed: {e}[/yellow]")
            console.print("Continuing to load step...")
            return False


def run_load_step(skip_load: bool) -> bool:
    """Execute the data loading step with proper error handling.

    Returns:
        bool: True if step completed successfully, False if failed or skipped
    """
    step = ETLPipelineStep("Loading data to Internet Archive", "📤", "cyan")

    if step.should_skip(skip_load):
        return True

    step.print_step_header(3)

    try:
        # Use dependency injection for loader in pipeline
        services = get_cli_services()
        loader = services.get_loader()
        loader.load()
        console.print("✅ Load completed successfully")
        return True
    except KeyboardInterrupt:
        console.print("🛑 [yellow]Load interrupted[/yellow]")
        return False
    except Exception as e:
        if _shutdown_requested:
            console.print("🛑 [yellow]Load stopped due to shutdown[/yellow]")
            return False
        else:
            console.print(f"⚠️ [yellow]Load step failed: {e}[/yellow]")
            return False


def print_pipeline_summary(extraction_results: dict):
    """Print the final pipeline summary."""
    console.print("")
    if _shutdown_requested:
        console.print("🛑 [yellow]ETL Pipeline stopped gracefully[/yellow]")
    else:
        console.print("🎉 [bold green]ETL Pipeline completed![/bold green]")
    console.print(
        f"📊 Total records processed: {extraction_results['total_records_extracted']:,}"
    )


@app.command()
def run(
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force: bool = typer.Option(
        False, "--force", help="Force re-extraction even if data exists"
    ),
    force_db: bool = typer.Option(
        False, "--force-db", help="Force database connection by removing locks"
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
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()

    console.print("🚀 [bold blue]Starting BALIZA ETL Pipeline[/bold blue]")
    console.print("")

    async def run_pipeline():
        # Step 1: Extract
        extraction_results = await run_extraction_step(concurrency, force_db, force)

        if _shutdown_requested:
            return extraction_results

        # Step 2: Transform
        run_transformation_step(skip_transform)

        if _shutdown_requested:
            return extraction_results

        # Step 3: Load
        run_load_step(skip_load)

        return extraction_results

    try:
        extraction_results = asyncio.run(run_pipeline())
        print_pipeline_summary(extraction_results)

    except KeyboardInterrupt:
        console.print("🛑 [yellow]Pipeline interrupted[/yellow]")
    except Exception as e:
        console.print(f"❌ [red]Pipeline failed: {e}[/red]")
        raise


@app.command()
def extract_dbt(
    start_date: str = typer.Option(
        "2024-01-01", help="Start date for extraction (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        "2024-01-31", help="End date for extraction (YYYY-MM-DD)"
    ),
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force_db: bool = typer.Option(
        False, "--force-db", help="Force database connection by removing locks"
    ),
    use_existing_plan: bool = typer.Option(
        True,
        "--use-existing-plan/--force-new-plan",
        help="Use existing task plan if available",
    ),
):
    """
    Run dbt-driven extraction (ADR-009 architecture).

    This command uses the new dbt-driven task planning architecture:
    1. Generate/validate task plan using dbt models
    2. Atomic task claiming for concurrent worker safety
    3. Stream results to runtime tables for monitoring
    """
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()

    # Parse dates
    try:
        start_dt = date.fromisoformat(start_date)
        end_dt = date.fromisoformat(end_date)
    except ValueError as e:
        console.print(f"❌ [red]Invalid date format: {e}[/red]")
        console.print("Use YYYY-MM-DD format (e.g., 2024-01-01)")
        raise typer.Exit(1)

    console.print("🚀 [bold blue]Starting dbt-driven extraction (ADR-009)[/bold blue]")
    console.print(f"📅 Date range: {start_date} to {end_date}")
    console.print(f"🔧 Concurrency: {concurrency}")
    console.print("")

    async def run_dbt_extraction():
        try:
            if _shutdown_requested:
                console.print(
                    "🛑 [yellow]Shutdown requested before extraction[/yellow]"
                )
                return {"total_records": 0}

            # Use dependency injection for dbt extraction
            services = get_cli_services()
            extractor = services.get_extractor(
                concurrency=concurrency, force_db=force_db
            )

            async with extractor as ext:
                results = await ext.extract_dbt_driven(
                    start_dt, end_dt, use_existing_plan
                )
                return results

        except KeyboardInterrupt:
            console.print("🛑 [yellow]Extraction interrupted gracefully[/yellow]")
            return {"total_records": 0}
        except Exception as e:
            if _shutdown_requested:
                console.print(
                    "🛑 [yellow]Extraction shutdown during operation[/yellow]"
                )
                return {"total_records": 0}
            else:
                console.print(f"❌ [red]dbt extraction failed: {e}[/red]")
                raise

    try:
        results = asyncio.run(run_dbt_extraction())

        if not _shutdown_requested:
            console.print("")
            console.print(
                "🎉 [bold green]dbt-driven extraction completed![/bold green]"
            )
            console.print(
                f"📊 Total records extracted: {results.get('total_records', 0):,}"
            )
            console.print(f"✅ Completed tasks: {results.get('completed_tasks', 0):,}")
            console.print(f"❌ Failed tasks: {results.get('failed_tasks', 0):,}")
            console.print(f"⏳ Pending tasks: {results.get('pending_tasks', 0):,}")

    except KeyboardInterrupt:
        console.print("🛑 [yellow]dbt extraction interrupted[/yellow]")
    except Exception as e:
        console.print(f"❌ [red]dbt extraction failed: {e}[/red]")
        raise typer.Exit(1)


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

    # Pass host and port to the MCP server
    mcp_server.run_server(host=host, port=port)


@app.command()
def status():
    """Show detailed system status and health monitoring."""
    dashboard.show_detailed_status()


@app.command()
def stats(
    force_db: bool = typer.Option(
        False, "--force-db", help="Force database connection by removing locks"
    ),
):
    """Show extraction statistics (legacy command)."""
    # Keep the original stats command for backward compatibility
    if force_db:
        # Remove lock file if exists
        lock_file = Path(str(BALIZA_DB_PATH) + ".lock")
        if lock_file.exists():
            lock_file.unlink()
            console.print("🔓 [yellow]Force mode: Removed database lock[/yellow]")

    conn = connect_utf8(str(BALIZA_DB_PATH), force=force_db)

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
