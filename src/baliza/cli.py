import asyncio
import signal
import sys
from calendar import monthrange
from datetime import date
from pathlib import Path

import typer

# Late imports for optional dependencies
from . import mcp_server
from .config import settings
from .dependencies import get_cli_services
from .enums import ENUM_REGISTRY, get_enum_choices
from .extractor import AsyncPNCPExtractor
from .ui import Dashboard, ErrorHandler, get_console

app = typer.Typer(no_args_is_help=False)  # Allow running without args
console = get_console()  # Use themed console
dashboard = Dashboard()
error_handler = ErrorHandler()

# Global shutdown flag
_shutdown_requested = False


def adjust_to_end_of_month(input_date: date) -> date:
    """Ajusta a data para o final do mês para manter requisições idempotentes."""
    year = input_date.year
    month = input_date.month
    _, last_day = monthrange(year, month)
    return date(year, month, last_day)


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
    start_date: str = typer.Option("2021-01-01", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(date.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)"),
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force_db: bool = typer.Option(
        False, "--force-db", help="Force database connection by removing locks"
    ),
):
    """Extract data from PNCP API using the new vectorized engine."""
    from .ui import create_header, get_theme

    theme = get_theme()
    start_dt = date.fromisoformat(start_date)
    end_dt = adjust_to_end_of_month(date.fromisoformat(end_date))

    # Mostrar a data ajustada para o usuário
    if end_date != end_dt.strftime("%Y-%m-%d"):
        console.print(f"📅 [yellow]Data final ajustada para final do mês: {end_dt.strftime('%Y-%m-%d')}[/yellow]")

    header = create_header(
        "Extracting Data (Vectorized)",
        "High-performance, stateless extraction",
        theme.ICONS["extract"],
    )
    console.print(header)

    async def main():
        setup_signal_handlers()
        try:
            extractor = AsyncPNCPExtractor(concurrency=concurrency, force_db=force_db)
            async with extractor as ext:
                if _shutdown_requested:
                    return
                await ext.extract_data(start_dt, end_dt)
        except Exception as e:
            error_handler.handle_api_error(e, "PNCP API", {})
            raise

    asyncio.run(main())


@app.command()
def transform():
    """Transform raw data into analytics-ready tables with dbt."""
    # This command remains largely the same
    from .ui import create_header, get_theme

    theme = get_theme()
    header = create_header(
        "Transforming Data with dbt",
        "Raw data → Analytics-ready tables",
        theme.ICONS["transform"],
    )
    console.print(header)

    try:
        services = get_cli_services()
        transformer = services.get_transformer()
        transformer.transform()
        console.print("✅ [bold green]Transformation complete![/bold green]")
    except Exception as e:
        error_handler.handle_database_error(e, "transform")
        raise


@app.command()
def load():
    """Load data to Internet Archive for public access."""
    # This command remains largely the same
    from .ui import create_header, get_theme

    theme = get_theme()
    header = create_header(
        "Loading Data to Internet Archive",
        "Publishing data for public access",
        theme.ICONS["load"],
    )
    console.print(header)

    try:
        services = get_cli_services()
        loader = services.get_loader()
        loader.load()
        console.print("✅ [bold green]Load complete![/bold green]")
    except Exception as e:
        error_handler.handle_api_error(e, "Internet Archive", {})
        raise


@app.command()
def run(
    start_date: str = typer.Option("2021-01-01", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(date.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)"),
    concurrency: int = typer.Option(
        settings.concurrency, help="Number of concurrent requests"
    ),
    force_db: bool = typer.Option(
        False, "--force-db", help="Force database connection by removing locks"
    ),
    skip_transform: bool = typer.Option(False, help="Skip dbt transformation"),
    skip_load: bool = typer.Option(False, help="Skip Internet Archive upload"),
):
    """Run the complete ETL pipeline: Extract → Transform → Load."""
    console.print("🚀 [bold blue]Starting BALIZA ETL Pipeline[/bold blue]")

    # Run Extract
    console.print("\n[bold green]Step 1: Extract[/bold green]")
    extract(start_date, end_date, concurrency, force_db)

    # Run Transform
    if not skip_transform:
        console.print("\n[bold yellow]Step 2: Transform[/bold yellow]")
        transform()

    # Run Load
    if not skip_load:
        console.print("\n[bold cyan]Step 3: Load[/bold cyan]")
        load()

    console.print("\n🎉 [bold green]ETL Pipeline finished![/bold green]")


# ... (keep other commands like mcp, status, stats, enums, explore, tutorial)
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
        "   $ [primary]baliza extract --start-date <today> --end-date <today>[/primary]",
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
        "Ready to start? Try: [primary]baliza extract[/primary]",
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
