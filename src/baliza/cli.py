import asyncio
import json
from datetime import date
from pathlib import Path

import typer
from rich.console import Console

from .extractor import (
    BALIZA_DB_PATH,
    CONCURRENCY,
    DATA_DIR,
    AsyncPNCPExtractor,
    connect_utf8,
)

app = typer.Typer()
console = Console(force_terminal=True, legacy_windows=False, stderr=False)


@app.command()
def extract(
    concurrency: int = typer.Option(CONCURRENCY, help="Number of concurrent requests"),
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


from . import mcp_server


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


import sys

if __name__ == "__main__":
    # Configure streams for UTF-8
    for std in (sys.stdin, sys.stdout, sys.stderr):
        if std and hasattr(std, "reconfigure"):
            std.reconfigure(encoding="utf-8", errors="surrogateescape")
    app()
