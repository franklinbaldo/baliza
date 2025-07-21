#!/usr/bin/env python3
"""
PNCP Endpoint Testing Script

Tests all configured PNCP endpoints to verify they are working correctly.
This script helps validate endpoint configurations before running the main extractor.
"""

import asyncio
from datetime import date, timedelta
from typing import Any

import httpx
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

from src.baliza.config import settings
from src.baliza.enums import ModalidadeContratacao
from src.baliza.ui.theme import get_console
from src.baliza.utils import parse_json_robust

console = get_console()


class EndpointTester:
    """Tests PNCP API endpoints for functionality and response validation."""

    def __init__(self):
        self.client = None
        self.results = []

    async def __aenter__(self):
        """Initialize HTTP client."""
        self.client = httpx.AsyncClient(
            base_url=settings.pncp_base_url,
            timeout=30.0,
            headers={
                "User-Agent": settings.user_agent,
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close HTTP client."""
        if self.client:
            await self.client.aclose()

    def _get_test_date_params(self, endpoint_config: dict) -> dict[str, str]:
        """Generate appropriate date parameters for testing."""
        # Use yesterday to ensure data exists
        test_date = date.today() - timedelta(days=1)
        # PNCP API uses YYYYMMDD format (no separators)
        date_str = test_date.strftime('%Y%m%d')

        params = {}

        if endpoint_config.get("supports_date_range", True):
            # Most endpoints use dataInicial/dataFinal
            date_params = endpoint_config.get("date_params", ["dataInicial", "dataFinal"])
            if "dataInicio" in date_params:  # PCA uses different param names
                params["dataInicio"] = date_str
                params["dataFim"] = date_str
            else:
                params["dataInicial"] = date_str
                params["dataFinal"] = date_str
        elif endpoint_config.get("requires_single_date"):
            # Some endpoints use only dataFinal
            params["dataFinal"] = date_str
        elif endpoint_config.get("requires_future_date"):
            # Some endpoints need future dates
            future_date = date.today() + timedelta(days=30)
            params["dataFinal"] = future_date.strftime('%Y%m%d')

        return params

    async def test_endpoint(self, endpoint_config: dict, modalidade: int = None) -> dict[str, Any]:
        """Test a single endpoint configuration."""
        endpoint_name = endpoint_config["name"]
        endpoint_path = endpoint_config["path"]

        # Build test parameters
        params = self._get_test_date_params(endpoint_config)
        params.update({
            "pagina": 1,
            "tamanhoPagina": 10,  # Small page size for testing
        })

        # Add modalidade if specified
        if modalidade is not None:
            params["modalidade"] = modalidade
            test_id = f"{endpoint_name}_modalidade_{modalidade}"
        else:
            test_id = endpoint_name

        result = {
            "test_id": test_id,
            "endpoint_name": endpoint_name,
            "endpoint_path": endpoint_path,
            "modalidade": modalidade,
            "params": params,
            "success": False,
            "status_code": None,
            "error": None,
            "response_size": 0,
            "total_records": 0,
            "total_pages": 0,
            "has_data": False,
        }

        try:
            # Make the request
            response = await self.client.get(endpoint_path, params=params)
            result["status_code"] = response.status_code

            if response.status_code == 200:
                content = response.text
                result["response_size"] = len(content)

                if content:
                    # Parse JSON response
                    data = parse_json_robust(content)
                    if data:
                        result["total_records"] = data.get("totalRegistros", 0)
                        result["total_pages"] = data.get("totalPaginas", 0)
                        result["has_data"] = len(data.get("data", [])) > 0
                        result["success"] = True
                    else:
                        result["error"] = "Invalid JSON response"
                else:
                    result["success"] = True  # Empty response is valid for some endpoints
                    result["has_data"] = False

            elif response.status_code == 404:
                result["success"] = True  # 404 is acceptable (no data for date range)
                result["error"] = "No data found (404)"

            elif response.status_code == 429:
                result["error"] = "Rate limited (429)"

            else:
                result["error"] = f"HTTP {response.status_code}: {response.text[:100]}"

        except Exception as e:
            result["error"] = f"Request failed: {str(e)[:100]}"

        return result

    async def test_all_endpoints(self):
        """Test all configured endpoints."""
        console.print("🧪 [bold blue]Testing PNCP API Endpoints[/bold blue]")
        console.print()

        # Collect all test configurations
        test_configs = []

        for endpoint_config in settings.pncp_endpoints:
            if endpoint_config.get("iterate_modalidades"):
                # Test each modalidade separately for contratacoes endpoints
                modalidades = [m.value for m in ModalidadeContratacao]
                for modalidade in modalidades:
                    test_configs.append((endpoint_config, modalidade))
            else:
                # Test endpoint without modalidade
                test_configs.append((endpoint_config, None))

        console.print(f"📊 Testing {len(test_configs)} endpoint configurations...")
        console.print()

        # Run tests with progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Testing endpoints..."),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console,
        ) as progress:
            task = progress.add_task("Testing", total=len(test_configs))

            for endpoint_config, modalidade in test_configs:
                result = await self.test_endpoint(endpoint_config, modalidade)
                self.results.append(result)
                progress.update(task, advance=1)

                # Small delay to avoid overwhelming the API
                await asyncio.sleep(0.1)

        console.print()
        console.print("✅ [bold green]Testing completed![/bold green]")
        console.print()

    def print_results(self):
        """Print formatted test results."""
        # Summary stats
        total_tests = len(self.results)
        successful = sum(1 for r in self.results if r["success"])
        failed = total_tests - successful
        with_data = sum(1 for r in self.results if r["has_data"])

        # Summary panel
        summary_content = [
            f"Total Tests: {total_tests}",
            f"✅ Successful: {successful} ({successful/total_tests*100:.1f}%)",
            f"❌ Failed: {failed} ({failed/total_tests*100:.1f}%)" if failed > 0 else "❌ Failed: 0",
            f"📊 With Data: {with_data} ({with_data/total_tests*100:.1f}%)",
        ]

        summary_panel = Panel(
            "\n".join(summary_content),
            title="🧪 Test Summary",
            border_style="blue",
        )
        console.print(summary_panel)

        # Detailed results table
        table = Table(title="📋 Detailed Test Results")
        table.add_column("Test ID", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Code", justify="center")
        table.add_column("Records", justify="right")
        table.add_column("Pages", justify="right")
        table.add_column("Has Data", justify="center")
        table.add_column("Error", style="red")

        for result in self.results:
            status = "✅ PASS" if result["success"] else "❌ FAIL"
            status_style = "green" if result["success"] else "red"

            has_data_icon = "📊" if result["has_data"] else "📭"
            error_text = result["error"][:50] + "..." if result["error"] and len(result["error"]) > 50 else (result["error"] or "")

            table.add_row(
                result["test_id"],
                f"[{status_style}]{status}[/{status_style}]",
                str(result["status_code"]) if result["status_code"] else "N/A",
                str(result["total_records"]),
                str(result["total_pages"]),
                has_data_icon,
                error_text,
            )

        console.print(table)

        # Failed tests details
        failed_tests = [r for r in self.results if not r["success"]]
        if failed_tests:
            console.print(f"\n❌ [bold red]Failed Tests Details ({len(failed_tests)} failures):[/bold red]")
            for result in failed_tests:
                console.print(f"  • {result['test_id']}: {result['error']}")

        # Recommendations
        if failed > 0:
            console.print("\n💡 [bold yellow]Recommendations:[/bold yellow]")
            console.print("  • Check if failing endpoints require different parameters")
            console.print("  • Verify API documentation for endpoint requirements")
            console.print("  • Consider rate limiting if getting 429 errors")
            console.print("  • Some 404 errors are normal for date ranges with no data")

        console.print(f"\n🎯 [bold green]Ready for extraction![/bold green] {successful}/{total_tests} endpoints working.")


async def main():
    """Main testing function."""
    console.print(Panel.fit(
        "[bold blue]PNCP Endpoint Tester[/bold blue]\n"
        "Validates all configured PNCP API endpoints",
        border_style="blue"
    ))

    async with EndpointTester() as tester:
        await tester.test_all_endpoints()
        tester.print_results()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n🛑 [bold red]Testing interrupted by user[/bold red]")
    except Exception as e:
        console.print(f"\n❌ [bold red]Testing failed: {e}[/bold red]")
