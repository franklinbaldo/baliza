#!/usr/bin/env python3
"""
PNCP API Stress Testing Script

This script systematically tests different concurrency and rate limiting
configurations to find optimal values that maximize throughput while
avoiding 429 rate limit errors.

Usage:
    uv run python scripts/stress_test_pncp.py
"""

import asyncio
import json
import logging
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.table import Table

# Configure logging
logging.basicConfig(level=logging.WARNING)  # Reduce noise during testing
logger = logging.getLogger(__name__)

console = Console()


@dataclass
class TestResult:
    """Results from a single test configuration."""

    concurrency: int
    rate_limit_delay: float
    total_requests: int
    successful_requests: int
    failed_requests: int
    rate_limit_errors: int  # 429 errors
    avg_response_time: float
    success_rate: float
    requests_per_second: float
    test_duration: float
    errors: list[str]


@dataclass
class TestConfig:
    """Configuration for a stress test."""

    concurrency: int
    rate_limit_delay: float
    test_duration: int = 60  # seconds
    base_url: str = "https://pncp.gov.br/api/consulta"


class PNCPStressTester:
    """Stress tester for PNCP API."""

    def __init__(self):
        self.results: list[TestResult] = []

    async def test_configuration(self, config: TestConfig) -> TestResult:
        """Test a specific configuration."""
        console.print(
            f"🧪 Testing: concurrency={config.concurrency}, delay={config.rate_limit_delay}s"
        )

        semaphore = asyncio.Semaphore(config.concurrency)
        start_time = time.time()
        end_time = start_time + config.test_duration

        # Test endpoints - use lightweight endpoints for testing
        test_endpoints = [
            "/v1/contratacoes/publicacao?dataInicial=20241201&dataFinal=20241201&pagina=1&tamanhoPagina=10&codigoModalidadeContratacao=6",
            "/v1/contratos?dataInicial=20241201&dataFinal=20241201&pagina=1&tamanhoPagina=10",
            "/v1/atas?dataInicial=20241201&dataFinal=20241201&pagina=1&tamanhoPagina=10",
        ]

        results = []
        tasks = []

        async with httpx.AsyncClient(
            base_url=config.base_url,
            timeout=30,
            headers={
                "User-Agent": "BALIZA/3.0-StressTest (Research purposes)",
                "Accept": "application/json",
            },
        ) as client:
            endpoint_cycle = 0

            while time.time() < end_time:
                # Create request task
                endpoint = test_endpoints[endpoint_cycle % len(test_endpoints)]
                task = asyncio.create_task(
                    self._make_request(
                        client, endpoint, semaphore, config.rate_limit_delay
                    )
                )
                tasks.append(task)
                endpoint_cycle += 1

                # Small delay to prevent overwhelming task creation
                await asyncio.sleep(0.01)

                # Collect completed tasks periodically
                if len(tasks) >= 50:
                    done_tasks = [t for t in tasks if t.done()]
                    for task in done_tasks:
                        try:
                            result = await task
                            results.append(result)
                        except Exception as e:
                            logger.exception(f"Task error: {e}")
                        tasks.remove(task)

            # Wait for remaining tasks with timeout
            if tasks:
                try:
                    remaining_results = await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=10
                    )
                    for result in remaining_results:
                        if isinstance(result, dict):
                            results.append(result)
                except TimeoutError:
                    console.print("⚠️ Some requests timed out during cleanup")

        # Analyze results
        return self._analyze_results(config, results, time.time() - start_time)

    async def _make_request(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        semaphore: asyncio.Semaphore,
        rate_limit_delay: float,
    ) -> dict[str, Any]:
        """Make a single request with rate limiting."""
        async with semaphore:
            # Rate limiting delay
            await asyncio.sleep(rate_limit_delay)

            start_time = time.time()
            try:
                response = await client.get(endpoint)
                response_time = time.time() - start_time

                return {
                    "success": response.status_code in [200, 204],
                    "status_code": response.status_code,
                    "response_time": response_time,
                    "error": None
                    if response.status_code in [200, 204]
                    else f"HTTP {response.status_code}",
                    "endpoint": endpoint,
                }
            except Exception as e:
                response_time = time.time() - start_time
                return {
                    "success": False,
                    "status_code": 0,
                    "response_time": response_time,
                    "error": str(e),
                    "endpoint": endpoint,
                }

    def _analyze_results(
        self, config: TestConfig, results: list[dict], duration: float
    ) -> TestResult:
        """Analyze test results and create summary."""
        if not results:
            return TestResult(
                concurrency=config.concurrency,
                rate_limit_delay=config.rate_limit_delay,
                total_requests=0,
                successful_requests=0,
                failed_requests=0,
                rate_limit_errors=0,
                avg_response_time=0.0,
                success_rate=0.0,
                requests_per_second=0.0,
                test_duration=duration,
                errors=["No results collected"],
            )

        total_requests = len(results)
        successful_requests = sum(1 for r in results if r["success"])
        failed_requests = total_requests - successful_requests
        rate_limit_errors = sum(1 for r in results if r["status_code"] == 429)

        response_times = [r["response_time"] for r in results if r["response_time"] > 0]
        avg_response_time = statistics.mean(response_times) if response_times else 0.0

        success_rate = (
            (successful_requests / total_requests) * 100 if total_requests > 0 else 0.0
        )
        requests_per_second = total_requests / duration if duration > 0 else 0.0

        # Collect unique errors
        errors = list({r["error"] for r in results if r["error"]})

        return TestResult(
            concurrency=config.concurrency,
            rate_limit_delay=config.rate_limit_delay,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            rate_limit_errors=rate_limit_errors,
            avg_response_time=avg_response_time,
            success_rate=success_rate,
            requests_per_second=requests_per_second,
            test_duration=duration,
            errors=errors,
        )

    async def run_stress_tests(self) -> list[TestResult]:
        """Run stress tests - start AGGRESSIVE to find 429 breaking point, then work backwards."""
        console.print("🚀 [bold blue]Starting PNCP API Stress Tests[/bold blue]")
        console.print(
            "Strategy: START AGGRESSIVE (guaranteed 429s) → work backwards to find optimal limits"
        )
        console.print()

        # STRATEGY: Start with settings that will definitely cause 429 errors
        test_configs = []

        # Phase 1: EXTREME AGGRESSIVE (guaranteed 429 errors)
        extreme_configs = [
            TestConfig(
                concurrency=20, rate_limit_delay=0.0, test_duration=15
            ),  # Maximum pressure
            TestConfig(
                concurrency=15, rate_limit_delay=0.0, test_duration=15
            ),  # Very high pressure
            TestConfig(
                concurrency=10, rate_limit_delay=0.0, test_duration=15
            ),  # High pressure, no delay
            TestConfig(
                concurrency=10, rate_limit_delay=0.05, test_duration=15
            ),  # High pressure, minimal delay
            TestConfig(
                concurrency=8, rate_limit_delay=0.0, test_duration=15
            ),  # High concurrency, no delay
        ]

        # Phase 2: High pressure (likely 429 errors)
        high_pressure_configs = [
            TestConfig(concurrency=8, rate_limit_delay=0.1, test_duration=15),
            TestConfig(concurrency=6, rate_limit_delay=0.1, test_duration=15),
            TestConfig(concurrency=5, rate_limit_delay=0.1, test_duration=15),
            TestConfig(concurrency=4, rate_limit_delay=0.1, test_duration=15),
            TestConfig(concurrency=3, rate_limit_delay=0.1, test_duration=15),
        ]

        # Phase 3: Medium pressure (some 429s expected)
        medium_configs = [
            TestConfig(concurrency=5, rate_limit_delay=0.25, test_duration=15),
            TestConfig(concurrency=4, rate_limit_delay=0.25, test_duration=15),
            TestConfig(concurrency=3, rate_limit_delay=0.25, test_duration=15),
            TestConfig(concurrency=2, rate_limit_delay=0.1, test_duration=15),
            TestConfig(concurrency=2, rate_limit_delay=0.2, test_duration=15),
        ]

        # Phase 4: Working backwards to find sweet spot
        working_back_configs = [
            TestConfig(concurrency=3, rate_limit_delay=0.5, test_duration=15),
            TestConfig(concurrency=2, rate_limit_delay=0.3, test_duration=15),
            TestConfig(concurrency=2, rate_limit_delay=0.5, test_duration=15),
            TestConfig(concurrency=1, rate_limit_delay=0.2, test_duration=15),
            TestConfig(concurrency=1, rate_limit_delay=0.5, test_duration=15),
        ]

        test_configs = (
            extreme_configs
            + high_pressure_configs
            + medium_configs
            + working_back_configs
        )

        console.print(f"📊 Running {len(test_configs)} test configurations...")
        console.print(
            "Each test runs for 15 seconds (shorter duration for faster iteration)"
        )
        console.print()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Running stress tests", total=len(test_configs))

            for i, config in enumerate(test_configs):
                progress.update(
                    task,
                    description=f"Test {i + 1}/{len(test_configs)}: c={config.concurrency}, d={config.rate_limit_delay}s",
                )

                result = await self.test_configuration(config)
                self.results.append(result)

                progress.advance(task)

                # Brief pause between tests to avoid overwhelming the API
                await asyncio.sleep(2)

        console.print("✅ [bold green]Stress tests completed![/bold green]")
        return self.results

    def analyze_and_report(self):
        """Analyze results and generate recommendations."""
        if not self.results:
            console.print("❌ No results to analyze")
            return

        # Filter out completely failed tests
        valid_results = [r for r in self.results if r.total_requests > 0]

        if not valid_results:
            console.print("❌ No valid results to analyze")
            return

        # Create results table
        table = Table(title="PNCP API Stress Test Results")
        table.add_column("Concurrency", style="cyan")
        table.add_column("Delay (s)", style="yellow")
        table.add_column("Total Req", style="blue")
        table.add_column("Success Rate", style="green")
        table.add_column("429 Errors", style="red")
        table.add_column("Req/s", style="magenta")
        table.add_column("Avg Response", style="white")
        table.add_column("Status", style="bold")

        for result in sorted(valid_results, key=lambda x: x.success_rate, reverse=True):
            status = (
                "✅ Excellent"
                if result.success_rate >= 95 and result.rate_limit_errors == 0
                else "🟡 Good"
                if result.success_rate >= 90 and result.rate_limit_errors <= 5
                else "🔴 Poor"
            )

            table.add_row(
                str(result.concurrency),
                f"{result.rate_limit_delay:.2f}",
                str(result.total_requests),
                f"{result.success_rate:.1f}%",
                str(result.rate_limit_errors),
                f"{result.requests_per_second:.2f}",
                f"{result.avg_response_time:.3f}s",
                status,
            )

        console.print(table)
        console.print()

        # Find optimal configurations
        excellent_configs = [
            r
            for r in valid_results
            if r.success_rate >= 95 and r.rate_limit_errors == 0
        ]

        if excellent_configs:
            # Sort by throughput (requests per second)
            best_throughput = max(
                excellent_configs, key=lambda x: x.requests_per_second
            )
            # Sort by lowest latency
            best_latency = min(excellent_configs, key=lambda x: x.avg_response_time)

            console.print("🎯 [bold green]Recommended Configurations[/bold green]")
            console.print()
            console.print(
                f"🚀 **Best Throughput**: concurrency={best_throughput.concurrency}, delay={best_throughput.rate_limit_delay}s"
            )
            console.print(
                f"   └── {best_throughput.requests_per_second:.2f} req/s, {best_throughput.success_rate:.1f}% success rate"
            )
            console.print()
            console.print(
                f"⚡ **Best Latency**: concurrency={best_latency.concurrency}, delay={best_latency.rate_limit_delay}s"
            )
            console.print(
                f"   └── {best_latency.avg_response_time:.3f}s avg response, {best_latency.success_rate:.1f}% success rate"
            )
            console.print()

            # Balanced recommendation
            balanced_configs = sorted(
                excellent_configs,
                key=lambda x: x.requests_per_second * (100 - x.avg_response_time * 100),
                reverse=True,
            )
            if balanced_configs:
                balanced = balanced_configs[0]
                console.print(
                    f"⚖️ **Balanced (Recommended)**: concurrency={balanced.concurrency}, delay={balanced.rate_limit_delay}s"
                )
                console.print(
                    f"   └── {balanced.requests_per_second:.2f} req/s, {balanced.avg_response_time:.3f}s response, {balanced.success_rate:.1f}% success"
                )
        else:
            console.print(
                "⚠️ [yellow]No configurations achieved 95%+ success rate with 0 rate limit errors[/yellow]"
            )
            # Find best available
            best_available = max(valid_results, key=lambda x: x.success_rate)
            console.print(
                f"📊 **Best Available**: concurrency={best_available.concurrency}, delay={best_available.rate_limit_delay}s"
            )
            console.print(
                f"   └── {best_available.success_rate:.1f}% success rate, {best_available.rate_limit_errors} rate limit errors"
            )

    def save_results(self, filename: str = "pncp_stress_test_results.json"):
        """Save results to JSON file."""
        results_data = {
            "test_timestamp": datetime.now().isoformat(),
            "test_summary": {
                "total_configurations_tested": len(self.results),
                "total_requests_made": sum(r.total_requests for r in self.results),
            },
            "results": [asdict(result) for result in self.results],
        }

        results_file = Path("data") / filename
        results_file.parent.mkdir(exist_ok=True)

        with open(results_file, "w") as f:
            json.dump(results_data, f, indent=2)

        console.print(f"📁 Results saved to: {results_file}")


async def main():
    """Main function to run stress tests."""
    tester = PNCPStressTester()

    try:
        await tester.run_stress_tests()
        tester.analyze_and_report()
        tester.save_results()

    except KeyboardInterrupt:
        console.print("\n⏹️ Tests interrupted by user")
        if tester.results:
            console.print("📊 Analyzing partial results...")
            tester.analyze_and_report()
            tester.save_results("pncp_stress_test_partial_results.json")
    except Exception as e:
        console.print(f"❌ Error during testing: {e}")
        logger.exception("Stress test failed")


if __name__ == "__main__":
    asyncio.run(main())
