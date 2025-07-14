#!/usr/bin/env python3
"""
Baliza Test Runner - Comprehensive End-to-End Testing

This script runs the complete Baliza test suite to validate all functionality.
Uses Typer for a better command-line interface.
"""

#!/usr/bin/env python3
"""
Baliza Test Runner - Comprehensive End-to-End Testing

This script runs the complete Baliza test suite to validate all functionality.
Uses Typer for a better command-line interface.
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional

try:
    import typer
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.table import Table
except ImportError as e:
    print(f"Error importing required packages: {e}")
    print("Please install the required packages with:")
    print("pip install -r requirements-dev.txt")
    sys.exit(1)

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Initialize Typer app
app = typer.Typer(
    help="Baliza Test Runner - Run comprehensive end-to-end tests",
    no_args_is_help=True,
    add_completion=False,
)

# Initialize Rich console
console = Console()

# Test categories
TEST_CATEGORIES = [
    "federation",
    "integration",
    "notebook",
    "dbt",
    "actions",
    "archive",
]

# Test configurations
TEST_CONFIGS = {
    "federation": {
        "path": str(project_root / "tests" / "test_ia_federation.py"),
        "description": "Internet Archive Federation Tests",
    },
    "integration": {
        "path": str(project_root / "tests" / "test_integration.py"),
        "description": "Data Pipeline Integration Tests",
    },
    "notebook": {
        "path": str(project_root / "tests" / "test_colab_notebook.py"),
        "description": "Google Colab Notebook Tests",
    },
    "dbt": {
        "path": str(project_root / "tests" / "test_dbt_models.py"),
        "description": "DBT Coverage Models Tests",
    },
    "actions": {
        "path": str(project_root / "tests" / "test_github_actions.py"),
        "description": "GitHub Actions Workflow Tests",
    },
    "archive": {
        "path": str(project_root / "tests" / "test_archive_first_flow.py"),
        "description": "Archive-First Data Flow Tests",
    },
}


def run_command(cmd: List[str], description: str) -> bool:
    """Run a command and return success status with rich output."""
    with console.status(f"[bold blue]🚀 Running {description}...") as status:
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
            )
            console.print(f"✅ [bold green]PASSED:[/] {description}")
            if result.stdout:
                console.print("\n[bold]Output:[/]")
                console.print(result.stdout, style="dim")
            return True
        except subprocess.CalledProcessError as e:
            console.print(f"❌ [bold red]FAILED:[/] {description}")
            if e.stdout:
                console.print("\n[bold]Output:[/]")
                console.print(e.stdout, style="dim")
            if e.stderr:
                console.print("\n[bold red]Error:[/]")
                console.print(e.stderr, style="red")
            return False


def get_test_configs(
    category: Optional[str] = None, quick: bool = False, integration: bool = False
) -> List[Dict]:
    """Get the appropriate test configurations based on the selected options."""
    base_cmd = ["uv", "run", "pytest"]
    test_configs = []

    if category:
        # Run specific category
        if category not in TEST_CONFIGS:
            console.print(f"[red]Error:[/] Unknown test category: {category}")
            raise typer.Exit(code=1)
        
        test_configs.append(
            {
                "cmd": base_cmd + [TEST_CONFIGS[category]["path"]],
                "description": TEST_CONFIGS[category]["description"],
            }
        )
    elif quick:
        # Quick validation tests
        test_configs.append(
            {
                "cmd": base_cmd
                + [
                    "tests.test_ia_federation::test_federation_init",
                    "tests.test_integration::test_database_initialization",
                    "tests.test_colab_notebook::test_notebook_exists",
                ],
                "description": "Quick Validation Tests",
            }
        )
    elif integration:
        # Integration tests
        test_configs.extend(
            [
                {
                    "cmd": base_cmd + [str(project_root / "tests" / "test_integration.py"), "--run-integration"],
                    "description": "Integration Tests",
                },
                {
                    "cmd": base_cmd
                    + [str(project_root / "tests" / "test_archive_first_flow.py"), "--run-integration"],
                    "description": "Archive-First Integration Tests",
                },
            ]
        )
    else:
        # Full test suite (default)
        test_configs.extend(
            [
                {"cmd": base_cmd + [config["path"]], "description": config["description"]}
                for config in TEST_CONFIGS.values()
            ]
        )
        # Special case for archive flow tests
        test_configs.append(
            {
                "cmd": base_cmd + [str(project_root / "tests" / "test_archive_first_flow.py::TestArchiveFirstFlow")],
                "description": "Archive-First Data Flow Tests (Detailed)",
            }
        )

    return test_configs


@app.command("run")
def run_tests(
    category: Optional[str] = typer.Option(
        None,
        "--category",
        "-c",
        help="Run a specific test category",
        case_sensitive=False,
        show_choices=True,
    ),
    quick: bool = typer.Option(
        False, "--quick", "-q", help="Run only quick validation tests"
    ),
    integration: bool = typer.Option(
        False, "--integration", "-i", help="Run integration tests"
    ),
    performance: bool = typer.Option(
        False, "--performance", "-p", help="Run performance tests"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output"
    ),
):
    """Run the Baliza test suite with the specified options."""
    # Set up base command
    base_cmd = ["uv", "run", "pytest"]
    if verbose:
        base_cmd.append("-v")

    # Handle performance tests separately
    if performance:
        test_configs = [
            {
                "cmd": base_cmd + [str(project_root / "tests"), "-m", "performance", "--run-performance"],
                "description": "Performance Tests",
            }
        ]
    else:
        test_configs = get_test_configs(category=category, quick=quick, integration=integration)

    # Print test plan
    console.print(Panel.fit("🧪 [bold blue]BALIZA END-TO-END TESTING SUITE[/]"))
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Test Suite", style="cyan")
    table.add_column("Description")
    
    for config in test_configs:
        table.add_row(
            config["description"].split(" ")[0],
            config["description"]
        )
    
    console.print(table)
    console.print(f"\n🎯 [bold]Test mode:[/] {'quick' if quick else 'full comprehensive' if not category else category}")

    # Run tests
    passed = 0
    failed = 0
    results = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Running tests...", total=len(test_configs))
        
        for config in test_configs:
            success = run_command(config["cmd"], config["description"])
            results.append((config["description"], success))
            if success:
                passed += 1
            else:
                failed += 1
            progress.update(task, advance=1)

    # Print summary
    console.print("\n" + "=" * 60)
    console.print("📊 [bold]TEST EXECUTION SUMMARY[/]")
    console.print("=" * 60)
    
    for desc, success in results:
        status = "✅ [green]PASSED[/]" if success else "❌ [red]FAILED[/]"
        console.print(f"{status} {desc}")
    
    console.print("\n" + "=" * 60)
    console.print(f"✅ [green]Passed:[/] {passed}")
    console.print(f"❌ [red]Failed:[/] {failed}")
    
    if passed + failed > 0:
        success_rate = (passed / (passed + failed)) * 100
        console.print(f"📈 [bold]Success Rate:[/] {success_rate:.1f}%")
    else:
        console.print("📊 [yellow]No tests were executed.[/]")

    if failed == 0 and passed > 0:
        console.print("\n🎉 [bold green]ALL TESTS PASSED![/] Baliza is ready for deployment.")
        raise typer.Exit(code=0)
    elif failed > 0:
        console.print(
            f"\n⚠️  [bold red]{failed} test suite(s) failed.[/] Review the output above for details."
        )
        raise typer.Exit(code=1)
    else:
        console.print("\nℹ️  No tests were executed. Use --help to see available options.")
        raise typer.Exit(code=0)


@app.command("list")
def list_categories():
    """List all available test categories."""
    console.print("\n[bold]Available Test Categories:[/]\n")
    
    for category, config in TEST_CONFIGS.items():
        console.print(f"  • [cyan]{category:<12}[/] - {config['description']}")
    
    console.print("\nRun with [bold]--help[/] for more options.")


if __name__ == "__main__":
    app()
