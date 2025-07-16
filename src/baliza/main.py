import subprocess
import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def run_dbt(
    start_date: str = typer.Option("2024-01-01", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option("2024-01-31", help="End date (YYYY-MM-DD)"),
):
    """
    Runs the dbt project, passing start_date and end_date as variables.
    """
    console.print(f"🚀 Running dbt for date range: [bold cyan]{start_date}[/] to [bold cyan]{end_date}[/]")

    dbt_vars = {
        "start_date": start_date,
        "end_date": end_date,
    }

    # Construct the dbt run command
    command = [
        "uv",
        "run",
        "dbt",
        "run",
        "--project-dir",
        "dbt_baliza",
        "--vars",
        str(dbt_vars),
    ]

    try:
        # Execute the command
        subprocess.run(command, check=True)
        console.print("✅ [bold green]dbt run completed successfully![/bold green]")
    except subprocess.CalledProcessError as e:
        console.print(f"❌ [bold red]dbt run failed with exit code {e.returncode}[/bold red]")
        raise typer.Exit(1)
    except FileNotFoundError:
        console.print("❌ [bold red]Error: 'uv' or 'dbt' command not found. Is dbt installed in the environment?[/bold red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
