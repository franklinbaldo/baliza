"""
Handles the 'transform' command by running dbt.
"""
import subprocess
import typer


def transform():
    """
    Runs the dbt transformation process.
    """
    try:
        process = subprocess.Popen(
            ["dbt", "run", "--project-dir", "dbt_baliza"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        if process.stdout:
            for line in process.stdout:
                typer.echo(line, nl=False)
        process.wait()
        if process.returncode != 0:
            raise typer.Exit(code=process.returncode)
    except FileNotFoundError:
        typer.secho(
            "dbt not found. Please ensure dbt is installed and in your PATH.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"An error occurred: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
