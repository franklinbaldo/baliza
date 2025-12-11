from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass
from rich.console import Console
from rich.table import Table
from rich import box

@dataclass
class MockRun:
    run_id: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    windows_completed: int
    rows_extracted: int
    error_message: Optional[str] = None

history = [
    MockRun(
        run_id="run_12345",
        status="completed",
        started_at=datetime.now() - timedelta(hours=2),
        completed_at=datetime.now() - timedelta(hours=1, minutes=30),
        windows_completed=10,
        rows_extracted=1500,
        error_message=None
    ),
    MockRun(
        run_id="run_67890",
        status="failed",
        started_at=datetime.now() - timedelta(days=1),
        completed_at=None,
        windows_completed=5,
        rows_extracted=200,
        error_message="Network timeout"
    ),
    MockRun(
        run_id="run_54321",
        status="running",
        started_at=datetime.now() - timedelta(minutes=10),
        completed_at=None,
        windows_completed=2,
        rows_extracted=50,
        error_message=None
    )
]

console = Console()

def show_history_table(history):
    table = Table(title="Run History", box=box.ROUNDED)

    table.add_column("Run ID", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Started", style="dim")
    table.add_column("Duration")
    table.add_column("Windows", justify="right")
    table.add_column("Rows", justify="right")
    table.add_column("Error", style="red")

    for run in history:
        status_style = {
            "completed": "green",
            "failed": "red",
            "running": "yellow",
        }.get(run.status, "white")

        duration = "-"
        if run.completed_at:
            duration = str(run.completed_at - run.started_at)
        elif run.status == "running":
             duration = f"Running for {datetime.now() - run.started_at}"

        table.add_row(
            run.run_id,
            f"[{status_style}]{run.status}[/{status_style}]",
            run.started_at.strftime("%Y-%m-%d %H:%M"),
            duration,
            str(run.windows_completed),
            f"{run.rows_extracted:,}",
            run.error_message or "-"
        )

    console.print(table)

show_history_table(history)
