"""BALIZA UI Components - Reusable Terminal Interface Elements

Provides consistent, beautiful UI components for the BALIZA CLI.
"""

import time
from typing import Any

from rich.align import Align
from rich.box import ROUNDED
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from .theme import get_console, get_theme


def create_header(
    title: str,
    subtitle: str | None = None,
    icon: str | None = None,
    width: int | None = None,
) -> Panel:
    """Create a beautiful header panel with title, subtitle, and icon."""
    console = get_console()

    # Build header content
    if icon:
        header_text = f"{icon} [header]{title}[/header]"
    else:
        header_text = f"[header]{title}[/header]"

    if subtitle:
        header_text += f"\n[subheader]{subtitle}[/subheader]"

    return Panel(
        Align.center(header_text),
        box=ROUNDED,
        border_style="primary",
        width=width or console.size.width - 4,
    )


def create_table(
    title: str,
    columns: list[str],
    rows: list[list[Any]],
    show_header: bool = True,
    show_lines: bool = False,
    width: int | None = None,
) -> Table:
    """Create a beautiful table with consistent styling."""
    table = Table(
        title=title,
        box=ROUNDED,
        show_header=show_header,
        show_lines=show_lines,
        width=width,
    )

    # Add columns with consistent styling
    for column in columns:
        table.add_column(column, style="body", header_style="subheader")

    # Add rows
    for row in rows:
        formatted_row = []
        for cell in row:
            if isinstance(cell, (int, float)):
                formatted_row.append(format_number(cell))
            elif isinstance(cell, str) and cell.lower() in [
                "success",
                "completed",
                "failed",
                "pending",
                "error",
            ]:
                formatted_row.append(format_status_inline(cell))
            else:
                formatted_row.append(str(cell))
        table.add_row(*formatted_row)

    return table


def create_progress_bar(
    total: int | None = None, show_percentage: bool = True, show_eta: bool = True
) -> Progress:
    """Create a beautiful progress bar with optional features."""
    columns = [
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
    ]

    if show_percentage:
        columns.append(TextColumn("[progress.percentage]{task.percentage:>3.0f}%"))

    if total is not None:
        columns.append(MofNCompleteColumn())

    columns.append(TextColumn("│"))

    if show_eta:
        columns.append(TimeRemainingColumn())

    columns.append(TimeElapsedColumn())

    return Progress(*columns, console=get_console())


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format with appropriate color."""
    if size_bytes == 0:
        return "[muted]0 B[/muted]"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1

    if i == 0:  # Bytes
        return f"[size]{size_bytes:.0f} {size_names[i]}[/size]"
    elif i == 1:  # KB
        return f"[size]{size_bytes:.1f} {size_names[i]}[/size]"
    else:  # MB, GB, TB
        return f"[size]{size_bytes:.1f} {size_names[i]}[/size]"


def format_duration(seconds: int | float) -> str:
    """Format duration in human-readable format."""
    if seconds < 1:
        return f"[muted]{seconds:.2f}s[/muted]"
    elif seconds < 60:
        return f"[info]{seconds:.1f}s[/info]"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"[info]{minutes}m {secs}s[/info]"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"[info]{hours}h {minutes}m[/info]"


def format_number(number: int | float, compact: bool = False) -> str:
    """Format numbers with thousands separators and appropriate styling."""
    if isinstance(number, float):
        if compact and abs(number) >= 1000:
            return format_compact_number(number)
        return f"[number]{number:,.1f}[/number]"
    else:
        if compact and abs(number) >= 1000:
            return format_compact_number(number)
        return f"[number]{number:,}[/number]"


def format_compact_number(number: int | float) -> str:
    """Format large numbers in compact form (e.g., 1.2K, 3.4M)."""
    abs_number = abs(number)
    sign = "-" if number < 0 else ""

    if abs_number >= 1_000_000_000:
        return f"[number]{sign}{abs_number / 1_000_000_000:.1f}B[/number]"
    elif abs_number >= 1_000_000:
        return f"[number]{sign}{abs_number / 1_000_000:.1f}M[/number]"
    elif abs_number >= 1_000:
        return f"[number]{sign}{abs_number / 1_000:.1f}K[/number]"
    else:
        return f"[number]{sign}{abs_number}[/number]"


def format_percentage(value: float, total: float) -> str:
    """Format percentage with appropriate color coding."""
    if total == 0:
        return "[muted]0.0%[/muted]"

    percentage = (value / total) * 100

    if percentage >= 90:
        return f"[success]{percentage:.1f}%[/success]"
    elif percentage >= 70:
        return f"[warning]{percentage:.1f}%[/warning]"
    else:
        return f"[error]{percentage:.1f}%[/error]"


def format_status_inline(status: str) -> str:
    """Format status for inline display in tables."""
    theme = get_theme()

    status_map = {
        "pending": f"[warning]{theme.ICONS['pending']}[/warning]",
        "in_progress": f"[primary]{theme.ICONS['in_progress']}[/primary]",
        "completed": f"[success]{theme.ICONS['success']}[/success]",
        "success": f"[success]{theme.ICONS['success']}[/success]",
        "failed": f"[error]{theme.ICONS['error']}[/error]",
        "error": f"[error]{theme.ICONS['error']}[/error]",
        "warning": f"[warning]{theme.ICONS['warning']}[/warning]",
    }

    return status_map.get(status.lower(), f"[muted]{status}[/muted]")


def format_currency(amount: float, currency: str = "R$") -> str:
    """Format currency amounts with appropriate styling."""
    if amount >= 1_000_000:
        return f"[currency]{currency} {amount / 1_000_000:.1f}M[/currency]"
    elif amount >= 1_000:
        return f"[currency]{currency} {amount / 1_000:.1f}K[/currency]"
    else:
        return f"[currency]{currency} {amount:,.2f}[/currency]"


def create_info_panel(
    content: str, title: str | None = None, icon: str | None = None, style: str = "info"
) -> Panel:
    """Create an informational panel with icon and styling."""
    theme = get_theme()

    if icon:
        formatted_content = f"{icon} {content}"
    else:
        formatted_content = content

    return Panel(formatted_content, title=title, border_style=style, box=ROUNDED)


def create_quick_stats(stats: dict[str, Any]) -> Table:
    """Create a quick stats table with two columns."""
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Metric", style="subheader", width=20)
    table.add_column("Value", style="body")

    for key, value in stats.items():
        # Format value based on type
        if isinstance(value, (int, float)) and "size" in key.lower():
            formatted_value = format_file_size(int(value))
        elif isinstance(value, (int, float)) and "percent" in key.lower():
            formatted_value = f"[percentage]{value:.1f}%[/percentage]"
        elif isinstance(value, (int, float)):
            formatted_value = format_number(value)
        elif isinstance(value, str) and value.lower() in [
            "success",
            "failed",
            "pending",
        ]:
            formatted_value = format_status_inline(value)
        else:
            formatted_value = str(value)

        table.add_row(key.replace("_", " ").title(), formatted_value)

    return table


def create_action_list(
    actions: list[tuple[str, str]], title: str = "Quick Actions"
) -> Panel:
    """Create a list of quick actions with keyboard shortcuts."""
    theme = get_theme()
    action_text = []

    for shortcut, description in actions:
        action_text.append(f"   [primary]{shortcut}[/primary] {description}")

    content = "\n".join(action_text)

    return Panel(
        content,
        title=f"{theme.ICONS['menu']} {title}",
        border_style="primary",
        box=ROUNDED,
    )


class LiveProgress:
    """A live progress display with statistics and ETA."""

    def __init__(self, description: str, total: int | None = None):
        self.description = description
        self.total = total
        self.current = 0
        self.start_time = time.time()
        self.console = get_console()
        self.progress = create_progress_bar(description, total)
        self.task_id = None

    def __enter__(self):
        self.task_id = self.progress.add_task(self.description, total=self.total)
        self.progress.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()

    def update(self, advance: int = 1, description: str | None = None, **kwargs):
        """Update progress with optional new description and stats."""
        self.current += advance

        if description:
            kwargs["description"] = description

        self.progress.update(self.task_id, advance=advance, **kwargs)

    def set_total(self, total: int):
        """Update the total for the progress bar."""
        self.total = total
        self.progress.update(self.task_id, total=total)

    def get_elapsed_time(self) -> float:
        """Get elapsed time since progress started."""
        return time.time() - self.start_time

    def get_rate(self) -> float:
        """Get the current processing rate (items per second)."""
        elapsed = self.get_elapsed_time()
        if elapsed > 0:
            return self.current / elapsed
        return 0.0
