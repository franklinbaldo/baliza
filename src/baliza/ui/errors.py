"""BALIZA Error Handling - Beautiful Error Messages and Recovery

Provides contextual error messages with recovery suggestions and actions.
"""

import time
from collections.abc import Callable
from typing import Any

from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from .components import create_header, create_info_panel
from .theme import get_console, get_theme


class ErrorHandler:
    """Beautiful error handling with recovery suggestions."""

    def __init__(self):
        self.console = get_console()
        self.theme = get_theme()

    def handle_rate_limit_error(
        self,
        retry_count: int,
        max_retries: int,
        backoff_time: int,
        on_retry: Callable | None = None,
    ) -> bool:
        """Handle rate limit errors with beautiful backoff display."""

        # Create error header
        header = create_header(
            "Rate Limit Encountered",
            "PNCP API temporarily limited requests - this is normal",
            self.theme.ICONS["warning"],
        )

        self.console.print(header)

        # Explanation
        explanation = create_info_panel(
            "The PNCP API temporarily limited our requests to prevent overload.\n"
            "This is normal and expected during high-traffic periods.",
            title="🔍 What happened?",
            style="warning",
        )

        self.console.print(explanation)

        # Auto-recovery info
        recovery_content = [
            f"Backing off for {backoff_time}s... (Auto-retry {retry_count}/{max_retries})",
            "",
            "💡 We'll automatically:",
            "✅ Wait for the optimal retry window",
            "✅ Resume exactly where we left off",
            "✅ Continue with reduced request rate",
        ]

        recovery_panel = Panel(
            "\n".join(recovery_content),
            title=f"{self.theme.ICONS['process']} Automatic Recovery",
            border_style="info",
        )

        self.console.print(recovery_panel)

        # Progress bar for backoff
        with Progress(
            SpinnerColumn(),
            TextColumn("Waiting for retry window..."),
            console=self.console,
        ) as progress:
            task = progress.add_task("backoff", total=backoff_time)

            for _i in range(backoff_time):
                time.sleep(1)
                progress.update(task, advance=1)

        # Call retry callback if provided
        if on_retry:
            on_retry()

        return True

    def handle_api_error(
        self, error: Exception, endpoint: str, context: dict[str, Any] | None = None
    ) -> None:
        """Handle general API errors with context and suggestions."""

        header = create_header(
            "API Request Failed",
            f"Error communicating with {endpoint}",
            self.theme.ICONS["error"],
        )

        self.console.print(header)

        # Error details
        error_content = [
            f"Error: {str(error)[:100]}{'...' if len(str(error)) > 100 else ''}",
            f"Endpoint: {endpoint}",
        ]

        if context:
            error_content.append(f"Context: {context}")

        error_panel = Panel(
            "\n".join(error_content),
            title=f"{self.theme.ICONS['error']} Error Details",
            border_style="error",
        )

        self.console.print(error_panel)

        # Recovery suggestions
        suggestions = self._get_error_suggestions(error, endpoint)
        if suggestions:
            suggestions_panel = Panel(
                "\n".join(suggestions),
                title=f"{self.theme.ICONS['info']} Suggested Actions",
                border_style="info",
            )

            self.console.print(suggestions_panel)

    def handle_database_error(self, error: Exception, operation: str) -> None:
        """Handle database-related errors."""

        header = create_header(
            "Database Error",
            f"Error during {operation} operation",
            self.theme.ICONS["error"],
        )

        self.console.print(header)

        # Check for common database issues
        error_str = str(error).lower()

        if "database is locked" in error_str:
            self._handle_database_lock_error()
        elif "no such table" in error_str:
            self._handle_missing_table_error()
        elif "disk" in error_str or "space" in error_str:
            self._handle_disk_space_error()
        else:
            self._handle_generic_database_error(error, operation)

    def _handle_database_lock_error(self) -> None:
        """Handle database lock errors."""
        content = [
            "The database is currently locked by another process.",
            "",
            "💡 Try these solutions:",
            "1. Wait a moment and try again",
            "2. Check if another BALIZA instance is running",
            "3. Remove stale lock files: rm data/baliza.duckdb.lock",
            "4. Restart the process if the lock persists",
        ]

        panel = Panel(
            "\n".join(content),
            title=f"{self.theme.ICONS['warning']} Database Locked",
            border_style="warning",
        )

        self.console.print(panel)

    def _handle_missing_table_error(self) -> None:
        """Handle missing table errors."""
        content = [
            "Required database tables are missing.",
            "",
            "💡 This usually happens on first run or after database changes.",
            "",
            "✅ Solution: Run 'baliza extract' to initialize the database",
        ]

        panel = Panel(
            "\n".join(content),
            title=f"{self.theme.ICONS['info']} Database Setup Required",
            border_style="info",
        )

        self.console.print(panel)

    def _handle_disk_space_error(self) -> None:
        """Handle disk space errors."""
        content = [
            "Insufficient disk space for database operations.",
            "",
            "💡 Recommended actions:",
            "1. Free up disk space on your system",
            "2. Run 'baliza prune' to clean old data",
            "3. Consider moving the database to a larger drive",
            "4. Check available space: df -h",
        ]

        panel = Panel(
            "\n".join(content),
            title=f"{self.theme.ICONS['warning']} Disk Space Issue",
            border_style="warning",
        )

        self.console.print(panel)

    def _handle_generic_database_error(self, error: Exception, operation: str) -> None:
        """Handle generic database errors."""
        content = [
            f"Database error during {operation}:",
            f"{str(error)[:200]}{'...' if len(str(error)) > 200 else ''}",
            "",
            "💡 Try these solutions:",
            "1. Check database file permissions",
            "2. Verify database file integrity",
            "3. Restart the operation",
            "4. Contact support if the issue persists",
        ]

        panel = Panel(
            "\n".join(content),
            title=f"{self.theme.ICONS['error']} Database Error",
            border_style="error",
        )

        self.console.print(panel)

    def _get_error_suggestions(self, error: Exception, endpoint: str) -> list[str]:
        """Get context-specific error suggestions."""
        error_str = str(error).lower()
        suggestions = []

        if "timeout" in error_str:
            suggestions.extend(
                [
                    "🌐 Network timeout detected:",
                    "• Check your internet connection",
                    "• Reduce concurrency with --concurrency option",
                    "• Try again during off-peak hours",
                ]
            )

        elif "connection" in error_str:
            suggestions.extend(
                [
                    "🔌 Connection error:",
                    "• Verify internet connectivity",
                    "• Check if PNCP API is accessible",
                    "• Wait a moment and retry",
                ]
            )

        elif "unauthorized" in error_str or "forbidden" in error_str:
            suggestions.extend(
                [
                    "🔒 Authorization error:",
                    "• Check API credentials if required",
                    "• Verify endpoint permissions",
                    "• Contact PNCP support if issue persists",
                ]
            )

        elif "not found" in error_str:
            suggestions.extend(
                [
                    "🔍 Resource not found:",
                    "• Verify the endpoint URL is correct",
                    "• Check if the resource exists",
                    "• Try a different date range",
                ]
            )

        else:
            suggestions.extend(
                [
                    "🔧 General troubleshooting:",
                    "• Wait a moment and try again",
                    "• Check your internet connection",
                    "• Reduce request frequency",
                    "• Contact support if issue persists",
                ]
            )

        return suggestions

    def show_recovery_success(self, message: str) -> None:
        """Show successful recovery message."""
        panel = Panel(
            f"{self.theme.ICONS['success']} {message}",
            title="Recovery Successful",
            border_style="success",
        )

        self.console.print(panel)

    def show_warning(
        self, title: str, message: str, suggestions: list[str] | None = None
    ) -> None:
        """Show a warning message with optional suggestions."""
        content = [message]

        if suggestions:
            content.extend(["", "💡 Suggestions:"])
            content.extend(f"• {suggestion}" for suggestion in suggestions)

        panel = Panel(
            "\n".join(content),
            title=f"{self.theme.ICONS['warning']} {title}",
            border_style="warning",
        )

        self.console.print(panel)
