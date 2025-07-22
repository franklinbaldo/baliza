"""BALIZA UI Module - Beautiful Terminal Interface

This module provides a comprehensive UI framework for the BALIZA CLI,
implementing the design system and components defined in the UX/UI plan.
"""

from .components import (
    create_header,
    create_progress_bar,
    create_table,
    format_duration,
    format_file_size,
    format_number,
)
from .dashboard import Dashboard
from .errors import ErrorHandler
from .theme import BalızaTheme, get_console, get_theme

__all__ = [
    "BalızaTheme",
    "Dashboard",
    "ErrorHandler",
    "create_header",
    "create_progress_bar",
    "create_table",
    "format_duration",
    "format_file_size",
    "format_number",
    "get_console",
    "get_theme",
]
