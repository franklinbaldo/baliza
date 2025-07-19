"""BALIZA Theme System - Design System and Visual Standards

Implements consistent colors, typography, icons, and styling across the CLI.
"""

from dataclasses import dataclass
from typing import ClassVar

from rich.console import Console
from rich.theme import Theme


@dataclass
class BalızaTheme:
    """BALIZA design system with colors, icons, and styling standards."""

    # Color Palette
    COLORS: ClassVar = {
        # Primary Colors
        "primary": "#3B82F6",  # Blue - Information, progress
        "success": "#10B981",  # Green - Success, completion
        "warning": "#F59E0B",  # Yellow - Warnings, attention
        "error": "#EF4444",  # Red - Errors, failures
        "info": "#8B5CF6",  # Purple - Special features, metadata
        # Neutral Colors
        "white": "#FFFFFF",  # Primary text
        "gray": "#6B7280",  # Secondary text, borders
        "dark_gray": "#374151",  # Backgrounds, containers
        "light_gray": "#F3F4F6",  # Light backgrounds
        # Status Colors
        "pending": "#F59E0B",  # Yellow - Pending operations
        "in_progress": "#3B82F6",  # Blue - Active operations
        "completed": "#10B981",  # Green - Completed operations
        "failed": "#EF4444",  # Red - Failed operations
    }

    # Icon Mappings
    ICONS: ClassVar = {
        # Data & Analytics
        "data": "📊",
        "analytics": "📈",
        "database": "💾",
        "storage": "🗄️",
        "archive": "📦",
        # Processing & Operations
        "process": "🔄",
        "extract": "📥",
        "transform": "🔄",
        "load": "📤",
        "build": "🏗️",
        "clean": "🧹",
        # Status & Feedback
        "success": "✅",
        "warning": "⚠️",
        "error": "❌",
        "info": "💡",
        "pending": "⏳",
        "in_progress": "🔄",
        # Navigation & UI
        "menu": "📋",
        "search": "🔍",
        "filter": "🎯",
        "settings": "⚙️",
        "help": "❓",
        "tutorial": "🎓",
        # System & Performance
        "performance": "🚀",
        "optimization": "⚡",
        "monitoring": "📊",
        "health": "🏥",
        "security": "🔒",
        # Business Domain
        "government": "🏛️",
        "contract": "📜",
        "procurement": "🛒",
        "spending": "💰",
        "agency": "🏢",
        "supplier": "🏪",
    }

    # Rich Theme for Consistent Styling
    RICH_THEME = Theme(
        {
            # Primary styles
            "primary": "#3B82F6",
            "success": "#10B981 bold",
            "warning": "#F59E0B bold",
            "error": "#EF4444 bold",
            "info": "#8B5CF6",
            # Text styles
            "header": "#FFFFFF bold",
            "subheader": "#6B7280 bold",
            "body": "#FFFFFF",
            "secondary": "#6B7280",
            "muted": "#374151",
            # Component styles
            "border": "#6B7280",
            "highlight": "#3B82F6 on #374151",
            "code": "#8B5CF6 on #374151",
            # Status styles
            "status.pending": "#F59E0B",
            "status.in_progress": "#3B82F6 bold",
            "status.completed": "#10B981 bold",
            "status.failed": "#EF4444 bold",
            # Data styles
            "number": "#3B82F6 bold",
            "percentage": "#10B981 bold",
            "currency": "#F59E0B bold",
            "size": "#8B5CF6",
        }
    )


# Global theme instance
_theme_instance: BalızaTheme | None = None


def get_theme() -> BalızaTheme:
    """Get the global BALIZA theme instance."""
    global _theme_instance
    if _theme_instance is None:
        _theme_instance = BalızaTheme()
    return _theme_instance


def get_console() -> Console:
    """Get a Rich console configured with BALIZA theme."""
    theme = get_theme()
    return Console(
        theme=theme.RICH_THEME, force_terminal=True, legacy_windows=False, stderr=False
    )


def get_icon(key: str, fallback: str = "•") -> str:
    """Get an icon by key with fallback."""
    theme = get_theme()
    return theme.ICONS.get(key, fallback)


def get_color(key: str, fallback: str = "#FFFFFF") -> str:
    """Get a color by key with fallback."""
    theme = get_theme()
    return theme.COLORS.get(key, fallback)


def format_status(status: str) -> str:
    """Format a status with appropriate icon and color."""
    theme = get_theme()

    status_map = {
        "pending": (theme.ICONS["pending"], "status.pending"),
        "in_progress": (theme.ICONS["in_progress"], "status.in_progress"),
        "completed": (theme.ICONS["success"], "status.completed"),
        "failed": (theme.ICONS["error"], "status.failed"),
        "success": (theme.ICONS["success"], "status.completed"),
        "error": (theme.ICONS["error"], "status.failed"),
        "warning": (theme.ICONS["warning"], "warning"),
    }

    icon, style = status_map.get(status.lower(), (theme.ICONS["info"], "info"))
    return f"[{style}]{icon} {status.replace('_', ' ').title()}[/{style}]"


def format_operation(operation: str) -> str:
    """Format an operation with appropriate icon."""
    theme = get_theme()

    operation_icons = {
        "extract": theme.ICONS["extract"],
        "transform": theme.ICONS["transform"],
        "load": theme.ICONS["load"],
        "build": theme.ICONS["build"],
        "clean": theme.ICONS["clean"],
        "analyze": theme.ICONS["analytics"],
        "monitor": theme.ICONS["monitoring"],
    }

    icon = operation_icons.get(operation.lower(), theme.ICONS["process"])
    return f"{icon} {operation.replace('_', ' ').title()}"


def format_category(category: str) -> str:
    """Format a data category with appropriate icon."""
    theme = get_theme()

    category_icons = {
        "contracts": theme.ICONS["contract"],
        "procurement": theme.ICONS["procurement"],
        "spending": theme.ICONS["spending"],
        "agencies": theme.ICONS["agency"],
        "suppliers": theme.ICONS["supplier"],
        "data": theme.ICONS["data"],
        "analytics": theme.ICONS["analytics"],
    }

    icon = category_icons.get(category.lower(), theme.ICONS["data"])
    return f"{icon} {category.replace('_', ' ').title()}"
