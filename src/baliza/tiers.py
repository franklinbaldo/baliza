"""Feature tier classification system for Baliza CLI.

This module defines the 4-tier hierarchy used to prioritize and organize
features based on criticality to the project's mission.

Tier 0: Critical Path - Without these, tool is useless
Tier 1: Core Features - Essential for production
Tier 2: Operator Experience - Quality-of-life enhancements
Tier 3: Future Enhancements - Aspirational features
"""

from __future__ import annotations

from enum import Enum
from typing import Callable, TypeVar

import typer

F = TypeVar("F", bound=Callable)


class FeatureTier(Enum):
    """Feature tier classification."""

    TIER_0 = ("0", "🔴 Critical", "Without these, tool is useless")
    TIER_1 = ("1", "🟠 Core", "Essential for production use")
    TIER_2 = ("2", "🟡 Enhanced", "Quality-of-life improvements")
    TIER_3 = ("3", "⚪ Future", "Aspirational features")

    def __init__(self, level: str, badge: str, description: str) -> None:
        self.level = level
        self.badge = badge
        self.description = description


# Command-to-Tier mapping based on feature hierarchy analysis
COMMAND_TIERS = {
    # Tier 0: Critical Path (~650 LOC)
    "extract": FeatureTier.TIER_0,  # Basic extraction without gap detection
    "export": FeatureTier.TIER_0,  # Basic export functionality
    # Tier 1: Core Features (~1,000 LOC)
    "backfill": FeatureTier.TIER_1,  # Historical backfill
    "verify": FeatureTier.TIER_1,  # Gap detection and verification
    # Tier 2: Operator Experience (~590 LOC)
    "state": FeatureTier.TIER_2,  # Coverage tracking, history, gaps
    "tiers": FeatureTier.TIER_2,  # Show tier classification
}


def get_tier(command_name: str) -> FeatureTier:
    """Get the tier classification for a command.

    Args:
        command_name: Name of the CLI command

    Returns:
        FeatureTier enum value, defaults to TIER_2 if not explicitly mapped
    """
    return COMMAND_TIERS.get(command_name, FeatureTier.TIER_2)


def tier_badge(command_name: str) -> str:
    """Get the tier badge for display in help text.

    Args:
        command_name: Name of the CLI command

    Returns:
        Formatted badge string (e.g., "🔴 Critical")
    """
    tier = get_tier(command_name)
    return tier.badge


def enhance_help(command_name: str, help_text: str) -> str:
    """Add tier badge to command help text.

    Args:
        command_name: Name of the CLI command
        help_text: Original help text

    Returns:
        Help text with tier badge prepended
    """
    badge = tier_badge(command_name)
    return f"[{badge}] {help_text}"


def tier_decorator(tier: FeatureTier) -> Callable[[F], F]:
    """Decorator to mark a command with its tier classification.

    This is primarily for documentation and future feature toggling.
    Currently, all tiers are enabled by default.

    Args:
        tier: The FeatureTier classification

    Returns:
        Decorator function that adds tier metadata

    Example:
        @tier_decorator(FeatureTier.TIER_0)
        @app.command("extract")
        def extract(...):
            pass
    """

    def decorator(func: F) -> F:
        # Store tier metadata on the function
        setattr(func, "__baliza_tier__", tier)
        return func

    return decorator


# Convenience decorators for each tier
def tier0(func: F) -> F:
    """Mark command as Tier 0 (Critical Path)."""
    return tier_decorator(FeatureTier.TIER_0)(func)


def tier1(func: F) -> F:
    """Mark command as Tier 1 (Core Features)."""
    return tier_decorator(FeatureTier.TIER_1)(func)


def tier2(func: F) -> F:
    """Mark command as Tier 2 (Operator Experience)."""
    return tier_decorator(FeatureTier.TIER_2)(func)


def tier3(func: F) -> F:
    """Mark command as Tier 3 (Future Enhancements)."""
    return tier_decorator(FeatureTier.TIER_3)(func)


def list_commands_by_tier() -> dict[FeatureTier, list[str]]:
    """Group commands by their tier classification.

    Returns:
        Dictionary mapping tiers to command names
    """
    result: dict[FeatureTier, list[str]] = {
        FeatureTier.TIER_0: [],
        FeatureTier.TIER_1: [],
        FeatureTier.TIER_2: [],
        FeatureTier.TIER_3: [],
    }

    for cmd_name, tier in COMMAND_TIERS.items():
        result[tier].append(cmd_name)

    return result


def get_tier_summary() -> str:
    """Generate a summary of all commands organized by tier.

    Returns:
        Formatted string showing tier organization
    """
    commands_by_tier = list_commands_by_tier()
    lines = ["Baliza Feature Tiers:", ""]

    for tier in FeatureTier:
        commands = commands_by_tier[tier]
        if commands:
            lines.append(f"{tier.badge} - {tier.description}")
            for cmd in sorted(commands):
                lines.append(f"  • baliza {cmd}")
            lines.append("")

    return "\n".join(lines)
