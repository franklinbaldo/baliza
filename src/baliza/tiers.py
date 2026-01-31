"""Tier system for Baliza features and commands."""

from collections.abc import Callable
from enum import Enum
from typing import TypeVar

from rich.table import Table

T = TypeVar("T", bound=Callable)


class FeatureTier(Enum):
    TIER0 = ("Tier 0", "Critical Path", "🔴")
    TIER1 = ("Tier 1", "Core Features", "🟠")
    TIER2 = ("Tier 2", "Operator Experience", "🟡")
    TIER3 = ("Tier 3", "Future Enhancements", "⚪")

    def __init__(self, label: str, description: str, emoji: str):
        self.label = label
        self.description = description
        self.emoji = emoji


# Store for command-to-tier mapping
COMMAND_TIERS: dict[str, FeatureTier] = {}


def _mark_tier(tier: FeatureTier) -> Callable[[T], T]:
    """Internal decorator to mark a function with a tier."""

    def decorator(func: T) -> T:
        # Use function name as default identifier
        COMMAND_TIERS[func.__name__] = tier
        # Also store on the function itself for inspection
        func._baliza_tier = tier
        return func

    return decorator


# Public decorators
tier0 = _mark_tier(FeatureTier.TIER0)
tier1 = _mark_tier(FeatureTier.TIER1)
tier2 = _mark_tier(FeatureTier.TIER2)
tier3 = _mark_tier(FeatureTier.TIER3)


def get_tier_summary() -> Table:
    """Generate a Rich table summarizing command tiers."""
    table = Table(title="Baliza Feature Hierarchy", box=None)
    table.add_column("Tier", style="bold")
    table.add_column("Command", style="cyan")
    table.add_column("Description")

    # Sort commands by tier then name
    sorted_commands = sorted(
        COMMAND_TIERS.items(), key=lambda x: (x[1].value[0], x[0])
    )

    current_tier = None
    for cmd_name, tier in sorted_commands:
        if tier != current_tier:
            if current_tier is not None:
                table.add_section()
            current_tier = tier
            table.add_row(f"{tier.emoji} {tier.label}", "", f"[italic]{tier.description}[/italic]")

        # Humanize command names (kebab-case)
        display_name = cmd_name.replace("_", "-")
        table.add_row("", display_name, "")

    return table
