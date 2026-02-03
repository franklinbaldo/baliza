"""Feature Tier system for Baliza CLI.

Categorizes commands into 4 tiers based on criticality.
"""

from collections.abc import Callable
from enum import Enum
from functools import wraps
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class FeatureTier(Enum):
    TIER0 = (0, "Critical Path", "🔴", "Minimum viable functionality")
    TIER1 = (1, "Core Features", "🟠", "Essential for production")
    TIER2 = (2, "Operator Experience", "🟡", "Quality-of-life enhancements")
    TIER3 = (3, "Future Enhancements", "⚪", "Aspirational features")

    def __init__(self, level: int, label: str, badge: str, description: str):
        self.level = level
        self.label = label
        self.badge = badge
        self.description = description


def tier_decorator(tier: FeatureTier) -> Callable[[F], F]:
    """Decorator to mark a command with its feature tier."""

    def decorator(func: F) -> F:
        func._feature_tier = tier

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper  # type: ignore

    return decorator


tier0 = tier_decorator(FeatureTier.TIER0)
tier1 = tier_decorator(FeatureTier.TIER1)
tier2 = tier_decorator(FeatureTier.TIER2)
tier3 = tier_decorator(FeatureTier.TIER3)

COMMAND_TIERS = {
    "extract": FeatureTier.TIER0,
    "export": FeatureTier.TIER0,
    "export-daily": FeatureTier.TIER1,
    "verify": FeatureTier.TIER1,
    "backfill": FeatureTier.TIER1,
    "status": FeatureTier.TIER2,
    "state": FeatureTier.TIER2,
    "buffer-stats": FeatureTier.TIER2,
}
