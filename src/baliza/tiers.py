"""Feature Tier system for Baliza."""

from collections.abc import Callable
from enum import Enum
from typing import TypeVar

T = TypeVar("T", bound=Callable)


class FeatureTier(Enum):
    TIER0 = ("Tier 0: Critical Path", "🔴", "Without these, tool is useless")
    TIER1 = ("Tier 1: Core Features", "🟠", "Essential for production, but tool works without them")
    TIER2 = ("Tier 2: Operator Experience", "🟡", "Quality-of-life that enhances usability")
    TIER3 = ("Tier 3: Future Enhancements", "⚪", "Aspirational features for later")

    def __init__(self, title: str, badge: str, description: str):
        self.title = title
        self.badge = badge
        self.description = description


COMMAND_TIERS = {}


def tier0(func: T) -> T:
    COMMAND_TIERS[func.__name__] = FeatureTier.TIER0
    return func


def tier1(func: T) -> T:
    COMMAND_TIERS[func.__name__] = FeatureTier.TIER1
    return func


def tier2(func: T) -> T:
    COMMAND_TIERS[func.__name__] = FeatureTier.TIER2
    return func


def tier3(func: T) -> T:
    COMMAND_TIERS[func.__name__] = FeatureTier.TIER3
    return func
