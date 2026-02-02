from enum import Enum
from functools import wraps
from typing import Any, Callable


class FeatureTier(Enum):
    TIER_0 = ("Critical Path", "🔴")
    TIER_1 = ("Core Features", "🟠")
    TIER_2 = ("Operator Experience", "🟡")
    TIER_3 = ("Future Enhancements", "⚪")

    def __init__(self, description: str, icon: str):
        self.description = description
        self.icon = icon


def tier0(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    wrapper._tier = FeatureTier.TIER_0  # type: ignore[attr-defined]
    return wrapper


def tier1(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    wrapper._tier = FeatureTier.TIER_1  # type: ignore[attr-defined]
    return wrapper


def tier2(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    wrapper._tier = FeatureTier.TIER_2  # type: ignore[attr-defined]
    return wrapper


def tier3(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    wrapper._tier = FeatureTier.TIER_3  # type: ignore[attr-defined]
    return wrapper
