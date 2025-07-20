"""
PNCP Enum Utilities - Centralized enum management for BALIZA
"""

from enum import Enum
from typing import Type

# Enum utilities
def get_enum_by_value(enum_class: type[Enum], value: int | str) -> Enum | None:
    """Get enum member by value, returning None if not found."""
    try:
        return enum_class(value)
    except ValueError:
        return None


def get_enum_name_by_value(enum_class: type[Enum], value: int | str) -> str | None:
    """Get enum member name by value, returning None if not found."""
    enum_member = get_enum_by_value(enum_class, value)
    return enum_member.name if enum_member else None


def validate_enum_value(enum_class: type[Enum], value: int | str) -> bool:
    """Validate that a value exists in the enum."""
    return get_enum_by_value(enum_class, value) is not None


def get_enum_values(enum_class: type[Enum]) -> list[int]:
    """Get all values from an enum class."""
    return [member.value for member in enum_class]


def get_enum_choices(enum_class: type[Enum]) -> dict[int, str]:
    """Get all enum values with their names as a dictionary."""
    return {member.value: member.name for member in enum_class}


def get_enum_description(enum_class: type[Enum], value: int | str) -> str:
    """Get a human-readable description of an enum value."""
    enum_member = get_enum_by_value(enum_class, value)
    if not enum_member:
        return f"Unknown {enum_class.__name__} value: {value}"

    # Convert enum name to human-readable format
    name = enum_member.name.replace("_", " ").title()
    return f"{name} ({enum_member.value})"

# Enum registry for dynamic access
ENUM_REGISTRY = {}

def register_enum(enum_class: Type[Enum]):
    """Decorator to register an enum class in the registry."""
    ENUM_REGISTRY[enum_class.__name__] = enum_class
    return enum_class


def get_enum_by_name(enum_name: str) -> type[Enum] | None:
    """Get enum class by name."""
    return ENUM_REGISTRY.get(enum_name)


def get_all_enum_metadata() -> dict[str, dict[str, str | list[dict[str, int | str]]]]:
    """Get metadata for all enums in the registry."""
    metadata = {}

    for enum_name, enum_class in ENUM_REGISTRY.items():
        metadata[enum_name] = {
            "name": enum_name,
            "description": f"Enum for {enum_name.replace('_', ' ').lower()}",
            "values": [
                {
                    "value": member.value,
                    "name": member.name,
                    "description": get_enum_description(enum_class, member.value),
                }
                for member in enum_class
            ],
        }

    return metadata
