from .contratos import CONTRATOS
from .specs import (
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    PNCPResource,
    RawDatasetSpec,
)

# Registry of every PNCP resource the pipeline knows how to ingest. Code
# paths that need to look up a resource by its CLI name read from here
# instead of importing CONTRATOS directly. Adding a new resource (e.g.
# atas) only requires registering it here and defining its module under
# resources/.
RESOURCES: dict[str, PNCPResource] = {
    CONTRATOS.name: CONTRATOS,
}


def get_resource(name: str) -> PNCPResource:
    """Look up a PNCPResource by its CLI name.

    Raises ValueError on unknown names so callers can surface a clear
    error instead of failing later with a KeyError.
    """
    try:
        return RESOURCES[name]
    except KeyError as e:
        known = ", ".join(sorted(RESOURCES))
        raise ValueError(
            f"unknown resource {name!r}; registered: {known}"
        ) from e


def page_filename(resource_name: str, page: int) -> str:
    """Per-page raw JSON filename for a resource's daily/monthly fetch."""
    return f"{resource_name}_p{page}.json"


def first_page_filename(resource_name: str) -> str:
    """Convenience for the first-page existence check used by mirror."""
    return page_filename(resource_name, 1)


__all__ = [
    "PNCPResource",
    "FetchSpec",
    "RawDatasetSpec",
    "EntitySpec",
    "CanonicalTableSpec",
    "CONTRATOS",
    "RESOURCES",
    "get_resource",
    "page_filename",
    "first_page_filename",
]
