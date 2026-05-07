from .atas import ATAS
from .contratos import CONTRATOS
from .pca import PCA
from .publicacoes import PUBLICACOES
from .specs import (
    ArtifactSpec,
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    FrontendExposureSpec,
    PartitionStrategy,
    PNCPResource,
    RawDatasetSpec,
)

# Registry of every PNCP resource the pipeline knows how to ingest. Code
# paths that need to look up a resource by its CLI name read from here
# instead of importing CONTRATOS directly. Adding a new resource only
# requires registering it here and defining its module under resources/.
RESOURCES: dict[str, PNCPResource] = {
    CONTRATOS.name: CONTRATOS,
    ATAS.name: ATAS,
}

# Resources that have been declared in code with their fetch/canonical
# shape but are intentionally NOT yet part of ``RESOURCES`` because they
# still carry TODO markers (modalidade fan-out for publicações; annual
# partitioning + flatten for PCA). They live here so:
#
#   * tests can assert their shape today (filename safety, name regex,
#     non-collision with the active registry);
#   * the frontend resource catalog can list them as "planejado" without
#     the backend pipeline trying to ingest them;
#   * promoting one to ``RESOURCES`` is a single-line move plus the
#     missing TODOs being closed in their respective modules.
PLANNED_RESOURCES: dict[str, PNCPResource] = {
    PUBLICACOES.name: PUBLICACOES,
    PCA.name: PCA,
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


def raw_zip_filename(resource_name: str, month_str: str) -> str:
    """Per-month raw ZIP filename used in the baliza-pncp-raw IA item.

    Contratos kept the legacy `raw-{month}.zip` shape so existing IA
    items don't need a rename. Other resources prefix the resource
    name to avoid colliding with contratos zips for the same partition.
    """
    if resource_name == CONTRATOS.name:
        return f"raw-{month_str}.zip"
    return f"raw-{resource_name}-{month_str}.zip"


__all__ = [
    "PNCPResource",
    "FetchSpec",
    "RawDatasetSpec",
    "EntitySpec",
    "ArtifactSpec",
    "CanonicalTableSpec",
    "FrontendExposureSpec",
    "PartitionStrategy",
    "CONTRATOS",
    "ATAS",
    "PUBLICACOES",
    "PCA",
    "RESOURCES",
    "PLANNED_RESOURCES",
    "get_resource",
    "page_filename",
    "first_page_filename",
    "raw_zip_filename",
]
