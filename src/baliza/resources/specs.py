import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any


class PartitionStrategy(StrEnum):
    """Partition cadence for a resource's raw mirror.

    Typed (instead of the historical free-form ``str``) so adding a
    new strategy is a single Enum entry and a misspelling at the
    declaration site fails at import time. ``StrEnum`` keeps
    string-equality comparisons (``strategy == "monthly"``) working
    for any code that hasn't migrated yet.
    """

    MONTHLY = "monthly"
    ANNUAL = "annual"

# Resource names land in filesystem paths, URL params, and DuckDB
# table names. Enforcing the charset at registration time means the
# whole codebase can trust that any name in the registry is safe to
# concatenate, without re-validating at every call site.
_RESOURCE_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")


@dataclass
class FetchSpec:
    endpoint: str               # "/v1/contratos", "/v1/pca/atualizacao"
    pagination_param: str       # "pagina" (contratos)
    page_size_param: str        # "tamanhoPagina"
    max_page_size: int          # 500 para PCA, 50 para publicação
    date_param_start: str       # "dataInicial" (contratos) vs "dataInicio" (PCA)
    date_param_end: str         # "dataFinal" vs "dataFim"
    response_data_key: str      # chave do array na resposta JSON ("data", "items", etc.)

    # Backward compatibility with old PNCPResource structure
    @property
    def date_param(self) -> str:
        # Some components expect this, though in new spec it's split.
        return self.date_param_start

    @property
    def paginate_by(self) -> str:
        return "month"


@dataclass
class RawDatasetSpec:
    ia_item_id: str             # item no IA onde o ZIP bruto vai
    filename_fn: Callable       # partição -> nome do arquivo ZIP
    partition_strategy: PartitionStrategy  # MONTHLY (contratos/atas) | ANNUAL (PCA)
    retention_policy: str       # "all" | "last_n=12"

@dataclass
class EntitySpec:
    name: str


@dataclass(frozen=True)
class FrontendExposureSpec:
    """How the frontend should surface a resource's canonical artifact.

    Lives on PNCPResource so the TS generator can iterate the live
    registry instead of importing per-resource constants.

    ``canonical_file_types`` is the allowlist of manifest ``file_type``
    values the website should treat as the canonical row for this
    resource. The empty string is included for backward compatibility
    with v1 manifests that pre-date the column. Adding a new file_type
    (e.g. ``annual_canonical`` for PCA) is a one-line change here —
    the generator unions every exposure's set into a single TS constant
    that ``isCanonicalRow()`` consults.
    """
    artifact_name: str
    table_alias: str
    is_canonical: bool = True
    canonical_file_types: tuple[str, ...] = ("", "monthly_canonical")

@dataclass
class CanonicalTableSpec:
    table_name: str                # "contratos_canonical", "pca_itens_canonical"
    schema_version: str            # "2.0.0"
    pk: str | list[str]            # PK simples ou composta
    flatten_fn: Callable | None    # row dict -> linha flat snake_case
    dedup_strategy: str            # "current_state" | "append_only"
    source_entity: str             # EntitySpec.name que origina esta tabela
    sort_columns: list[str]
    bloom_filter_columns: list[str]
    # Optional override for the ORDER BY clause used when exporting the
    # monthly Parquet. When None, callers derive a default from
    # sort_columns + pk (preserving determinism). Contratos sets this
    # explicitly because it carries a `data_publicacao DESC` shape that
    # downstream consumers depend on; new resources should leave it
    # blank to get the derived ordering.
    order_by_sql: str | None = None
    # Whether the canonical schema carries a ``uf_sigla`` column the
    # consolidator can shard on. Contratos = True (every contrato has a
    # buyer's UF); atas = False (atas API responses don't carry UF info).
    # Drives ``_build_per_uf_shards`` + ``register_monthly_uf_shards``
    # in the consolidator. Keep this declarative because the column
    # presence isn't always derivable from sort/bloom column lists.
    partition_by_uf: bool = False

@dataclass
class PNCPResource:
    resource_name: str
    fetch: FetchSpec
    raw_dataset: RawDatasetSpec
    entities: list[EntitySpec]
    canonical_tables: list[CanonicalTableSpec]
    # Pydantic class used to validate raw API entries before flattening.
    # Stored as Any to avoid a hard pydantic import at the spec layer
    # (specs.py is imported very early and importing pydantic would
    # pull in heavy machinery for every CLI invocation). The extractor
    # asserts the type when it actually uses the field.
    entity_model: Any = field(default=None)
    # First date on which PNCP exposed source data for this resource.
    # Backfills clamp to this floor so workers don't probe months that
    # cannot contain records. Living on the resource (instead of a
    # parallel dict in constants.py) means registering a new resource
    # is a single-file change.
    data_start: date | None = field(default=None)
    # How the frontend exposes this resource's canonical artifact(s).
    # Empty by default so resources without a frontend surface (e.g.
    # planned ones still missing flatten_fn) don't leak placeholder
    # entries into the generated TS.
    frontend_exposures: list["FrontendExposureSpec"] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not _RESOURCE_NAME_RE.match(self.resource_name):
            raise ValueError(
                f"Invalid resource_name {self.resource_name!r}: must match {_RESOURCE_NAME_RE.pattern}"
            )

    @property
    def name(self) -> str:
        # backward compatibility with old PNCPResource.name
        return self.resource_name
