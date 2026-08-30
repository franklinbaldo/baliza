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
    # Required query params the extractor must fan out across. The
    # ``/v1/contratacoes/publicacao`` endpoint, for instance, requires
    # ``codigoModalidadeContratacao`` and returns 400 if absent — the
    # extractor walks the cartesian product of these values for each
    # partition window. Empty default keeps existing resources
    # (contratos, atas) untouched.
    required_params: dict[str, list[int | str]] = field(default_factory=dict)

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

@dataclass(frozen=True)
class ArtifactSpec:
    """Declarative description of one artifact a resource publishes.

    Today the uploader, consolidator and exporter each carry their own
    branches that hardcode artifact-axis facts: filename shape, IA
    item, ``file_type`` written to the manifest, sort/bloom columns
    used at export time. ``ArtifactSpec`` is the type that lets a new
    resource (PCA's annual rollup, future per-cnpj index, ...) declare
    those facts in one place and have the runtime iterate over
    ``resource.artifacts``.

    This PR only introduces the type and populates it for the
    artifacts contratos / atas already produce today; the runtime
    paths still hardcode the same facts. The follow-up wiring PR
    deletes those branches in favor of iterating ``artifacts``.
    """

    # Manifest ``file_type`` value the runtime writes for this artifact.
    # Matches the strings ``isCanonicalRow`` / consolidator already emit
    # so the wiring PR can be a behavior-preserving substitution.
    file_type: str
    # IA item the artifact lands in (e.g. ``baliza-pncp-consolidated``
    # for annual rollups, ``baliza-pncp-{YYYY-MM}`` for monthly
    # canonical — for partitioned items, this is the *prefix* the
    # runtime appends the partition label to).
    ia_item_id: str
    # Optional human label for plan/docs and future explorer surfaces.
    description: str = ""
    # Sort + bloom columns used when materializing the artifact.
    # Empty means "inherit from canonical_tables[0]" — keeps contratos
    # / atas declarations tight while leaving the door open for
    # per-artifact overrides (e.g. annual rollup sorted by year).
    # Tuples (not lists) so ``frozen=True`` actually means immutable —
    # a list field would still let a caller append at runtime and
    # poison the shared registry for every later caller.
    sort_columns: tuple[str, ...] = ()
    bloom_filter_columns: tuple[str, ...] = ()
    # Full ORDER BY override. Mirrors CanonicalTableSpec.order_by_sql.
    order_by_sql: str | None = None

    def __post_init__(self) -> None:
        # Type annotations don't enforce runtime: a caller passing
        # ``sort_columns=[...]`` would still slip through and the list
        # could be mutated later, poisoning the shared registry.
        # Coerce to tuple so the immutability claim is real regardless
        # of how the spec was constructed. ``object.__setattr__`` is
        # required because the dataclass is frozen.
        if not isinstance(self.sort_columns, tuple):
            object.__setattr__(self, "sort_columns", tuple(self.sort_columns))
        if not isinstance(self.bloom_filter_columns, tuple):
            object.__setattr__(
                self, "bloom_filter_columns", tuple(self.bloom_filter_columns),
            )


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
    # Artifacts the resource publishes (monthly_canonical, optional
    # monthly_uf, optional annual_canonical, future per-cnpj index...).
    # Empty default keeps PLANNED_RESOURCES that don't have a runtime
    # path yet from having to spell artifacts they don't produce.
    artifacts: tuple["ArtifactSpec", ...] = ()

    def __post_init__(self) -> None:
        if not _RESOURCE_NAME_RE.match(self.resource_name):
            raise ValueError(
                f"Invalid resource_name {self.resource_name!r}: "
                f"must match {_RESOURCE_NAME_RE.pattern}"
            )
        # PNCPResource itself isn't frozen (existing fields like
        # entities / canonical_tables predate this PR and stay as
        # lists), but ``artifacts`` describes the runtime publish
        # contract — an accidental ``.append()`` on the registry
        # singleton would silently change which artifacts get
        # uploaded once the wiring PR consumes it (Codex P2). Coerce
        # to tuple so callers passing list literals still work while
        # in-place mutation fails.
        if not isinstance(self.artifacts, tuple):
            self.artifacts = tuple(self.artifacts)

    @property
    def name(self) -> str:
        # backward compatibility with old PNCPResource.name
        return self.resource_name
