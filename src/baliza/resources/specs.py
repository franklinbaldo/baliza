import re
from collections.abc import Callable
from dataclasses import dataclass

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
    partition_strategy: str     # "monthly" (contratos) | "annual" (PCA) | "weekly"
    retention_policy: str       # "all" | "last_n=12"

@dataclass
class EntitySpec:
    name: str

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

@dataclass
class PNCPResource:
    resource_name: str
    fetch: FetchSpec
    raw_dataset: RawDatasetSpec
    entities: list[EntitySpec]
    canonical_tables: list[CanonicalTableSpec]

    def __post_init__(self) -> None:
        if not _RESOURCE_NAME_RE.match(self.resource_name):
            raise ValueError(
                f"Invalid resource_name {self.resource_name!r}: must match {_RESOURCE_NAME_RE.pattern}"
            )

    @property
    def name(self) -> str:
        # backward compatibility with old PNCPResource.name
        return self.resource_name
