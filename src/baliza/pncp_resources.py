from dataclasses import dataclass, field

@dataclass(frozen=True)
class FetchSpec:
    endpoint: str
    date_param: str  # e.g., "data_publicacao"
    paginate_by: str  # e.g., "month"

@dataclass(frozen=True)
class CanonicalTableSpec:
    name: str            # ex: "contratos"
    pk: str | list[str]  # PR A: composite PK support
    deduped: bool = True
    schema: dict | None = None  # opcional: tipos das colunas

@dataclass(frozen=True)
class DerivedTableSpec:
    name: str            # ex: "contratos_yearly_uf"
    sources: list[str]   # nomes de CanonicalTableSpec ou DerivedTableSpec
    transform: str       # nome da função que materializa (ex: "build_yearly_uf")

@dataclass(frozen=True)
class PNCPResource:
    name: str  # e.g., "contratos"
    fetch: FetchSpec
    canonical_tables: list[CanonicalTableSpec]
    derived_tables: list[DerivedTableSpec] = field(default_factory=list)

CONTRATOS = PNCPResource(
    name="contratos",
    fetch=FetchSpec(
        endpoint="contratos",
        date_param="data_publicacao",
        paginate_by="month",
    ),
    canonical_tables=[
        # Note: name will change to "contratos_canonical" when compound cases (PCA, PR G) justify the separation
        CanonicalTableSpec(name="contratos", pk="numero_controle_pncp")
    ],
    derived_tables=[],
)
