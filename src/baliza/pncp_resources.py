from dataclasses import dataclass

@dataclass(frozen=True)
class FetchSpec:
    endpoint: str
    date_param: str  # e.g., "data_publicacao"
    paginate_by: str  # e.g., "month"

@dataclass(frozen=True)
class PNCPResource:
    name: str  # e.g., "contratos"
    fetch: FetchSpec

CONTRATOS = PNCPResource(
    name="contratos",
    fetch=FetchSpec(
        endpoint="contratos",
        date_param="data_publicacao",
        paginate_by="month",
    ),
)
