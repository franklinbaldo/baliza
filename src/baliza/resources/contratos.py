from ..transforms import _flatten_contrato
from .specs import (
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    PNCPResource,
    RawDatasetSpec,
)


def _monthly_zip_filename(part_str: str) -> str:
    return f"contratos_{part_str}.zip"

CONTRATOS = PNCPResource(
    resource_name="contratos",
    fetch=FetchSpec(
        endpoint="contratos",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        max_page_size=50,
        date_param_start="dataInicial",
        date_param_end="dataFinal",
        response_data_key="data",
    ),
    raw_dataset=RawDatasetSpec(
        ia_item_id="baliza-pncp-raw",  # Assume same place for now, or baliza-pncp-contratos-raw
        filename_fn=_monthly_zip_filename,
        partition_strategy="monthly",
        retention_policy="all",
    ),
    entities=[
        EntitySpec(name="contrato"),
    ],
    canonical_tables=[
        CanonicalTableSpec(
            table_name="contratos", # Using old table name 'contratos' for zero regression now
            schema_version="2.0.0",
            pk="numero_controle_pncp",
            flatten_fn=_flatten_contrato,
            dedup_strategy="current_state",
            source_entity="contrato",
            sort_columns=["cnpj_orgao", "data_publicacao_pncp"],
            bloom_filter_columns=["cnpj_orgao"],
        )
    ]
)
