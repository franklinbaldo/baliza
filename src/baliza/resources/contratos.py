from datetime import date

from ..models import RecuperarContratoDTO
from ..transforms import _flatten_contrato
from .specs import (
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    FrontendExposureSpec,
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
            # Preserve the historical ORDER BY shape so existing
            # parquet files (and the bloom filter prefiltering that
            # depends on cnpj_orgao locality) stay byte-comparable
            # across this multi-resource refactor.
            order_by_sql="cnpj_orgao, data_publicacao DESC, numero_controle_pncp",
            # Contratos has uf_sigla in every row (buyer's UF) and the
            # consolidator emits per-state monthly_uf shards from it.
            partition_by_uf=True,
        )
    ],
    entity_model=RecuperarContratoDTO,
    # PNCP launched in mid-2021; the contratos endpoint's first record
    # is 2021-09-06. Backfill workers skip earlier months without ever
    # hitting the API.
    data_start=date(2021, 9, 6),
    frontend_exposures=[
        FrontendExposureSpec(
            artifact_name="contratos",
            table_alias="contratos",
            is_canonical=True,
        ),
    ],
)
