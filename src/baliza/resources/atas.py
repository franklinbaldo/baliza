from ..models import RecuperarAtaDTO
from ..transforms import _flatten_ata
from .specs import (
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    PNCPResource,
    RawDatasetSpec,
)


def _monthly_zip_filename(part_str: str) -> str:
    return f"atas_{part_str}.zip"


ATAS = PNCPResource(
    resource_name="atas",
    fetch=FetchSpec(
        endpoint="atas",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        # The /v1/atas spec says 500 max — confirmed in the audit
        # (run #25415111962): one day already returns ~500K registros so
        # we want the largest legal page size to keep round-trips down.
        max_page_size=500,
        date_param_start="dataInicial",
        date_param_end="dataFinal",
        response_data_key="data",
    ),
    raw_dataset=RawDatasetSpec(
        ia_item_id="baliza-pncp-raw",
        filename_fn=_monthly_zip_filename,
        partition_strategy="monthly",
        retention_policy="all",
    ),
    entities=[
        EntitySpec(name="ata"),
    ],
    canonical_tables=[
        CanonicalTableSpec(
            table_name="atas",
            schema_version="1.0.0",
            pk="numero_controle_pncp_ata",
            flatten_fn=_flatten_ata,
            dedup_strategy="current_state",
            source_entity="ata",
            sort_columns=["cnpj_orgao", "data_publicacao_pncp"],
            bloom_filter_columns=["cnpj_orgao", "numero_controle_pncp_compra"],
        )
    ],
    entity_model=RecuperarAtaDTO,
)
