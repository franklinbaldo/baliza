from datetime import date

from ..models import RecuperarAtaDTO
from ..transforms import _flatten_ata
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
        partition_strategy=PartitionStrategy.MONTHLY,
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
    # PNCP launched in mid-2021 and atas were available from GA;
    # use the contratos floor until we observe an empty earlier month.
    data_start=date(2021, 9, 6),
    frontend_exposures=[
        FrontendExposureSpec(
            artifact_name="atas",
            table_alias="atas",
            is_canonical=True,
        ),
    ],
    # Atas does not produce monthly_uf shards (partition_by_uf=False
    # on the canonical table — atas API responses don't carry buyer's
    # UF). monthly_canonical + annual_canonical only.
    artifacts=(
        ArtifactSpec(
            file_type="monthly_canonical",
            ia_item_id="baliza-pncp-{partition}",
            description="Per-month deduplicated canonical atas snapshot.",
        ),
        ArtifactSpec(
            file_type="annual_canonical",
            ia_item_id="baliza-pncp-consolidated",
            description="Yearly rollup of monthly canonicals.",
        ),
    ),
)
