from datetime import date

from ..models import RecuperarContratoDTO
from ..transforms import _flatten_contrato
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
    return f"contratos_{part_str}.zip"

CONTRATOS = PNCPResource(
    resource_name="contratos",
    fetch=FetchSpec(
        endpoint="contratos",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        # Verified in production for years: contratos accepts 500 even
        # though the OpenAPI spec used to claim 50. Matches the historic
        # PAGE_SIZE constant the extractor sent before Drift-B publicacoes
        # wiring made this field actually drive the request.
        max_page_size=500,
        date_param_start="dataInicial",
        date_param_end="dataFinal",
        response_data_key="data",
    ),
    raw_dataset=RawDatasetSpec(
        ia_item_id="baliza-pncp-raw",  # Assume same place for now, or baliza-pncp-contratos-raw
        filename_fn=_monthly_zip_filename,
        partition_strategy=PartitionStrategy.MONTHLY,
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
    # Artifacts contratos publishes today. Drift-B prep: declared in
    # one place; the uploader / consolidator still hardcode the same
    # facts and will be rewired in the follow-up PR.
    artifacts=(
        ArtifactSpec(
            file_type="monthly_canonical",
            ia_item_id="baliza-pncp-{partition}",
            description="Per-month deduplicated canonical contratos snapshot.",
        ),
        ArtifactSpec(
            # IAConsolidator.consolidate_year uploads the per-UF shards
            # to baliza-pncp-consolidated alongside the annual rollup
            # — NOT to the per-month item. Verified at
            # consolidator.py:395 + register_monthly_uf_shards rows
            # carrying ia_item_id=CONSOLIDATED_IA_ITEM.
            file_type="monthly_uf",
            ia_item_id="baliza-pncp-consolidated",
            description="Per-UF shard of the monthly canonical, "
            "gated by CanonicalTableSpec.partition_by_uf. Uploaded by "
            "the consolidator alongside the annual rollup.",
        ),
        ArtifactSpec(
            file_type="annual_canonical",
            ia_item_id="baliza-pncp-consolidated",
            description="Yearly rollup of monthly canonicals into a "
            "single Parquet for explorer queries.",
        ),
    ),
)
