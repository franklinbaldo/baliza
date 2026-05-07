"""Plano Anual de Contratações (PNCP /v1/pca/atualizacao).

Promoted to ``RESOURCES`` after probe verification (#574) confirmed:

* ``/v1/pca/atualizacao`` accepts the date-range FetchSpec shape (no
  fan-out needed); a 3-day window returned 542 pages / 270818 records.
* ``PlanoContratacaoComItensDoUsuarioDTO`` validates 100% of live
  payloads; 500 nested items per page typical.
* The composite PK ``(idPcaPncp, numeroItem)`` is unique within a
  page (probe ran 500 distinct / 0 duplicated). Drift-A's
  anti_join path handles the composite case.
* PCA partitioning is annual (one row per anoPca per partition);
  Drift-D's iter_partitions handles the calendar-year iteration.
"""

from __future__ import annotations

from datetime import date

from ..models import PlanoContratacaoComItensDoUsuarioDTO
from ..transforms import _flatten_pca_item
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


def _annual_zip_filename(part_str: str) -> str:
    # part_str is YYYY for annual partitions (Drift-D iter_partitions
    # emits partition_label = str(year) for ANNUAL strategy).
    return f"pca_{part_str}.zip"


PCA = PNCPResource(
    resource_name="pca",
    fetch=FetchSpec(
        # Verified by scripts/probe_pca.py (#574) — only endpoint of
        # the three /v1/pca/* candidates that fits a global mirror
        # without per-user or per-classificacao-superior fan-out.
        endpoint="pca/atualizacao",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        max_page_size=500,
        # PCA uses dataInicio/dataFim, not dataInicial/dataFinal —
        # mirrors the OpenAPI spec.
        date_param_start="dataInicio",
        date_param_end="dataFim",
        response_data_key="data",
    ),
    raw_dataset=RawDatasetSpec(
        ia_item_id="baliza-pncp-raw",
        filename_fn=_annual_zip_filename,
        partition_strategy=PartitionStrategy.ANNUAL,
        retention_policy="all",
    ),
    entities=[
        EntitySpec(name="pca_plano"),
        EntitySpec(name="pca_item"),
    ],
    canonical_tables=[
        CanonicalTableSpec(
            table_name="pca",
            schema_version="1.0.0",
            # Composite PK — Drift-A's anti_join handles this. Single
            # column would silently drop items that share an id_pca_pncp
            # with a colliding row.
            pk=["id_pca_pncp", "numero_item"],
            flatten_fn=_flatten_pca_item,
            dedup_strategy="current_state",
            source_entity="pca_item",
            sort_columns=["cnpj_orgao", "ano_pca", "id_pca_pncp"],
            bloom_filter_columns=["cnpj_orgao", "id_pca_pncp"],
            # PCA item rows don't carry a buyer's UF (the parent's
            # unidadeOrgao isn't queried here — see
            # PlanoContratacaoComItensDoUsuarioDTO). Skip per-UF shards
            # like atas.
            partition_by_uf=False,
        ),
    ],
    entity_model=PlanoContratacaoComItensDoUsuarioDTO,
    # PNCP launched in 2021. The first PCA-eligible year was 2022 in
    # practice; PCAs for 2021 weren't published. Use the contratos
    # floor as a safe pre-data clamp.
    data_start=date(2021, 9, 6),
    frontend_exposures=[
        FrontendExposureSpec(
            artifact_name="pca",
            table_alias="pca",
            is_canonical=True,
            # PCA only publishes an annual canonical (no monthly
            # rollup). Add 'annual_canonical' to the allowlist that
            # isCanonicalRow() consults so the web layer treats those
            # manifest rows as the canonical ones for PCA.
            canonical_file_types=("", "annual_canonical"),
        ),
    ],
    # PCA does NOT publish a monthly_canonical (the partition is
    # annual end-to-end). Just annual_canonical lives in the
    # consolidated IA item alongside contratos / atas annuals.
    artifacts=(
        ArtifactSpec(
            file_type="annual_canonical",
            ia_item_id="baliza-pncp-consolidated",
            description="Yearly PCA snapshot — one row per item per "
            "anoPca, deduplicated by (id_pca_pncp, numero_item).",
        ),
    ),
)
