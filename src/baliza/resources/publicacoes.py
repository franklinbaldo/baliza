"""Publicações de contratações (PNCP /v1/contratacoes/publicacao).

Promoted to ``RESOURCES`` after probe verification (#572) confirmed:

* The endpoint requires ``codigoModalidadeContratacao`` — the extractor
  fans out across modalidade IDs 1..14 per partition window. Modalidade
  14 returns 204 (empty); the rest carry data on a typical day.
* ``RecuperarCompraPublicacaoDTO`` validates 100% of live payloads
  after the situacaoCompraId IntEnum fix in #572.
* ``numeroControlePNCP`` is unique within a partition window across the
  fan-out (270 distinct in a 3-day probe, all appearing exactly once),
  so single-PK ``current_state`` dedup is safe.
"""

from __future__ import annotations

from datetime import date

from ..models import RecuperarCompraPublicacaoDTO
from ..transforms import _flatten_publicacao
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
    return f"publicacoes_{part_str}.zip"


# PNCP modalidade IDs covered by /v1/contratacoes/publicacao. The
# probe (scripts/probe_publicacoes.py, run #572) confirmed values
# 1, 3, 5, 7, 8, 9, 10, 11, 12, 13 carry data and 14 returns 204
# empty consistently; 2, 4, 6 timed out intermittently — include
# them so we don't silently drop data when the upstream is healthy.
_PUBLICACAO_MODALIDADES: list[int | str] = list(range(1, 15))


PUBLICACOES = PNCPResource(
    resource_name="publicacoes",
    fetch=FetchSpec(
        endpoint="contratacoes/publicacao",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        # Probe runs used 50; PNCP docs cap at 50 for this endpoint.
        max_page_size=50,
        date_param_start="dataInicial",
        date_param_end="dataFinal",
        response_data_key="data",
        # Modalidade fan-out — extractor walks the cartesian product
        # of these values per partition window. Empty modalidades
        # (HTTP 204) are tolerated; transient timeouts retry.
        required_params={"codigoModalidadeContratacao": _PUBLICACAO_MODALIDADES},
    ),
    raw_dataset=RawDatasetSpec(
        ia_item_id="baliza-pncp-raw",
        filename_fn=_monthly_zip_filename,
        partition_strategy=PartitionStrategy.MONTHLY,
        retention_policy="all",
    ),
    entities=[
        EntitySpec(name="publicacao"),
    ],
    canonical_tables=[
        CanonicalTableSpec(
            table_name="publicacoes",
            schema_version="1.0.0",
            pk="numero_controle_pncp",
            flatten_fn=_flatten_publicacao,
            dedup_strategy="current_state",
            source_entity="publicacao",
            sort_columns=["cnpj_orgao", "data_publicacao_pncp"],
            bloom_filter_columns=["cnpj_orgao", "modalidade_id"],
            # publicacoes carries unidadeOrgao.ufSigla on every record,
            # like contratos — emit per-UF shards alongside the monthly
            # canonical so UF-filtered web queries don't pay the full
            # parquet scan.
            partition_by_uf=True,
        )
    ],
    entity_model=RecuperarCompraPublicacaoDTO,
    # PNCP launched in 2021. The contratos floor (2021-09-06) is also
    # the safest publicacoes floor until we observe an earlier non-empty
    # response from the API.
    data_start=date(2021, 9, 6),
    frontend_exposures=[
        FrontendExposureSpec(
            artifact_name="publicacoes",
            table_alias="publicacoes",
            is_canonical=True,
        ),
    ],
    artifacts=(
        ArtifactSpec(
            file_type="monthly_canonical",
            ia_item_id="baliza-pncp-{partition}",
            description="Per-month deduplicated canonical publicacoes snapshot.",
        ),
        ArtifactSpec(
            file_type="monthly_uf",
            ia_item_id="baliza-pncp-consolidated",
            description="Per-UF shard of the monthly canonical, "
            "uploaded by the consolidator alongside the annual rollup.",
        ),
        ArtifactSpec(
            file_type="annual_canonical",
            ia_item_id="baliza-pncp-consolidated",
            description="Yearly rollup of monthly canonicals.",
        ),
    ),
)
