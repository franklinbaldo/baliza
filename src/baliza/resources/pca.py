"""Plano Anual de Contratações (PNCP /v1/pca/...).

Status: PLANNED, not yet wired into the active ``RESOURCES`` registry.

PCA is the upstream-most layer of the Baliza resource atlas: it
expresses *intended* future procurement, before any opportunity is
opened. Cross-joining PCA with publicações and contratos is what lets
Baliza answer "did this agency actually buy what it said it would?".

Known gaps before this can be promoted to ``RESOURCES``:

* TODO(pncp-pca-endpoint): the API exposes a family of endpoints under
  ``/v1/pca/`` (atualizacao, usuario, etc.). The exact endpoint Baliza
  should mirror as the canonical "PCA snapshot" is not decided yet.
* TODO(pncp-pca-partitioning): PCA is naturally partitioned by
  ``anoPca`` (annual), not monthly. Promoting PCA requires either
  extending ``RawDatasetSpec.partition_strategy`` plumbing in the
  mirror/uploader (currently month-only) or generating one synthetic
  monthly partition per year.
* TODO(pncp-pca-flatten): PCA payloads are nested (item-level rows
  under each plan). The canonical model is item-level, so the flatten
  function emits one row per ``PlanoContratacaoItemDTO`` enriched with
  the parent ``PlanoContratacaoComItensDoUsuarioDTO`` fields.
* TODO(pncp-pca-pk): item-level PK is composite — provisional choice
  ``(id_pca_pncp, numero_item)`` pending verification.
"""

from __future__ import annotations

from ..models import PlanoContratacaoComItensDoUsuarioDTO
from .specs import (
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    PNCPResource,
    RawDatasetSpec,
)


def _annual_zip_filename(part_str: str) -> str:
    # part_str is YYYY for annual partitions. The mirror plumbing today
    # passes YYYY-MM; promoting PCA must teach it to pass YYYY.
    return f"pca_{part_str}.zip"


PCA = PNCPResource(
    resource_name="pca",
    fetch=FetchSpec(
        # TODO(pncp-pca-endpoint): provisional; verify before wiring.
        endpoint="pca/atualizacao",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        max_page_size=500,
        # PCA uses dataInicio/dataFim, not dataInicial/dataFinal.
        date_param_start="dataInicio",
        date_param_end="dataFim",
        response_data_key="data",
    ),
    raw_dataset=RawDatasetSpec(
        ia_item_id="baliza-pncp-raw",
        filename_fn=_annual_zip_filename,
        partition_strategy="annual",
        retention_policy="all",
    ),
    entities=[
        EntitySpec(name="pca_plan"),
        EntitySpec(name="pca_item"),
    ],
    canonical_tables=[
        CanonicalTableSpec(
            table_name="pca_itens",
            schema_version="0.1.0",
            pk=["id_pca_pncp", "numero_item"],
            flatten_fn=None,  # TODO(pncp-pca-flatten): see module docstring.
            dedup_strategy="current_state",
            source_entity="pca_item",
            sort_columns=["orgao_entidade_cnpj", "ano_pca"],
            bloom_filter_columns=["orgao_entidade_cnpj"],
        )
    ],
    entity_model=PlanoContratacaoComItensDoUsuarioDTO,
)
