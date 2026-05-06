"""Publicações de contratações (PNCP /v1/contratacoes/publicacao).

Status: PLANNED, not yet wired into the active ``RESOURCES`` registry.

This module defines the resource shape we intend to register once the
remaining facts below are verified against the official PNCP API and
covered by tests. Keeping the spec here (instead of in a design doc)
means promotion to a registered resource is a single-line change in
``resources/__init__.py`` and forces every gap to be a code-visible TODO
rather than prose.

Why publicações is the next strategic resource:

* It captures *current opportunity* — what is open right now — which is
  the missing temporal layer between PCA (intended demand) and contratos
  (finalized facts). The B2G-supplier and public-buyer journeys both
  depend on it.

Known gaps before this can be promoted to ``RESOURCES``:

* TODO(pncp-publicacao-modalidade): the ``/v1/contratacoes/publicacao``
  endpoint requires the ``codigoModalidadeContratacao`` query parameter
  (see ``debug_api.py``). Our current ``FetchSpec`` has no fan-out
  primitive; promoting publicações requires either (a) extending
  FetchSpec with an iterable ``required_params`` field and teaching the
  extractor to fan out, or (b) registering one resource per modalidade.
  Decision deferred.
* TODO(pncp-publicacao-flatten): no ``_flatten_publicacao`` exists in
  ``transforms.py``. The Pydantic model ``RecuperarCompraPublicacaoDTO``
  is already generated from the OpenAPI spec and can be the basis for a
  flatten function once the canonical column list is agreed.
* TODO(pncp-publicacao-pk): the natural primary key is
  ``numeroControlePNCP``, but we should verify that it is unique within
  a single fetched window before committing to ``current_state`` dedup.
* TODO(pncp-publicacao-data-start): confirm the earliest date PNCP
  exposes ``contratacoes/publicacao`` records for. Until then we leave
  ``data_start`` unset so backfills do not silently clamp to a wrong
  floor.
"""

from __future__ import annotations

from ..models import RecuperarCompraPublicacaoDTO
from .specs import (
    CanonicalTableSpec,
    EntitySpec,
    FetchSpec,
    PNCPResource,
    RawDatasetSpec,
)


def _monthly_zip_filename(part_str: str) -> str:
    return f"publicacoes_{part_str}.zip"


PUBLICACOES = PNCPResource(
    resource_name="publicacoes",
    fetch=FetchSpec(
        # Verified against debug_api.py: PNCP exposes this as
        # /v1/contratacoes/publicacao. The base URL in extractor.py
        # already includes /v1, so the endpoint is the suffix.
        endpoint="contratacoes/publicacao",
        pagination_param="pagina",
        page_size_param="tamanhoPagina",
        # TODO(pncp-publicacao-page-size): confirm the documented max.
        # Using 50 as a conservative default mirroring the contratos
        # original until verified — do NOT raise without testing.
        max_page_size=50,
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
        EntitySpec(name="publicacao"),
    ],
    canonical_tables=[
        CanonicalTableSpec(
            table_name="publicacoes",
            schema_version="0.1.0",
            # TODO(pncp-publicacao-pk): see module docstring.
            pk="numero_controle_pncp",
            # TODO(pncp-publicacao-flatten): see module docstring.
            flatten_fn=None,
            dedup_strategy="current_state",
            source_entity="publicacao",
            sort_columns=["cnpj_orgao", "data_publicacao_pncp"],
            bloom_filter_columns=["cnpj_orgao"],
        )
    ],
    entity_model=RecuperarCompraPublicacaoDTO,
    # data_start intentionally omitted until verified against the API.
    # clamp_to_known_data_start_month tolerates this and returns the
    # caller-provided start unchanged.
)
