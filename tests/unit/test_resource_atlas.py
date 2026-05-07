"""Tests for the PNCP Resource Atlas: registered + planned resources.

These tests pin down the architectural contract documented in
``web/features/pncp-resource-atlas.feature``. They protect:

* the registered resources keep their established names, schema versions
  and raw filename shapes (regression for contratos / atas);
* the planned resources (publicações, PCA) are declared with the same
  spec shape as registered ones, so promotion is mechanical;
* raw ZIP filenames cannot collide across resources for the same
  monthly partition.
"""

from __future__ import annotations

import pytest

from baliza.resources import (
    ATAS,
    CONTRATOS,
    PCA,
    PLANNED_RESOURCES,
    PUBLICACOES,
    RESOURCES,
    PNCPResource,
    first_page_filename,
    get_resource,
    page_filename,
    raw_zip_filename,
)


def test_registered_resources_includes_all_four():
    """publicacoes (#568) and pca (#569) both promoted out of PLANNED."""
    assert set(RESOURCES) == {"contratos", "atas", "publicacoes", "pca"}


def test_planned_resources_is_empty_after_pca_promotion():
    """All originally-planned resources have shipped."""
    assert PLANNED_RESOURCES == {}


def test_planned_resources_do_not_overlap_with_registered():
    assert set(RESOURCES).isdisjoint(set(PLANNED_RESOURCES))


@pytest.mark.parametrize(
    "resource",
    [CONTRATOS, ATAS, PUBLICACOES, PCA],
    ids=lambda r: r.name,
)
def test_resource_shape_is_complete(resource: PNCPResource) -> None:
    # Endpoint / pagination / date-range params are present.
    assert resource.fetch.endpoint
    assert resource.fetch.pagination_param
    assert resource.fetch.page_size_param
    assert resource.fetch.date_param_start
    assert resource.fetch.date_param_end
    assert resource.fetch.response_data_key

    # Raw dataset.
    assert resource.raw_dataset.ia_item_id
    assert callable(resource.raw_dataset.filename_fn)
    assert resource.raw_dataset.partition_strategy in {"monthly", "annual", "weekly"}

    # At least one canonical table with PK + schema_version.
    assert resource.canonical_tables
    for ct in resource.canonical_tables:
        assert ct.table_name
        assert ct.schema_version
        assert ct.pk

    # Pydantic entity model (validation seam).
    assert resource.entity_model is not None


def test_contratos_keeps_legacy_raw_zip_shape():
    # Regression: existing IA items must not need renaming.
    assert raw_zip_filename(CONTRATOS.name, "2024-01") == "raw-2024-01.zip"


def test_non_contratos_resources_have_prefixed_raw_zip():
    assert raw_zip_filename(ATAS.name, "2024-01") == "raw-atas-2024-01.zip"
    assert (
        raw_zip_filename(PUBLICACOES.name, "2024-01")
        == "raw-publicacoes-2024-01.zip"
    )
    assert raw_zip_filename(PCA.name, "2024") == "raw-pca-2024.zip"


def test_raw_zip_filenames_do_not_collide_across_resources():
    month = "2024-01"
    names = {
        raw_zip_filename(r.name, month)
        for r in (CONTRATOS, ATAS, PUBLICACOES, PCA)
    }
    assert len(names) == 4


def test_per_page_filenames_are_resource_scoped():
    assert page_filename("publicacoes", 3) == "publicacoes_p3.json"
    assert first_page_filename("pca") == "pca_p1.json"
    # Contratos and a planned resource never produce the same page filename.
    assert page_filename("contratos", 1) != page_filename("publicacoes", 1)


def test_get_resource_returns_all_registered():
    assert get_resource("contratos") is CONTRATOS
    assert get_resource("atas") is ATAS
    assert get_resource("publicacoes") is PUBLICACOES
    assert get_resource("pca") is PCA
    with pytest.raises(ValueError, match="unknown resource"):
        get_resource("not_a_resource")


def test_publicacoes_endpoint_is_contratacoes_publicacao():
    # Verified against debug_api.py probes.
    assert PUBLICACOES.fetch.endpoint == "contratacoes/publicacao"


def test_pca_uses_inicio_fim_date_params():
    # PCA uses dataInicio/dataFim, contratos uses dataInicial/dataFinal.
    assert PCA.fetch.date_param_start == "dataInicio"
    assert PCA.fetch.date_param_end == "dataFim"


def test_pca_canonical_table_uses_composite_pk():
    ct = PCA.canonical_tables[0]
    assert isinstance(ct.pk, list)
    assert ct.pk == ["id_pca_pncp", "numero_item"]
