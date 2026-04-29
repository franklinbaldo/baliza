from baliza.pncp_resources import (
    CONTRATOS,
    CanonicalTableSpec,
    DerivedTableSpec,
    FetchSpec,
    PNCPResource,
)


def test_canonical_table_spec_attributes():
    spec = CanonicalTableSpec(name="test_canonical", pk="id", deduped=True, schema={"id": "string"})
    assert spec.name == "test_canonical"
    assert spec.pk == "id"
    assert spec.deduped is True
    assert spec.schema == {"id": "string"}

def test_canonical_table_spec_composite_pk():
    spec = CanonicalTableSpec(name="test_canonical", pk=["id", "version"])
    assert spec.pk == ["id", "version"]

def test_derived_table_spec_attributes():
    spec = DerivedTableSpec(name="test_derived", sources=["test_canonical"], transform="build_derived")
    assert spec.name == "test_derived"
    assert spec.sources == ["test_canonical"]
    assert spec.transform == "build_derived"

def test_pncp_resource_attributes():
    fetch = FetchSpec(endpoint="test", date_param="date", paginate_by="month")
    canonical = CanonicalTableSpec(name="canon", pk="id")
    derived = DerivedTableSpec(name="derived", sources=["canon"], transform="trans")

    resource = PNCPResource(
        name="test_resource",
        fetch=fetch,
        canonical_tables=[canonical],
        derived_tables=[derived]
    )

    assert resource.name == "test_resource"
    assert resource.fetch == fetch
    assert len(resource.canonical_tables) == 1
    assert resource.canonical_tables[0].name == "canon"
    assert len(resource.derived_tables) == 1
    assert resource.derived_tables[0].name == "derived"

def test_contratos_resource_configuration():
    assert CONTRATOS.name == "contratos"
    assert len(CONTRATOS.canonical_tables) == 1
    assert CONTRATOS.canonical_tables[0].name == "contratos"
    assert CONTRATOS.canonical_tables[0].pk == "numero_controle_pncp"
    assert CONTRATOS.derived_tables == []
