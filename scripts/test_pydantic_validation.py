"""Ensure the declarative schema lists the expected fields."""

from baliza.pipelines import pncp


def test_resource_metadata_fields() -> None:
    config = pncp.load_pncp_config()
    resource = config["resources"][0]
    endpoint = resource["endpoint"]
    params = endpoint["params"]
    assert set(params.keys()) >= {"pagina", "tamanhoPagina", "dataInicial", "dataFinal"}
