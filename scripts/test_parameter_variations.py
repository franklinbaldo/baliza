"""Regression tests for the declarative PNCP configuration."""

from baliza.pipelines import pncp


def test_parameter_variations() -> None:
    config = pncp.load_pncp_config()
    resources = config["resources"]
    assert len(resources) == 1

    contratos = resources[0]
    assert contratos["name"] == "contratos"
    params = contratos["endpoint"]["params"]
    assert params["pagina"] == 1
    assert params["tamanhoPagina"] == 500

    incremental = contratos["endpoint"]["incremental"]
    assert incremental["start_param"] == "dataInicial"
    assert incremental["end_param"] == "dataFinal"
