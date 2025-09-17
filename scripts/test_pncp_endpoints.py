"""Ensure the declarative contracts resource exposes incremental metadata."""

from baliza.pipelines import pncp


def test_incremental_configuration() -> None:
    config = pncp.load_pncp_config()
    incremental = config["resources"][0]["endpoint"]["incremental"]
    assert incremental["cursor_path"] == "dataAtualizacao"
    assert incremental["start_param"] == "dataInicial"
    assert incremental["end_param"] == "dataFinal"
