"""Lightweight regression check for the PNCP declarative client settings."""

from baliza.pipelines import pncp


def test_single_resource_contains_expected_keys() -> None:
    config = pncp.load_pncp_config()
    client = config["client"]
    assert client["base_url"].startswith("https://")

    paginator = client["paginator"]
    assert paginator["type"] == "page_number"
    assert paginator["page_param"] == "pagina"

    resource = config["resources"][0]
    assert resource["write_disposition"] == "merge"
    assert resource["primary_key"] == "numeroControlePNCP"
