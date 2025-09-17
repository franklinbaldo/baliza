"""Additional sanity checks for the PNCP declarative config."""

from baliza.pipelines import pncp
from baliza.utils.dates import to_pncp_window


def test_incremental_conversion_examples() -> None:
    config = pncp.load_pncp_config()
    convert = config["resources"][0]["endpoint"]["incremental"]["convert"]

    assert convert("2024-02-29T12:34:56Z") == "20240229"
    assert convert("20240301") == "20240301"
    assert convert(None) == to_pncp_window(None)
