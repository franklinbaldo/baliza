"""Smoke test for instantiating the PNCP dlt source."""

from unittest.mock import patch

from baliza.pipelines import pncp


def test_pncp_source_invokes_rest_api_source() -> None:
    with patch("baliza.pipelines.pncp.rest_api_source") as rest_mock:
        rest_mock.return_value = "source"
        assert pncp.pncp_source() == "source"
        rest_mock.assert_called_once()
