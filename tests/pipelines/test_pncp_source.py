import pytest
import dlt
from baliza.pipelines.pncp import pncp_source
from baliza.legacy.enums import PncpEndpoint

def test_pncp_source_lists_all_endpoints():
    """Verifies that the pncp_source lists all endpoints by default."""
    source = pncp_source()
    assert len(source.resources) == len(list(PncpEndpoint))
    for endpoint in PncpEndpoint:
        assert endpoint.name in source.resources

def test_pncp_source_lists_selected_endpoints():
    """Verifies that the pncp_source lists only selected endpoints."""
    endpoints = [PncpEndpoint.CONTRATOS, PncpEndpoint.ATAS]
    source = pncp_source(endpoints=endpoints)
    assert len(source.resources) == 2
    assert PncpEndpoint.CONTRATOS.name in source.resources
    assert PncpEndpoint.ATAS.name in source.resources
    assert PncpEndpoint.LICITACOES.name not in source.resources
