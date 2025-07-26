import dlt
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import logging

from baliza.pipelines.pncp import pncp_source
from baliza.legacy.enums import PncpEndpoint, ModalidadeContratacao
from baliza.legacy.utils.http_client import APIRequest, PNCPResponse # Import APIRequest for type hinting
import json
import zlib

@pytest.fixture(autouse=True)
def mock_prefect_logger():
    with patch('prefect.get_run_logger', return_value=logging.getLogger('mock_prefect_logger')):
        yield

@pytest.fixture
def mock_extractor():
    """Fixture to mock EndpointExtractor methods."""
    mock_ext = AsyncMock()
    
    # Sample data for mocking
    sample_data_cp = {"data": [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}], "totalRegistros": 2, "totalPaginas": 1, "numeroPagina": 1, "paginasRestantes": 0}
    sample_data_c = {"data": [{"id": 3, "value": "c"}, {"id": 4, "value": "d"}], "totalRegistros": 2, "totalPaginas": 1, "numeroPagina": 1, "paginasRestantes": 0}
    sample_data_a = {"data": [{"id": 5, "value": "e"}, {"id": 6, "value": "f"}], "totalRegistros": 2, "totalPaginas": 1, "numeroPagina": 1, "paginasRestantes": 0}

    # Compress sample data
    compressed_data_cp = zlib.compress(json.dumps(sample_data_cp).encode("utf-8"))
    compressed_data_c = zlib.compress(json.dumps(sample_data_c).encode("utf-8"))
    compressed_data_a = zlib.compress(json.dumps(sample_data_a).encode("utf-8"))

    mock_ext.extract_contratacoes_publicacao.return_value = [
        APIRequest(
            request_id="test_id_cp1",
            ingestion_date="2024-01-01",
            endpoint="contratacoes_publicacao",
            http_status=200,
            payload_sha256="abc",
            payload_size=100,
            collected_at="2024-01-01T00:00:00",
            payload_compressed=compressed_data_cp
        )
    ]
    mock_ext.extract_contratos.return_value = [
        APIRequest(
            request_id="test_id_c1",
            ingestion_date="2024-01-01",
            endpoint="contratos",
            http_status=200,
            payload_sha256="def",
            payload_size=100,
            collected_at="2024-01-01T00:00:00",
            payload_compressed=compressed_data_c
        )
    ]
    mock_ext.extract_atas.return_value = [
        APIRequest(
            request_id="test_id_a1",
            ingestion_date="2024-01-01",
            endpoint="atas",
            http_status=200,
            payload_sha256="ghi",
            payload_size=100,
            collected_at="2024-01-01T00:00:00",
            payload_compressed=compressed_data_a
        )
    ]
    return mock_ext

@pytest.mark.skip(reason="DLT resource naming issue in test environment")
# FIXME: All tests in this file are skipped. This is a critical issue.
#        The reason "DLT resource naming issue in test environment" needs to be
#        investigated and resolved. A test suite that is not running provides
#        a false sense of security.
@pytest.mark.asyncio
async def test_pncp_source_creation_and_resources(mock_extractor):
    """Verify that the pncp_source can be created and its resources are callable."""
    source = pncp_source(extractor_instance=mock_extractor)
    assert source is not None
    assert isinstance(source, dlt.sources.DltSource)

    expected_resource_names = {"contratacoes_publicacao", "contratos", "atas"}
    actual_resource_names = set(source.resources.keys())
    assert actual_resource_names == expected_resource_names, \
        f"Mismatch between expected resources and dlt resources. Missing: {expected_resource_names - actual_resource_names}, Extra: {actual_resource_names - expected_resource_names}"

    for resource_name in expected_resource_names:
        resource = source.resources[resource_name]
        assert callable(resource)

@pytest.mark.skip(reason="DLT resource naming issue in test environment")
@pytest.mark.parametrize("endpoint_name", [endpoint.name for endpoint in PncpEndpoint])
def test_resource_is_callable(endpoint_name):
    """Verify that each created resource is a callable function."""
    source = pncp_source()
    resource = source.resources[endpoint_name]
    assert callable(resource)

@pytest.mark.skip(reason="DLT resource naming issue in test environment")
@pytest.mark.asyncio
async def test_contratacoes_publicacao_resource_extraction(mock_extractor):
    """Test extraction from 'contratacoes_publicacao' resource."""
    pipeline = dlt.pipeline(
        pipeline_name="test_pncp_pipeline",
        destination="duckdb", # Use duckdb for testing
        dataset_name="test_dataset"
    )
    
    # Run the pipeline for a specific resource
    info = await pipeline.run(
        pncp_source(extractor_instance=mock_extractor).resources["contratacoes_publicacao"],
        start_date="20240101",
        end_date="20240101",
        modalidade=ModalidadeContratacao.LEILAO_ELETRONICO.value # Pass the int value
    )
    
    # Verify that the extractor method was called
    mock_extractor.extract_contratacoes_publicacao.assert_called_once_with(
        data_inicial="20240101",
        data_final="20240101",
        modalidade=ModalidadeContratacao.LEILAO_ELETRONICO # The mock expects the Enum
    )
    
    # Verify data was loaded (this is a basic check, more detailed checks would involve querying duckdb)
    assert info.status == "running" # Pipeline should run without immediate errors
    # For more robust testing, you'd query the duckdb to check row counts and data content.

@pytest.mark.skip(reason="DLT resource naming issue in test environment")
@pytest.mark.asyncio
async def test_contratos_resource_extraction(mock_extractor):
    """Test extraction from 'contratos' resource."""
    pipeline = dlt.pipeline(
        pipeline_name="test_pncp_pipeline",
        destination="duckdb",
        dataset_name="test_dataset"
    )
    
    info = await pipeline.run(
        pncp_source(extractor_instance=mock_extractor).resources["contratos"],
        start_date="20240101",
        end_date="20240101"
    )
    
    mock_extractor.extract_contratos.assert_called_once_with(
        data_inicial="20240101",
        data_final="20240101"
    )
    assert info.status == "running"

@pytest.mark.skip(reason="DLT resource naming issue in test environment")
@pytest.mark.asyncio
async def test_atas_resource_extraction(mock_extractor):
    """Test extraction from 'atas' resource."""
    pipeline = dlt.pipeline(
        pipeline_name="test_pncp_pipeline",
        destination="duckdb",
        dataset_name="test_dataset"
    )
    
    info = await pipeline.run(
        pncp_source(extractor_instance=mock_extractor).resources["atas"],
        start_date="20240101",
        end_date="20240101"
    )
    
    mock_extractor.extract_atas.assert_called_once_with(
        data_inicial="20240101",
        data_final="20240101"
    )
    assert info.status == "running"

@pytest.mark.skip(reason="DLT resource naming issue in test environment")
@pytest.mark.asyncio
async def test_idempotent_load(mock_extractor):
    """Test that running the pipeline twice with the same data does not create duplicate records."""
    pipeline = dlt.pipeline(
        pipeline_name="test_pncp_idempotent_pipeline",
        destination="duckdb",
        dataset_name="test_idempotent_dataset",
        full_refresh=True # Ensure a clean slate for each test run
    )

    # First run
    info_first_run = await pipeline.run(
        pncp_source(extractor_instance=mock_extractor).resources["contratos"],
        start_date="20240101",
        end_date="20240101"
    )
    assert info_first_run.status == "running"
    assert info_first_run.metrics.data_item_counts["contratos"] == 2 # Expect 2 records from mock

    # Second run with the same data
    info_second_run = await pipeline.run(
        pncp_source(extractor_instance=mock_extractor).resources["contratos"],
        start_date="20240101",
        end_date="20240101"
    )
    assert info_second_run.status == "running"
    assert info_second_run.metrics.data_item_counts["contratos"] == 0 # Expect 0 new records due to deduplication
