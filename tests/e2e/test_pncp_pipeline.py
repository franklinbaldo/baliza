"""
Modern E2E test suite for DLT-based PNCP pipeline.
Tests the complete extraction pipeline using real DLT components.
"""

import pytest
import dlt
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import date, timedelta

from baliza.extraction.pipeline import pncp_source, run_structured_extraction
from baliza.extraction.config import create_pncp_rest_config
from baliza.extraction.gap_detector import find_extraction_gaps
from baliza.schemas import ModalidadeContratacao
from baliza.utils.completion_tracking import mark_extraction_completed, get_completed_extractions


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_pncp_response():
    """Sample PNCP API response for mocking."""
    return {
        "data": [
            {
                "numeroControlePNCP": "12345-2024-001",
                "anoCompra": 2024,
                "sequencialCompra": 1,
                "valorTotalEstimado": 100000.00,
                "modalidadeId": 6,
                "modalidadeNome": "Pregão Eletrônico",
                "situacaoCompraId": "1",
                "dataPublicacaoPncp": "2024-01-15T10:00:00Z",
                "orgaoEntidade": {
                    "cnpj": "12345678000123",
                    "razaoSocial": "Teste Órgão",
                    "poderId": "E",
                    "esferaId": "F"
                }
            }
        ],
        "totalRegistros": 1,
        "totalPaginas": 1,
        "numeroPagina": 1,
        "paginasRestantes": 0,
        "empty": False
    }


class TestPNCPPipelineConfig:
    """Test DLT configuration generation."""
    
    def test_create_basic_config(self):
        """Test basic REST API config creation."""
        config = create_pncp_rest_config(
            start_date="20240101",
            end_date="20240131"
        )
        
        assert "client" in config
        assert "resources" in config
        assert config["client"]["base_url"] == "https://pncp.gov.br/api/consulta"
        assert len(config["resources"]) > 0
        
        # Check first resource structure
        resource = config["resources"][0]
        assert "name" in resource
        assert "endpoint" in resource
        assert "primary_key" in resource
        assert resource["write_disposition"] == "merge"
    
    def test_config_with_modalidades(self):
        """Test config generation with specific modalidades."""
        modalidades = [ModalidadeContratacao.PREGAO_ELETRONICO.value]
        config = create_pncp_rest_config(
            start_date="20240101",
            end_date="20240131",
            modalidades=modalidades
        )
        
        # Should have resources for endpoints that require modalidades
        resource_names = [r["name"] for r in config["resources"]]
        assert any("contratacoes_publicacao" in name for name in resource_names)


class TestGapDetection:
    """Test gap detection functionality."""
    
    def test_find_gaps_no_existing_data(self, temp_output_dir):
        """Test gap detection when no data exists."""
        gaps = find_extraction_gaps(
            start_date="20240101",
            end_date="20240131",
            endpoints=["contratacoes_publicacao"]
        )
        
        assert len(gaps) > 0
        assert gaps[0].endpoint == "contratacoes_publicacao"
        assert gaps[0].start_date == "20240101"
        assert gaps[0].end_date == "20240131"
    
    def test_find_gaps_with_completed_data(self, temp_output_dir):
        """Test gap detection when some data already exists."""
        # Mark some extractions as completed
        mark_extraction_completed(
            temp_output_dir,
            "20240101",
            "20240131", 
            ["contratacoes_publicacao"]
        )
        
        # Patch the gap detector to use our temp directory
        with patch('baliza.extraction.gap_detector.get_completed_extractions') as mock_get:
            mock_get.return_value = get_completed_extractions(temp_output_dir)
            
            gaps = find_extraction_gaps(
                start_date="20240101",
                end_date="20240131",
                endpoints=["contratacoes_publicacao"]
            )
            
            # Should have no gaps for completed data
            assert len(gaps) == 0


class TestPNCPSource:
    """Test DLT source creation."""
    
    @patch('baliza.extraction.pipeline.find_extraction_gaps')
    def test_source_creation_with_gaps(self, mock_find_gaps):
        """Test source creation when gaps exist."""
        from baliza.extraction.gap_detector import DataGap
        
        # Mock gaps
        mock_gaps = [
            DataGap("20240101", "20240131", "contratacoes_publicacao")
        ]
        mock_find_gaps.return_value = mock_gaps
        
        source = pncp_source(
            start_date="20240101",
            end_date="20240131"
        )
        
        assert source is not None
        # Source should be created for the gap
        mock_find_gaps.assert_called_once()
    
    @patch('baliza.extraction.pipeline.find_extraction_gaps')
    def test_source_creation_no_gaps(self, mock_find_gaps):
        """Test source creation when no gaps exist."""
        mock_find_gaps.return_value = []
        
        source = pncp_source(
            start_date="20240101",
            end_date="20240131"
        )
        
        assert source is not None
        # Should return empty source
        mock_find_gaps.assert_called_once()


class TestDLTIntegration:
    """Test DLT pipeline integration."""
    
    @pytest.mark.integration
    @patch('dlt.sources.rest_api.rest_api_source')
    def test_pipeline_run_mocked(self, mock_rest_api, temp_output_dir, sample_pncp_response):
        """Test pipeline run with mocked DLT source."""
        # Create mock DLT source that returns sample data
        mock_source = MagicMock()
        mock_resource = MagicMock()
        mock_resource.__iter__ = lambda x: iter([sample_pncp_response["data"][0]])
        mock_source.resources = {"contratacoes_publicacao": mock_resource}
        mock_rest_api.return_value = mock_source
        
        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name="test_pncp",
            destination="duckdb",
            dataset_name="test_pncp_data"
        )
        
        # Create source (will use mocked rest_api_source)
        source = pncp_source(
            start_date="20240101",
            end_date="20240131",
            endpoints=["contratacoes_publicacao"]
        )
        
        # Run pipeline
        info = pipeline.run(source)
        
        # Verify pipeline ran successfully
        assert info is not None
        # Note: In real integration, we'd verify data was loaded
    
    def test_completion_tracking(self, temp_output_dir):
        """Test completion tracking functionality."""
        endpoints = ["contratacoes_publicacao", "contratos"]
        
        # Mark extraction as completed
        mark_extraction_completed(
            temp_output_dir,
            "20240101",
            "20240131",
            endpoints
        )
        
        # Verify completion was recorded
        completed = get_completed_extractions(temp_output_dir)
        
        for endpoint in endpoints:
            assert endpoint in completed
            assert "2024-01" in completed[endpoint]
        
        # Check completion marker files exist
        for endpoint in endpoints:
            marker_path = Path(temp_output_dir) / endpoint / "2024" / "01" / ".completed"
            assert marker_path.exists()


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_invalid_date_range(self):
        """Test handling of invalid date ranges."""
        with pytest.raises((ValueError, TypeError)):
            create_pncp_rest_config(
                start_date="invalid",
                end_date="20240131"
            )
    
    def test_invalid_modalidade(self):
        """Test handling of invalid modalidade values."""
        # Should not raise error, but may produce unexpected results
        config = create_pncp_rest_config(
            start_date="20240101",
            end_date="20240131",
            modalidades=[999]  # Invalid modalidade
        )
        
        assert config is not None
        # API will handle invalid modalidade values


class TestDataTransformation:
    """Test data transformation and processing steps."""
    
    def test_hash_id_generation(self, sample_pncp_response):
        """Test hash-based ID generation for deduplication."""
        from baliza.extraction.config import _add_hash_id
        
        record = sample_pncp_response["data"][0]
        processed = _add_hash_id(record)
        
        assert "_dlt_id" in processed
        assert processed["_dlt_id"] is not None
        assert len(processed["_dlt_id"]) > 0
        
        # Hash should be consistent
        processed2 = _add_hash_id(record)
        assert processed["_dlt_id"] == processed2["_dlt_id"]
    
    def test_metadata_addition(self, sample_pncp_response):
        """Test metadata addition to records."""
        from baliza.extraction.config import _add_metadata
        
        record = sample_pncp_response["data"][0]
        processed = _add_metadata(record)
        
        assert "_baliza_extracted_at" in processed
        assert processed["_baliza_extracted_at"] == date.today().isoformat()


class TestPerformanceAndScaling:
    """Test performance considerations and scaling."""
    
    def test_large_date_range_handling(self):
        """Test handling of large date ranges."""
        # Test 1 year range
        config = create_pncp_rest_config(
            start_date="20230101",
            end_date="20231231"
        )
        
        assert config is not None
        assert len(config["resources"]) > 0
        
        # Resources should have appropriate pagination settings
        for resource in config["resources"]:
            params = resource["endpoint"]["params"]
            assert "tamanhoPagina" in params
            assert params["tamanhoPagina"] > 0
    
    def test_multiple_modalidades(self):
        """Test handling of multiple modalidades."""
        all_modalidades = [m.value for m in ModalidadeContratacao]
        
        config = create_pncp_rest_config(
            start_date="20240101",
            end_date="20240131",
            modalidades=all_modalidades
        )
        
        assert config is not None
        # Should handle all modalidades without errors


@pytest.mark.integration
class TestRealAPIIntegration:
    """Integration tests against real PNCP API (use sparingly)."""
    
    # TODO: These tests are currently skipped. It is crucial to enable and run
    #       these integration tests against the real PNCP API as part of a
    #       dedicated integration testing suite. This ensures that the pipeline
    #       functions correctly with the actual API, catching any issues that
    #       might not be apparent with mocked data or unit tests.
    
    @pytest.mark.skip(reason="Real API test - enable for integration testing")
    def test_real_api_connection(self):
        """Test actual connection to PNCP API."""
        import requests
        
        # Test basic API connectivity
        response = requests.get(
            "https://pncp.gov.br/api/consulta/contratacoes/publicacao",
            params={
                "dataInicial": "20240101",
                "dataFinal": "20240101",
                "codigoModalidadeContratacao": 6,
                "tamanhoPagina": 1,
                "pagina": 1
            },
            timeout=30
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "totalRegistros" in data
    
    @pytest.mark.skip(reason="Real API test - enable for integration testing")  
    def test_full_pipeline_integration(self, temp_output_dir):
        """Test full pipeline against real API (small dataset)."""
        # Run extraction for 1 day only
        yesterday = date.today() - timedelta(days=1)
        date_str = yesterday.strftime("%Y%m%d")
        
        result = run_structured_extraction(
            start_date=date_str,
            end_date=date_str,
            endpoints=["contratacoes_publicacao"],
            output_dir=temp_output_dir
        )
        
        # Should complete without errors
        assert result is not None
        
        # Check output files were created
        output_path = Path(temp_output_dir)
        assert output_path.exists()
        
        # Check completion markers
        completed = get_completed_extractions(temp_output_dir)
        assert "contratacoes_publicacao" in completed


if __name__ == "__main__":
    pytest.main([__file__, "-v"])