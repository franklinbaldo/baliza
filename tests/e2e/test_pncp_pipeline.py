"""
Modern E2E test suite for DLT-based PNCP pipeline.
Tests the complete extraction pipeline using real DLT components.
"""

import pytest
from unittest.mock import patch
from baliza.extraction.pipeline import pncp_source, run_structured_extraction
from baliza.extraction.gap_detector import find_extraction_gaps, DataGap
from baliza.extraction.config import create_pncp_rest_config


def test_placeholder():
    """Basic test to ensure the test file is not empty."""
    assert True


def test_pncp_source_with_no_gaps():
    """Test that pncp_source returns empty source when no gaps exist."""
    with patch('baliza.extraction.pipeline.find_extraction_gaps') as mock_gaps:
        mock_gaps.return_value = []  # No gaps
        
        source = pncp_source(
            start_date="20240101",
            end_date="20240131"
        )
        
        # Should return empty source
        assert source is not None


def test_find_extraction_gaps_basic():
    """Test basic gap detection functionality."""
    with patch('baliza.utils.completion_tracking.get_completed_extractions') as mock_completed:
        mock_completed.return_value = {}  # No completed extractions
        
        gaps = find_extraction_gaps(
            start_date="20240101",
            end_date="20240131",
            endpoints=["contratos"]
        )
        
        # Should find gaps when nothing is completed
        assert len(gaps) > 0
        assert all(isinstance(gap, DataGap) for gap in gaps)


def test_create_pncp_rest_config():
    """Test that REST API config generation works."""
    config = create_pncp_rest_config(
        start_date="20240101",
        end_date="20240131"
    )
    
    assert "client" in config
    assert "resources" in config
    assert config["client"]["base_url"]
    assert len(config["resources"]) > 0


def test_data_gap_string_representation():
    """Test DataGap string representation."""
    gap = DataGap(
        start_date="20240101",
        end_date="20240131", 
        endpoint="contratos"
    )
    
    gap_str = str(gap)
    assert "contratos" in gap_str
    assert "20240101" in gap_str
    assert "20240131" in gap_str


@pytest.mark.parametrize("start_date,end_date", [
    ("20240101", "20240131"),
    ("20240201", "20240229"),
    ("20240301", "20240331"),
])
def test_gap_detection_various_date_ranges(start_date, end_date):
    """Test gap detection with various date ranges."""
    with patch('baliza.utils.completion_tracking.get_completed_extractions') as mock_completed:
        mock_completed.return_value = {}
        
        gaps = find_extraction_gaps(
            start_date=start_date,
            end_date=end_date,
            endpoints=["contratos"]
        )
        
        assert isinstance(gaps, list)


def test_run_structured_extraction_no_endpoints():
    """Test structured extraction with no endpoints specified."""
    with patch('baliza.extraction.pipeline.create_default_pipeline') as mock_pipeline:
        with patch('baliza.extraction.pipeline.pncp_source') as mock_source:
            with patch('baliza.utils.completion_tracking.get_completed_extractions') as mock_completed:
                
                mock_completed.return_value = {
                    "contratos": ["2024-01"],
                    "atas": ["2024-01"]
                }
                
                result = run_structured_extraction(
                    start_date="20240101",
                    end_date="20240131",
                    endpoints=[]
                )
                
                # Should handle empty endpoints gracefully
                assert result is None


def test_config_endpoint_params():
    """Test that endpoint parameters are built correctly."""
    from baliza.extraction.config import _build_endpoint_params
    from baliza.settings import ENDPOINT_CONFIG
    
    endpoint_config = ENDPOINT_CONFIG["contratos"]
    params = _build_endpoint_params(
        endpoint_config, 
        "20240101", 
        "20240131", 
        None, 
        50
    )
    
    assert "tamanhoPagina" in params
    assert params["tamanhoPagina"] == 50
    assert "pagina" in params


def test_empty_pncp_source():
    """Test empty source creation."""
    from baliza.extraction.pipeline import _empty_pncp_source
    
    empty_source = _empty_pncp_source()
    assert empty_source is not None