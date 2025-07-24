"""
Tests for endpoint utilities
"""

import pytest
from datetime import date
from unittest.mock import patch

from src.baliza.utils.endpoints import (
    build_contratacao_url,
    build_contratos_url,
    build_atas_url,
    DateRangeHelper,
    EndpointValidator,
    PaginationHelper,
    get_phase_2a_endpoints,
)
from src.baliza.enums import ModalidadeContratacao


class TestEndpointBuilder:
    """Test URL building functionality"""

    def test_build_contratacoes_url(self):
        """Test building contratacoes URL with all parameters"""
        url = build_contratacao_url(
            data_inicial="2024-01-01",
            data_final="2024-01-31",
            modalidade=1,
            pagina=1,
        )

        expected = (
            "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
            "?dataInicial=20240101&dataFinal=20240131"
            "&codigoModalidadeContratacao=6&pagina=1"
        )
        assert url == expected

    def test_build_contratos_url(self):
        """Test building contratos URL"""
        url = build_contratos_url(
            data_inicial="2024-01-01", data_final="2024-01-31", pagina=2
        )

        expected = (
            "https://pncp.gov.br/api/consulta/v1/contratos"
            "?dataInicial=20240101&dataFinal=20240131&pagina=2"
        )
        assert url == expected

    def test_build_atas_url(self):
        """Test building atas URL"""
        url = build_atas_url(
            data_inicial="2024-01-01", data_final="2024-01-31", pagina=3
        )

        expected = (
            "https://pncp.gov.br/api/consulta/v1/atas"
            "?dataInicial=20240101&dataFinal=20240131&pagina=3"
        )
        assert url == expected


class TestDateRangeHelper:
    """Test date range utilities"""

    def test_get_last_n_days(self):
        """Test getting last N days range"""
        with patch("src.baliza.utils.endpoints.date") as mock_date:
            mock_date.today.return_value = date(2024, 1, 15)

            start, end = DateRangeHelper.get_last_n_days(7)

            assert start == "20240108"
            assert end == "20240115"

    def test_get_current_month(self):
        """Test getting current month range"""
        with patch("src.baliza.utils.endpoints.date") as mock_date:
            mock_date.today.return_value = date(2024, 1, 15)
            start, end = DateRangeHelper.get_current_month()
            assert start == "20240101"
            assert end == "20240115"

        assert start == "20240101"
        assert end == "20240131"

    def test_parse_date(self):
        """Test parsing date strings"""
        # Valid date
        parsed = DateRangeHelper.parse_date("20240115")
        assert parsed == date(2024, 1, 15)

        # Invalid date should raise ValueError
        with pytest.raises(ValueError):
            DateRangeHelper.parse_date("invalid-date")

    def test_chunk_date_range(self):
        """Test splitting date range into chunks"""
        chunks = DateRangeHelper.chunk_date_range("20240101", "20240105", 2)
        assert chunks == [("20240101", "20240102"), ("20240103", "20240104"), ("20240105", "20240105")]


class TestPaginationHelper:
    """Test pagination utilities"""

    def test_calculate_total_pages(self):
        """Test calculating total pages"""
        assert PaginationHelper.calculate_total_pages(100, 20) == 5
        assert PaginationHelper.calculate_total_pages(101, 20) == 6
        assert PaginationHelper.calculate_total_pages(0, 20) == 0


    def test_get_page_ranges(self):
        """Test getting page ranges for parallel processing"""
        ranges = PaginationHelper.get_page_ranges(10, batch_size=3)

        expected = [
            (1, 3),  # pages 1-3
            (4, 6),  # pages 4-6
            (7, 9),  # pages 7-9
            (10, 10),  # page 10 only
        ]

        assert ranges == expected

    def test_estimate_requests_needed(self):
        """Test estimating requests needed"""
        # Test with typical response
        estimated = PaginationHelper.estimate_requests_needed(
            date_range_days=30, avg_records_per_day=100, records_per_page=20
        )

        # 30 days * 100 records/day / 20 records/page = 150 requests
        assert estimated == 150


class TestEndpointValidator:
    """Test endpoint validation utilities"""

    def test_validate_modalidade(self):
        """Test modalidade validation"""
        # Valid modalidade
        EndpointValidator.validate_modalidade(1)

        # Invalid modalidade
        with pytest.raises(ValueError):
            EndpointValidator.validate_modalidade(999)
        with pytest.raises(ValueError):
            EndpointValidator.validate_modalidade(None)


class TestPhase2AEndpoints:
    """Test Phase 2A endpoint configuration"""

    def test_get_phase_2a_endpoints(self):
        """Test getting Phase 2A endpoints"""
        endpoints = get_phase_2a_endpoints()

        # Should return list of endpoint configurations
        assert isinstance(endpoints, list)
        assert len(endpoints) > 0

        # Check required fields and values
        for endpoint in endpoints:
            assert isinstance(endpoint, str)

        # Check high priority endpoints are included
        assert "contratacoes_publicacao" in endpoints
        assert "contratos" in endpoints
        assert "atas" in endpoints
