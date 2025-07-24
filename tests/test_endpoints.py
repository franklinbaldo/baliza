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
    PaginationHelper,
    EndpointValidator,
    get_phase_2a_endpoints,
)
from src.baliza.enums import ModalidadeContratacao


class TestURLBuilder:
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
            "?dataInicial=2024-01-01&dataFinal=2024-01-31"
            "&codigoModalidadeContratacao=1&pagina=1"
        )
        assert url == expected

    def test_build_contratos_url(self):
        """Test building contratos URL"""
        url = build_contratos_url(
            data_inicial="2024-01-01", data_final="2024-01-31", pagina=2
        )

        expected = (
            "https://pncp.gov.br/api/consulta/v1/contratos"
            "?dataInicial=2024-01-01&dataFinal=2024-01-31&pagina=2"
        )
        assert url == expected

    def test_build_atas_url(self):
        """Test building atas URL"""
        url = build_atas_url(
            data_inicial="2024-01-01", data_final="2024-01-31", pagina=3
        )

        expected = (
            "https://pncp.gov.br/api/consulta/v1/atas"
            "?dataInicial=2024-01-01&dataFinal=2024-01-31&pagina=3"
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

    def test_get_month_range(self):
        """Test getting month range"""
        start, end = DateRangeHelper.get_month_range(2024, 1)

        assert start == "20240101"
        assert end == "20240131"

    def test_parse_date_string(self):
        """Test parsing date strings"""
        # Valid date
        parsed = DateRangeHelper.parse_date_string("2024-01-15")
        assert parsed == date(2024, 1, 15)

        # Invalid date should raise ValueError
        with pytest.raises(ValueError):
            DateRangeHelper.parse_date_string("invalid-date")

    def test_validate_date_range(self):
        """Test date range validation"""
        # Valid range
        assert DateRangeHelper.validate_date_range("2024-01-01", "2024-01-31")

        # Invalid range (start > end)
        assert not DateRangeHelper.validate_date_range("2024-01-31", "2024-01-01")

        # Invalid date format
        assert not DateRangeHelper.validate_date_range("invalid", "2024-01-31")


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
        validator = EndpointValidator()

        # Valid modalidade
        assert validator.validate_modalidade(1)

        # Invalid modalidade
        assert not validator.validate_modalidade(999)
        assert not validator.validate_modalidade(None)

    def test_validate_date_format(self):
        """Test date format validation"""
        validator = EndpointValidator()

        # Valid formats
        assert validator.validate_date_format("2024-01-01")
        assert validator.validate_date_format("2024-12-31")

        # Invalid formats
        assert not validator.validate_date_format("24-01-01")
        assert not validator.validate_date_format("2024/01/01")
        assert not validator.validate_date_format("invalid")

    def test_validate_pagination_params(self):
        """Test pagination parameter validation"""
        validator = EndpointValidator()

        # Valid params
        assert validator.validate_pagination_params(1, 20)
        assert validator.validate_pagination_params(100, 50)

        # Invalid params
        assert not validator.validate_pagination_params(0, 20)  # page < 1
        assert not validator.validate_pagination_params(1, 0)  # size < 1
        assert not validator.validate_pagination_params(1, 101)  # size > 100

    def test_validate_endpoint_params(self):
        """Test complete endpoint parameter validation"""
        validator = EndpointValidator()

        # Valid contratacoes params
        valid_params = {
            "data_inicial": "2024-01-01",
            "data_final": "2024-01-31",
            "modalidade": 1,
            "pagina": 1,
        }

        result = validator.validate_endpoint_params(
            "contratacoes_publicacao", valid_params
        )
        assert result["valid"]
        assert result["errors"] == []

        # Invalid params
        invalid_params = {
            "data_inicial": "invalid-date",
            "data_final": "2024-01-31",
            "modalidade": 999,
            "pagina": 0,
        }

        result = validator.validate_endpoint_params(
            "contratacoes_publicacao", invalid_params
        )
        assert not result["valid"]
        assert len(result["errors"]) > 0


class TestPhase2AEndpoints:
    """Test Phase 2A endpoint configuration"""

    def test_get_phase_2a_endpoints(self):
        """Test getting Phase 2A endpoints"""
        endpoints = get_phase_2a_endpoints()

        # Should return list of endpoint configurations
        assert isinstance(endpoints, list)
        assert len(endpoints) > 0

        # Check required fields
        for endpoint in endpoints:
            assert isinstance(endpoint, str)

        # Check high priority endpoints are included
        assert "contratacoes_publicacao" in endpoints
        assert "contratos" in endpoints
        assert "atas" in endpoints
