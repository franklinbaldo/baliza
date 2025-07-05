import pytest
from unittest.mock import Mock
from baliza.main import harvest_endpoint_data
from tenacity import RetryError


def test_harvest_endpoint_data_single_page(mocker):
    """Tests harvesting data from a single page."""
    mock_fetch = mocker.patch('baliza.main.fetch_data_from_pncp')
    mock_fetch.return_value = {
        "items": [{"id": 1}, {"id": 2}],
        "totalPaginas": 1,
        "paginaAtual": 1
    }

    endpoint_cfg = {"api_path": "/test"}
    records = harvest_endpoint_data("2024-01-01", "test_endpoint", endpoint_cfg)

    assert len(records) == 2
    mock_fetch.assert_called_once()


def test_harvest_endpoint_data_multiple_pages(mocker):
    """Tests harvesting data across multiple pages."""
    mock_fetch = mocker.patch('baliza.main.fetch_data_from_pncp')
    mock_fetch.side_effect = [
        {
            "items": [{"id": 1}],
            "totalPaginas": 2,
            "paginaAtual": 1
        },
        {
            "items": [{"id": 2}],
            "totalPaginas": 2,
            "paginaAtual": 2
        }
    ]

    endpoint_cfg = {"api_path": "/test"}
    records = harvest_endpoint_data("2024-01-01", "test_endpoint", endpoint_cfg)

    assert len(records) == 2
    assert mock_fetch.call_count == 2


def test_harvest_endpoint_data_no_items(mocker):
    """Tests harvesting when no items are returned."""
    mock_fetch = mocker.patch('baliza.main.fetch_data_from_pncp')
    mock_fetch.return_value = {
        "items": [],
        "totalPaginas": 0,
        "paginaAtual": 1
    }

    endpoint_cfg = {"api_path": "/test"}
    records = harvest_endpoint_data("2024-01-01", "test_endpoint", endpoint_cfg)

    assert len(records) == 0
    mock_fetch.assert_called_once()


def test_harvest_endpoint_data_fetch_error(mocker):
    """Tests harvesting when fetch_data_from_pncp raises an error."""
    mock_fetch = mocker.patch('baliza.main.fetch_data_from_pncp')
    mock_fetch.side_effect = RetryError("API Error")

    endpoint_cfg = {"api_path": "/test"}
    records = harvest_endpoint_data("2024-01-01", "test_endpoint", endpoint_cfg)

    assert records is None
    mock_fetch.assert_called_once()
