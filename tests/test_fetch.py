from unittest.mock import Mock
from baliza.main import fetch_data_from_pncp


def test_fetch_data_from_pncp_success(mocker):
    """Tests that fetch_data_from_pncp returns data on success."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"key": "value"}
    mocker.patch("requests.get", return_value=mock_response)

    data = fetch_data_from_pncp("/v1/contratacoes/publicacao", {})

    assert data == {"key": "value"}
