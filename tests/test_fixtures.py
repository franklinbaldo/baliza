import pytest
import json
from pathlib import Path
from unittest.mock import patch, Mock
import zlib

from src.baliza.utils.http_client import PNCPResponse, APIRequest, PNCPClient
from src.baliza.utils.endpoints import EndpointBuilder, DateRangeHelper
from src.baliza.enums import ModalidadeContratacao, get_enum_by_value
from src.baliza.config import settings


@pytest.fixture
def contratacoes_publicacao_fixture():
    """Loads the contratacoes_publicacao_response.json fixture."""
    fixture_path = Path(__file__).parent / "fixtures/contratacoes_publicacao_response.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def contratos_fixture():
    """Loads the contratos_response.json fixture."""
    fixture_path = Path(__file__).parent / "fixtures/contratos_response.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def atas_fixture():
    """Loads the atas_response.json fixture."""
    fixture_path = Path(__file__).parent / "fixtures/atas_response.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def orgaos_fixture():
    """Loads the orgaos_response.json fixture."""
    fixture_path = Path(__file__).parent / "fixtures/orgaos_response.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.asyncio
class TestFixtureLoadingAndParsing:
    """Tests that collected fixtures can be loaded and parsed correctly."""

    async def test_contratacoes_publicacao_fixture(self, contratacoes_publicacao_fixture):
        """Test loading and basic structure of contratacoes_publicacao fixture."""
        assert "data" in contratacoes_publicacao_fixture
        assert "totalRegistros" in contratacoes_publicacao_fixture
        assert isinstance(contratacoes_publicacao_fixture["data"], list)
        assert isinstance(contratacoes_publicacao_fixture["totalRegistros"], int)
        
        if contratacoes_publicacao_fixture["data"]:
            # Test parsing with PNCPResponse model
            response = PNCPResponse(**contratacoes_publicacao_fixture)
            assert len(response.data) == len(contratacoes_publicacao_fixture["data"])
            assert response.totalRegistros == contratacoes_publicacao_fixture["totalRegistros"]

            # Test fetch_data with mocked response
            client = PNCPClient()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"etag": "test_etag"}
            with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
                mock_get.return_value.json.return_value = contratacoes_publicacao_fixture
                api_request = await client.fetch_endpoint_data("contratacoes_publicacao", "https://test.com/api")
                assert api_request.http_status == 200
                assert api_request.endpoint == "contratacoes_publicacao"
                assert api_request.payload_size > 0
                
                # Decompress and verify payload
                decompressed_payload = zlib.decompress(api_request.payload_compressed).decode("utf-8")
                assert json.loads(decompressed_payload) == contratacoes_publicacao_fixture


    async def test_contratos_fixture(self, contratos_fixture):
        """Test loading and basic structure of contratos fixture."""
        assert "data" in contratos_fixture
        assert "totalRegistros" in contratos_fixture
        assert isinstance(contratos_fixture["data"], list)
        assert isinstance(contratos_fixture["totalRegistros"], int)

        if contratos_fixture["data"]:
            response = PNCPResponse(**contratos_fixture)
            assert len(response.data) == len(contratos_fixture["data"])
            assert response.totalRegistros == contratos_fixture["totalRegistros"]

            client = PNCPClient()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"etag": "test_etag"}
            with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
                mock_get.return_value.json.return_value = contratos_fixture
                api_request = await client.fetch_endpoint_data("contratos", "https://test.com/api")
                assert api_request.http_status == 200
                assert api_request.endpoint == "contratos"
                assert api_request.payload_size > 0
                
                decompressed_payload = zlib.decompress(api_request.payload_compressed).decode("utf-8")
                assert json.loads(decompressed_payload) == contratos_fixture

    async def test_atas_fixture(self, atas_fixture):
        """Test loading and basic structure of atas fixture."""
        assert "data" in atas_fixture
        assert "totalRegistros" in atas_fixture
        assert isinstance(atas_fixture["data"], list)
        assert isinstance(atas_fixture["totalRegistros"], int)

        if atas_fixture["data"]:
            response = PNCPResponse(**atas_fixture)
            assert len(response.data) == len(atas_fixture["data"])
            assert response.totalRegistros == atas_fixture["totalRegistros"]

            client = PNCPClient()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"etag": "test_etag"}
            with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
                mock_get.return_value.json.return_value = atas_fixture
                api_request = await client.fetch_endpoint_data("atas", "https://test.com/api")
                assert api_request.http_status == 200
                assert api_request.endpoint == "atas"
                assert api_request.payload_size > 0
                
                decompressed_payload = zlib.decompress(api_request.payload_compressed).decode("utf-8")
                assert json.loads(decompressed_payload) == atas_fixture

    @pytest.mark.skip(reason="orgaos_response.json is not being collected due to API 404")
    async def test_orgaos_fixture(self, orgaos_fixture):
        """Test loading and basic structure of orgaos fixture."""
        assert "data" in orgaos_fixture
        assert "totalRegistros" in orgaos_fixture
        assert isinstance(orgaos_fixture["data"], list)
        assert isinstance(orgaos_fixture["totalRegistros"], int)

        if orgaos_fixture["data"]:
            response = PNCPResponse(**orgaos_fixture)
            assert len(response.data) == len(orgaos_fixture["data"])
            assert response.totalRegistros == orgaos_fixture["totalRegistros"]

            client = PNCPClient()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {"etag": "test_etag"}
            with patch("httpx.AsyncClient.get", return_value=mock_response) as mock_get:
                mock_get.return_value.json.return_value = orgaos_fixture
                api_request = await client.fetch_endpoint_data("orgaos", "https://test.com/api")
                assert api_request.http_status == 200
                assert api_request.endpoint == "orgaos"
                assert api_request.payload_size > 0
                
                decompressed_payload = zlib.decompress(api_request.payload_compressed).decode("utf-8")
                assert json.loads(decompressed_payload) == orgaos_fixture
