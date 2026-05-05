"""Integration tests that replay pre-recorded PNCP API cassettes.

These tests validate:
  1. Each cassette contains a valid PNCP response (schema sanity check).
  2. The cassette files are present — CI fails fast when they're missing.
  3. The response structure matches what the Baliza extractor / web UI expects.

Cassettes live in tests/cassettes/pncp/.  Re-record with:
    uv run python scripts/record_pncp_cassettes.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests
import vcr as _vcr_module

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CASSETTE_DIR = Path(__file__).parent.parent / "cassettes" / "pncp"

REQUIRED_CASSETTES = [
    "contratos_page1.yaml",
    "publicacao_pregao_page1.yaml",
    "publicacao_dispensa_page1.yaml",
    "publicacao_inexigibilidade_page1.yaml",
    "publicacao_municipio_pregao_page1.yaml",
]


def _cassette_body(name: str) -> dict:
    """Return the parsed JSON body from a cassette file."""
    import yaml  # local import to keep module-level imports clean

    path = CASSETTE_DIR / name
    cassette = yaml.safe_load(path.read_text(encoding="utf-8"))
    return json.loads(cassette["interactions"][0]["response"]["body"]["string"])


# ---------------------------------------------------------------------------
# Presence check — fails before any VCR replay
# ---------------------------------------------------------------------------


def test_cassette_files_are_present():
    """All required cassette files must exist in the repo."""
    missing = [n for n in REQUIRED_CASSETTES if not (CASSETTE_DIR / n).exists()]
    assert not missing, (
        f"Missing cassettes: {missing}\n"
        "Run `uv run python scripts/record_pncp_cassettes.py` to record them."
    )


# ---------------------------------------------------------------------------
# Schema tests against cassette data (no network — pure YAML parsing)
# ---------------------------------------------------------------------------


class TestContratosSchema:
    """Validates the /api/consulta/v1/contratos response structure."""

    @pytest.fixture(autouse=True)
    def _data(self):
        self.data = _cassette_body("contratos_page1.yaml")

    def test_pagination_fields_present(self):
        assert "totalRegistros" in self.data
        assert "totalPaginas" in self.data
        assert "numeroPagina" in self.data

    def test_data_array_non_empty(self):
        assert isinstance(self.data["data"], list)
        assert len(self.data["data"]) > 0

    def test_total_registros_positive(self):
        assert self.data["totalRegistros"] > 0

    def test_contract_has_required_fields(self):
        contract = self.data["data"][0]
        required = {
            "numeroControlePNCP",
            "orgaoEntidade",
            "niFornecedor",
            "valorGlobal",
            "objetoContrato",
            "dataPublicacaoPncp",
        }
        missing = required - set(contract.keys())
        assert not missing, f"Missing contract fields: {missing}"

    def test_orgao_entidade_shape(self):
        orgao = self.data["data"][0]["orgaoEntidade"]
        assert "cnpj" in orgao
        assert "razaoSocial" in orgao
        assert len(orgao["cnpj"]) == 14, "CNPJ should be 14 digits"

    def test_valor_global_is_numeric(self):
        val = self.data["data"][0]["valorGlobal"]
        assert isinstance(val, (int, float))

    def test_data_publicacao_iso_format(self):
        ts = self.data["data"][0]["dataPublicacaoPncp"]
        assert "T" in ts, "Expected ISO-8601 timestamp with 'T' separator"


class TestPublicacaoPregaoSchema:
    """Validates the publicacao endpoint for Pregão Eletrônico (modalidade 6)."""

    @pytest.fixture(autouse=True)
    def _data(self):
        self.data = _cassette_body("publicacao_pregao_page1.yaml")

    def test_pagination_present(self):
        assert "totalRegistros" in self.data
        assert self.data["totalRegistros"] > 0

    def test_data_array(self):
        assert isinstance(self.data["data"], list)
        assert len(self.data["data"]) > 0

    def test_publicacao_fields(self):
        item = self.data["data"][0]
        required = {"numeroControlePNCP", "orgaoEntidade", "dataPublicacaoPncp"}
        missing = required - set(item.keys())
        assert not missing, f"Missing fields: {missing}"


class TestPublicacaoDispensaSchema:
    """Validates the publicacao endpoint for Dispensa de Licitação (modalidade 8)."""

    @pytest.fixture(autouse=True)
    def _data(self):
        self.data = _cassette_body("publicacao_dispensa_page1.yaml")

    def test_pagination_present(self):
        assert "totalRegistros" in self.data
        assert self.data["totalRegistros"] > 0

    def test_data_array(self):
        assert isinstance(self.data["data"], list)
        assert len(self.data["data"]) > 0

    def test_publicacao_fields(self):
        item = self.data["data"][0]
        assert "numeroControlePNCP" in item
        assert "orgaoEntidade" in item


class TestPublicacaoInexigibilidadeSchema:
    """Validates the publicacao endpoint for Inexigibilidade (modalidade 9)."""

    @pytest.fixture(autouse=True)
    def _data(self):
        self.data = _cassette_body("publicacao_inexigibilidade_page1.yaml")

    def test_pagination_present(self):
        assert "totalRegistros" in self.data

    def test_data_array(self):
        assert isinstance(self.data["data"], list)
        assert len(self.data["data"]) > 0


class TestPublicacaoMunicipioSchema:
    """Validates the publicacao endpoint filtered by município IBGE code.

    Covers the LocalBids / CompararView use case where the web UI passes
    codigoMunicipioIbge to narrow results to a specific city.
    """

    @pytest.fixture(autouse=True)
    def _data(self):
        self.data = _cassette_body("publicacao_municipio_pregao_page1.yaml")

    def test_pagination_present(self):
        assert "totalRegistros" in self.data

    def test_data_array(self):
        assert isinstance(self.data["data"], list)
        assert len(self.data["data"]) > 0

    def test_all_records_belong_to_municipality(self):
        """Every returned record must be from IBGE 3550308 (São Paulo)."""
        for item in self.data["data"]:
            ibge = item.get("unidadeOrgao", {}).get("codigoIbge")
            assert ibge == "3550308", f"Unexpected IBGE code {ibge!r} in municipio-filtered result"

    def test_publicacao_fields(self):
        item = self.data["data"][0]
        assert "numeroControlePNCP" in item
        assert "orgaoEntidade" in item
        assert "dataPublicacaoPncp" in item


# ---------------------------------------------------------------------------
# VCR-replay tests: replay cassettes via the VCR context manager, verifying
# the full request/response round-trip without hitting the network.
# ---------------------------------------------------------------------------

_PNCP_VCR = _vcr_module.VCR(
    record_mode="none",
    match_on=["uri", "method"],
    decode_compressed_response=True,
)


def test_contratos_vcr_replay():
    """Replay the contratos cassette through VCR and validate the response."""
    cassette_path = str(CASSETTE_DIR / "contratos_page1.yaml")
    with _PNCP_VCR.use_cassette(cassette_path):
        resp = requests.get(
            "https://pncp.gov.br/api/consulta/v1/contratos",
            params={
                "dataInicial": "20250101",
                "dataFinal": "20250103",
                "pagina": "1",
                "tamanhoPagina": "10",
            },
            headers={
                "Accept": "application/json",
                "User-Agent": "Baliza/1.0 (cassette-recorder)",
            },
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    assert "totalRegistros" in data
    assert data["totalRegistros"] > 0


def test_publicacao_pregao_vcr_replay():
    """Replay the publicacao/pregão cassette through VCR without network."""
    cassette_path = str(CASSETTE_DIR / "publicacao_pregao_page1.yaml")
    with _PNCP_VCR.use_cassette(cassette_path):
        resp = requests.get(
            "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
            params={
                "dataInicial": "20250101",
                "dataFinal": "20250103",
                "codigoModalidadeContratacao": "6",
                "pagina": "1",
                "tamanhoPagina": "10",
            },
            headers={
                "Accept": "application/json",
                "User-Agent": "Baliza/1.0 (cassette-recorder)",
            },
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["totalRegistros"] > 0
