from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from baliza.engine import BalizaEngine
from baliza.ia_uploader import ManifestReadError

# =============================================================================
# Test Tier Configuration
# =============================================================================
# Tiers are defined in pyproject.toml:
#   tier0: Critical Path - Without these, tool is useless
#   tier1: Core Features - Essential for production use
#   tier2: Operator Experience - Quality-of-life improvements
#   tier3: Future Enhancements - Aspirational features


def pytest_collection_modifyitems(config, items):
    """Auto-apply tier markers based on feature file tags or location."""
    for item in items:
        if hasattr(item, "callspec") and "scenario" in item.callspec.params:
            scenario = item.callspec.params["scenario"]
            if hasattr(scenario, "tags"):
                for tag in scenario.tags:
                    if tag.startswith("tier"):
                        item.add_marker(getattr(pytest.mark, tag))

        test_path = str(item.fspath)
        if "/tier0/" in test_path:
            item.add_marker(pytest.mark.tier0)
        elif "/tier1/" in test_path:
            item.add_marker(pytest.mark.tier1)
        elif "/tier2/" in test_path:
            item.add_marker(pytest.mark.tier2)
        elif "/tier3/" in test_path:
            item.add_marker(pytest.mark.tier3)

        tier_markers = [m for m in item.iter_markers() if m.name.startswith("tier")]
        if not tier_markers:
            item.add_marker(pytest.mark.tier1)


# =============================================================================
# VCR Configuration for Integration Tests
# =============================================================================


@pytest.fixture(scope="module")
def vcr_config():
    """VCR config: replay only, no recording in CI."""
    return {
        "record_mode": "none",
        "match_on": ["uri", "method"],
        "filter_headers": [
            ("authorization", "REDACTED"),
            ("cookie", "REDACTED"),
        ],
        "cassette_library_dir": str(Path(__file__).parent / "cassettes"),
        "path_transformer": lambda path: path + ".yaml",
        "decode_compressed_response": True,
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    return Path(__file__).parent / "cassettes"


@pytest.fixture
def pncp_cassette_dir() -> Path:
    return Path(__file__).parent / "cassettes" / "pncp"


# =============================================================================
# Shared fixtures for BDD step definitions
# =============================================================================

# DDL that mirrors the ~55-column snake_case schema DailyExporter reads from.
# Kept together in one constant so both `baliza_raw` (exporter) and any future
# test that wants to seed raw contract rows use the same shape.
_CONTRATOS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS baliza_raw.contratos (
    numero_controle_pncp VARCHAR PRIMARY KEY,
    numero_controle_pncp_compra VARCHAR,
    ano_contrato INTEGER,
    sequencial_contrato INTEGER,
    ano_compra INTEGER,
    sequencial_compra INTEGER,
    numero_contrato_empenho VARCHAR,
    numero_retificacao INTEGER,
    processo VARCHAR,
    cnpj_orgao VARCHAR,
    razao_social_orgao VARCHAR,
    poder_id VARCHAR,
    esfera_id VARCHAR,
    codigo_unidade VARCHAR,
    nome_unidade VARCHAR,
    uf_sigla VARCHAR,
    uf_nome VARCHAR,
    municipio_nome VARCHAR,
    codigo_ibge VARCHAR,
    cnpj_orgao_subrogado VARCHAR,
    razao_social_orgao_subrogado VARCHAR,
    codigo_unidade_subrogada VARCHAR,
    nome_unidade_subrogada VARCHAR,
    uf_sigla_subrogada VARCHAR,
    ni_fornecedor VARCHAR,
    tipo_pessoa VARCHAR,
    nome_razao_social_fornecedor VARCHAR,
    codigo_pais_fornecedor VARCHAR,
    ni_fornecedor_subcontratado VARCHAR,
    nome_fornecedor_subcontratado VARCHAR,
    tipo_pessoa_subcontratada VARCHAR,
    modalidade_id INTEGER,
    modalidade_nome VARCHAR,
    tipo_contrato_id INTEGER,
    tipo_contrato_nome VARCHAR,
    categoria_processo_id INTEGER,
    categoria_processo_nome VARCHAR,
    receita BOOLEAN,
    valor_inicial DOUBLE,
    valor_parcela DOUBLE,
    valor_global DOUBLE,
    valor_acumulado DOUBLE,
    numero_parcelas INTEGER,
    data_publicacao TIMESTAMP,
    data_publicacao_pncp TIMESTAMP,
    data_assinatura DATE,
    data_vigencia_inicio DATE,
    data_vigencia_fim DATE,
    data_inclusao TIMESTAMP,
    data_atualizacao TIMESTAMP,
    data_atualizacao_global TIMESTAMP,
    objeto_contrato VARCHAR,
    informacao_complementar VARCHAR,
    link_sistema_origem VARCHAR,
    identificador_cipi VARCHAR,
    url_cipi VARCHAR,
    usuario_nome VARCHAR
)
"""


@pytest.fixture
def baliza_engine(tmp_path: Path) -> Iterator[BalizaEngine]:
    """Fresh BalizaEngine on a temp DuckDB file, auto-closed."""
    db_file = tmp_path / "test.duckdb"
    engine = BalizaEngine(db_path=db_file)
    try:
        yield engine
    finally:
        try:
            engine.con.disconnect()
        except Exception:
            pass


@pytest.fixture
def make_db_with_contracts(tmp_path: Path) -> Callable[..., dict[str, Any]]:
    """Factory that creates a DuckDB file with baliza_raw.contratos pre-seeded.

    The DDL matches DailyExporter's SELECT columns so exports succeed without
    any migration layer. Rows use evenly-distributed CNPJ values so callers
    can control unique-org counts via the `count` parameter.
    """

    def _factory(date_str: str, count: int = 50, org_count: int | None = None) -> dict[str, Any]:
        db_file = tmp_path / "test.duckdb"
        output_dir = tmp_path / "daily"
        # Default heuristic: ~5 contracts per org. Callers can override for
        # dedup tests that need exact org cardinality.
        unique_orgs = org_count if org_count is not None else max(1, count // 5)

        with duckdb.connect(str(db_file)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS baliza_raw")
            con.execute(_CONTRATOS_TABLE_DDL)
            for i in range(count):
                org_no = i % unique_orgs
                cnpj = f"{org_no:014d}"
                con.execute(
                    """
                    INSERT OR IGNORE INTO baliza_raw.contratos (
                        numero_controle_pncp,
                        data_publicacao,
                        cnpj_orgao,
                        razao_social_orgao,
                        poder_id,
                        esfera_id,
                        codigo_unidade,
                        nome_unidade,
                        uf_sigla,
                        uf_nome,
                        municipio_nome,
                        codigo_ibge,
                        ni_fornecedor,
                        tipo_pessoa,
                        nome_razao_social_fornecedor,
                        codigo_pais_fornecedor,
                        modalidade_id,
                        modalidade_nome,
                        valor_inicial,
                        objeto_contrato,
                        processo,
                        usuario_nome
                    ) VALUES (
                        ?, CAST(?||'T10:00:00' AS TIMESTAMP), ?, ?, 'E', 'F',
                        '001', 'Unit 1', 'SP', 'São Paulo', 'São Paulo', '3550308',
                        '12000000000190', 'PJ', 'Fornecedor X', 'BR',
                        1, 'Pregão', ?, 'Test contract', 'PROC-001', 'Test User'
                    )
                    """,
                    [f"CTRL-{i:05d}", date_str, cnpj, f"Org {org_no}", 1000.00 + float(i)],
                )

        return {
            "db_path": db_file,
            "output_dir": output_dir,
            "date_str": date_str,
            "org_count": unique_orgs,
        }

    return _factory


@pytest.fixture
def mock_ia_manifest(monkeypatch) -> Callable[[list[dict[str, Any]] | None], None]:
    """Patch the IA manifest readers so no HTTP is attempted.

    Passing None causes both readers to raise ManifestReadError (simulates a
    transient IA outage). The `try_` variant swallows the error and returns
    [] per its contract; the strict variant propagates — matching prod.
    """

    def _install(rows: list[dict[str, Any]] | None) -> None:
        if rows is None:

            def _raise() -> list[dict[str, Any]]:
                raise ManifestReadError("simulated manifest read failure")

            monkeypatch.setattr("baliza.ia_uploader.read_manifest_from_ia", _raise)

            def _try_empty() -> list[dict[str, Any]]:
                return []

            monkeypatch.setattr("baliza.ia_uploader.try_read_manifest_from_ia", _try_empty)
            return

        def _reader() -> list[dict[str, Any]]:
            return list(rows)

        monkeypatch.setattr("baliza.ia_uploader.read_manifest_from_ia", _reader)
        monkeypatch.setattr("baliza.ia_uploader.try_read_manifest_from_ia", _reader)

    return _install


@pytest.fixture
def mock_ia_upload(monkeypatch) -> list[dict[str, Any]]:
    """Patch internetarchive.upload to record calls without hitting the network."""
    calls: list[dict[str, Any]] = []

    def _fake_upload(item_id, files=None, metadata=None, **kwargs):
        calls.append(
            {
                "item_id": item_id,
                "files": dict(files) if files else {},
                "metadata": dict(metadata) if metadata else {},
                "kwargs": kwargs,
            }
        )
        return [MagicMock(status_code=200)]

    monkeypatch.setattr("internetarchive.upload", _fake_upload)
    monkeypatch.setattr("baliza.ia_uploader.ia.upload", _fake_upload)
    monkeypatch.setattr("baliza.consolidator.ia.upload", _fake_upload)
    return calls
