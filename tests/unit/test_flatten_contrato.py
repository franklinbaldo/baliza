"""Unit tests for _flatten_contrato.

Pinning the camelCase → snake_case + struct-flattening contract keeps
future API field additions from silently dropping data in the monthly
parquet pipeline (see the prod bug fixed in PR #364).
"""

from __future__ import annotations

from baliza.extractor import _flatten_contrato
from baliza.models import RecuperarContratoDTO


def _full_wire_payload() -> dict:
    """Mirrors the PNCP JSON shape the extractor dumps via Pydantic."""
    return {
        "numeroControlePNCP": "23000-1-000001/2023",
        "numeroControlePncpCompra": "23000-1-COMPRA/2023",
        "anoContrato": 2023,
        "sequencialContrato": 1,
        "numeroContratoEmpenho": "EMP-42",
        "numeroRetificacao": 0,
        "processo": "PROC-001",
        "orgaoEntidade": {
            "cnpj": "00000000000001",
            "razaoSocial": "Ministério da Fazenda",
            "poderId": "E",
            "esferaId": "F",
        },
        "unidadeOrgao": {
            "codigoUnidade": "UNI-01",
            "nomeUnidade": "Unidade Central",
            "ufSigla": "DF",
            "ufNome": "Distrito Federal",
            "municipioNome": "Brasília",
            "codigoIbge": "5300108",
        },
        "orgaoSubRogado": {
            "cnpj": "00000000000002",
            "razaoSocial": "Órgão Sub-rogado",
        },
        "unidadeSubRogada": {
            "codigoUnidade": "UNI-SUB",
            "nomeUnidade": "Unidade Sub-rogada",
            "ufSigla": "SP",
        },
        "niFornecedor": "11111111111111",
        "tipoPessoa": "PJ",
        "nomeRazaoSocialFornecedor": "Fornecedor X Ltda",
        "codigoPaisFornecedor": "BR",
        "niFornecedorSubContratado": "22222222222222",
        "nomeFornecedorSubContratado": "Subcontratado Y",
        "tipoPessoaSubContratada": "PJ",
        "tipoContrato": {"id": 1, "nome": "Empenho"},
        "categoriaProcesso": {"id": 5, "nome": "Compras"},
        "receita": False,
        "valorInicial": 1500.50,
        "valorParcela": 500.00,
        "valorGlobal": 1500.50,
        "valorAcumulado": 1500.50,
        "numeroParcelas": 3,
        "dataPublicacaoPncp": "2023-01-15T10:00:00+00:00",
        "dataAssinatura": "2023-01-10",
        "dataVigenciaInicio": "2023-01-15",
        "dataVigenciaFim": "2023-12-31",
        "dataAtualizacao": "2023-06-01T12:00:00+00:00",
        "dataAtualizacaoGlobal": "2023-06-01T12:00:00+00:00",
        "objetoContrato": "Aquisição de serviços",
        "informacaoComplementar": "Observação",
        "identificadorCipi": "CIPI-01",
        "urlCipi": "https://cipi.example/CIPI-01",
        "usuarioNome": "operador",
    }


def test_flatten_maps_every_scalar_field():
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["numero_controle_pncp"] == "23000-1-000001/2023"
    assert flat["numero_controle_pncp_compra"] == "23000-1-COMPRA/2023"
    assert flat["ano_contrato"] == 2023
    assert flat["sequencial_contrato"] == 1
    assert flat["numero_contrato_empenho"] == "EMP-42"
    assert flat["numero_retificacao"] == 0
    assert flat["processo"] == "PROC-001"
    assert flat["ni_fornecedor"] == "11111111111111"
    assert flat["tipo_pessoa"] == "PJ"
    assert flat["nome_razao_social_fornecedor"] == "Fornecedor X Ltda"
    assert flat["codigo_pais_fornecedor"] == "BR"
    assert flat["ni_fornecedor_subcontratado"] == "22222222222222"
    assert flat["nome_fornecedor_subcontratado"] == "Subcontratado Y"
    assert flat["tipo_pessoa_subcontratada"] == "PJ"
    assert flat["receita"] is False
    assert flat["valor_inicial"] == 1500.50
    assert flat["valor_parcela"] == 500.00
    assert flat["valor_global"] == 1500.50
    assert flat["valor_acumulado"] == 1500.50
    assert flat["numero_parcelas"] == 3
    assert flat["objeto_contrato"] == "Aquisição de serviços"
    assert flat["informacao_complementar"] == "Observação"
    assert flat["identificador_cipi"] == "CIPI-01"
    assert flat["url_cipi"] == "https://cipi.example/CIPI-01"
    assert flat["usuario_nome"] == "operador"


def test_flatten_expands_orgao_entidade_struct():
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["cnpj_orgao"] == "00000000000001"
    assert flat["razao_social_orgao"] == "Ministério da Fazenda"
    assert flat["poder_id"] == "E"
    assert flat["esfera_id"] == "F"
    assert "orgaoEntidade" not in flat


def test_flatten_expands_unidade_orgao_struct():
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["codigo_unidade"] == "UNI-01"
    assert flat["nome_unidade"] == "Unidade Central"
    assert flat["uf_sigla"] == "DF"
    assert flat["uf_nome"] == "Distrito Federal"
    assert flat["municipio_nome"] == "Brasília"
    assert flat["codigo_ibge"] == "5300108"
    assert "unidadeOrgao" not in flat


def test_flatten_expands_subrogado_structs():
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["cnpj_orgao_subrogado"] == "00000000000002"
    assert flat["razao_social_orgao_subrogado"] == "Órgão Sub-rogado"
    assert flat["codigo_unidade_subrogada"] == "UNI-SUB"
    assert flat["nome_unidade_subrogada"] == "Unidade Sub-rogada"
    assert flat["uf_sigla_subrogada"] == "SP"


def test_flatten_expands_tipo_contrato_and_categoria():
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["tipo_contrato_id"] == 1
    assert flat["tipo_contrato_nome"] == "Empenho"
    assert flat["categoria_processo_id"] == 5
    assert flat["categoria_processo_nome"] == "Compras"


def test_flatten_surfaces_data_publicacao_under_both_names():
    # dataPublicacaoPncp must appear as data_publicacao AND data_publicacao_pncp
    # so downstream WHERE/ORDER BY on the canonical column isn't NULL-filtered.
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["data_publicacao"] is not None
    assert flat["data_publicacao"] == flat["data_publicacao_pncp"]


def test_flatten_tolerates_missing_nested_structs():
    # Real PNCP responses often omit orgao_sub_rogado / unidade_sub_rogada /
    # tipoContrato / categoriaProcesso — the flattener must yield None for
    # their derived columns rather than raising AttributeError.
    dumped = RecuperarContratoDTO.model_validate(
        {
            "numeroControlePNCP": "MIN-001",
            "orgaoEntidade": {"cnpj": "1"},
        }
    ).model_dump()
    flat = _flatten_contrato(dumped)

    assert flat["numero_controle_pncp"] == "MIN-001"
    assert flat["cnpj_orgao"] == "1"
    assert flat["cnpj_orgao_subrogado"] is None
    assert flat["codigo_unidade_subrogada"] is None
    assert flat["tipo_contrato_id"] is None
    assert flat["categoria_processo_nome"] is None
    assert flat["codigo_unidade"] is None


def test_flatten_produces_only_snake_case_keys():
    dumped = RecuperarContratoDTO.model_validate(_full_wire_payload()).model_dump()
    flat = _flatten_contrato(dumped)

    for key in flat:
        assert key == key.lower(), f"non-lowercase key: {key}"
        assert " " not in key, f"whitespace in key: {key}"
        # No camelCase survivors
        assert not any(c.isupper() for c in key), f"camelCase leaked: {key}"
