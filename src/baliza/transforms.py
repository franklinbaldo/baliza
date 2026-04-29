from typing import Any


def _flatten_contrato(dumped: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Pydantic-dumped RecuperarContratoDTO into the snake_case
    scalar shape that MonthlyExporter, DailyExporter, and consolidator all
    expect.

    The PNCP API (and therefore our DTOs) use camelCase + nested structs for
    orgaoEntidade / unidadeOrgao / tipoContrato / categoriaProcesso. Downstream
    SQL joins on scalar columns like cnpj_orgao, data_publicacao,
    numero_controle_pncp — so we collapse those structs here rather than
    forcing every reader to learn the wire schema.
    """
    orgao = dumped.get("orgaoEntidade") or {}
    unidade = dumped.get("unidadeOrgao") or {}
    orgao_sub = dumped.get("orgaoSubRogado") or {}
    unidade_sub = dumped.get("unidadeSubRogada") or {}
    tipo_contrato = dumped.get("tipoContrato") or {}
    categoria = dumped.get("categoriaProcesso") or {}
    # dataPublicacao is not emitted by the contratos endpoint — surface
    # dataPublicacaoPncp under both names so WHERE/ORDER BY clauses that
    # target the canonical `data_publicacao` don't end up NULL-filtered.
    data_publicacao = dumped.get("dataPublicacaoPncp")

    return {
        "numero_controle_pncp": dumped.get("numeroControlePNCP"),
        "numero_controle_pncp_compra": dumped.get("numeroControlePncpCompra"),
        "ano_contrato": dumped.get("anoContrato"),
        "sequencial_contrato": dumped.get("sequencialContrato"),
        "numero_contrato_empenho": dumped.get("numeroContratoEmpenho"),
        "numero_retificacao": dumped.get("numeroRetificacao"),
        "processo": dumped.get("processo"),
        "cnpj_orgao": orgao.get("cnpj"),
        "razao_social_orgao": orgao.get("razaoSocial"),
        "poder_id": orgao.get("poderId"),
        "esfera_id": orgao.get("esferaId"),
        "codigo_unidade": unidade.get("codigoUnidade"),
        "nome_unidade": unidade.get("nomeUnidade"),
        "uf_sigla": unidade.get("ufSigla"),
        "uf_nome": unidade.get("ufNome"),
        "municipio_nome": unidade.get("municipioNome"),
        "codigo_ibge": unidade.get("codigoIbge"),
        "cnpj_orgao_subrogado": orgao_sub.get("cnpj"),
        "razao_social_orgao_subrogado": orgao_sub.get("razaoSocial"),
        "codigo_unidade_subrogada": unidade_sub.get("codigoUnidade"),
        "nome_unidade_subrogada": unidade_sub.get("nomeUnidade"),
        "uf_sigla_subrogada": unidade_sub.get("ufSigla"),
        "ni_fornecedor": dumped.get("niFornecedor"),
        "tipo_pessoa": dumped.get("tipoPessoa"),
        "nome_razao_social_fornecedor": dumped.get("nomeRazaoSocialFornecedor"),
        "codigo_pais_fornecedor": dumped.get("codigoPaisFornecedor"),
        "ni_fornecedor_subcontratado": dumped.get("niFornecedorSubContratado"),
        "nome_fornecedor_subcontratado": dumped.get("nomeFornecedorSubContratado"),
        "tipo_pessoa_subcontratada": dumped.get("tipoPessoaSubContratada"),
        "tipo_contrato_id": tipo_contrato.get("id"),
        "tipo_contrato_nome": tipo_contrato.get("nome"),
        "categoria_processo_id": categoria.get("id"),
        "categoria_processo_nome": categoria.get("nome"),
        "receita": dumped.get("receita"),
        "valor_inicial": dumped.get("valorInicial"),
        "valor_parcela": dumped.get("valorParcela"),
        "valor_global": dumped.get("valorGlobal"),
        "valor_acumulado": dumped.get("valorAcumulado"),
        "numero_parcelas": dumped.get("numeroParcelas"),
        "data_publicacao": data_publicacao,
        "data_publicacao_pncp": data_publicacao,
        "data_assinatura": dumped.get("dataAssinatura"),
        "data_vigencia_inicio": dumped.get("dataVigenciaInicio"),
        "data_vigencia_fim": dumped.get("dataVigenciaFim"),
        "data_atualizacao": dumped.get("dataAtualizacao"),
        "data_atualizacao_global": dumped.get("dataAtualizacaoGlobal"),
        "objeto_contrato": dumped.get("objetoContrato"),
        "informacao_complementar": dumped.get("informacaoComplementar"),
        "identificador_cipi": dumped.get("identificadorCipi"),
        "url_cipi": dumped.get("urlCipi"),
        "usuario_nome": dumped.get("usuarioNome"),
    }
