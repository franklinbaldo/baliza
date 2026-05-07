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


def _flatten_publicacao(dumped: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Pydantic-dumped RecuperarCompraPublicacaoDTO into the
    snake_case scalar shape MonthlyExporter / consolidator expect.

    Mirrors ``_flatten_contrato`` for the publicacoes endpoint
    (``/v1/contratacoes/publicacao``). The wire schema has the same
    nested orgaoEntidade / unidadeOrgao / amparoLegal / sub-orgao shape
    contratos uses, plus publication-specific fields (modalidade, modo
    de disputa, valor estimado, situacao). UF + município live on
    unidadeOrgao like contratos, so the canonical row carries
    ``uf_sigla`` and ``partition_by_uf`` is safe.
    """
    orgao = dumped.get("orgaoEntidade") or {}
    unidade = dumped.get("unidadeOrgao") or {}
    orgao_sub = dumped.get("orgaoSubRogado") or {}
    unidade_sub = dumped.get("unidadeSubRogada") or {}
    amparo = dumped.get("amparoLegal") or {}

    return {
        # Identity
        "numero_controle_pncp": dumped.get("numeroControlePNCP"),
        "ano_compra": dumped.get("anoCompra"),
        "sequencial_compra": dumped.get("sequencialCompra"),
        "numero_compra": dumped.get("numeroCompra"),
        "processo": dumped.get("processo"),
        # Órgão contratante
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
        # Órgão sub-rogado
        "cnpj_orgao_subrogado": orgao_sub.get("cnpj"),
        "razao_social_orgao_subrogado": orgao_sub.get("razaoSocial"),
        "codigo_unidade_subrogada": unidade_sub.get("codigoUnidade"),
        "nome_unidade_subrogada": unidade_sub.get("nomeUnidade"),
        "uf_sigla_subrogada": unidade_sub.get("ufSigla"),
        # Modalidade / modo de disputa / instrumento convocatório
        "modalidade_id": dumped.get("modalidadeId"),
        "modalidade_nome": dumped.get("modalidadeNome"),
        "modo_disputa_id": dumped.get("modoDisputaId"),
        "modo_disputa_nome": dumped.get("modoDisputaNome"),
        "tipo_instrumento_convocatorio_codigo": dumped.get("tipoInstrumentoConvocatorioCodigo"),
        "tipo_instrumento_convocatorio_nome": dumped.get("tipoInstrumentoConvocatorioNome"),
        # Amparo legal
        "amparo_legal_codigo": amparo.get("codigo"),
        "amparo_legal_nome": amparo.get("nome"),
        "amparo_legal_descricao": amparo.get("descricao"),
        # Financial
        "valor_total_estimado": dumped.get("valorTotalEstimado"),
        "valor_total_homologado": dumped.get("valorTotalHomologado"),
        # SRP / situação
        "srp": dumped.get("srp"),
        # situacaoCompraId is now an IntEnum (post-OpenAPI fix); store
        # the int value so downstream SQL can compare against literals.
        "situacao_compra_id": _enum_value(dumped.get("situacaoCompraId")),
        "situacao_compra_nome": dumped.get("situacaoCompraNome"),
        # Dates
        "data_publicacao_pncp": dumped.get("dataPublicacaoPncp"),
        "data_inclusao": dumped.get("dataInclusao"),
        "data_atualizacao": dumped.get("dataAtualizacao"),
        "data_atualizacao_global": dumped.get("dataAtualizacaoGlobal"),
        "data_abertura_proposta": dumped.get("dataAberturaProposta"),
        "data_encerramento_proposta": dumped.get("dataEncerramentoProposta"),
        # Text / links
        "objeto_compra": dumped.get("objetoCompra"),
        "informacao_complementar": dumped.get("informacaoComplementar"),
        "justificativa_presencial": dumped.get("justificativaPresencial"),
        "link_sistema_origem": dumped.get("linkSistemaOrigem"),
        "link_processo_eletronico": dumped.get("linkProcessoEletronico"),
        # Audit
        "usuario_nome": dumped.get("usuarioNome"),
    }


def _enum_value(v: Any) -> Any:
    """Return ``v.value`` for IntEnum / StrEnum members, else v unchanged.

    Pydantic preserves enum members through model_dump() unless
    ``mode="json"`` is used; the flatten functions receive plain dicts
    *or* pydantic-dumped dicts depending on the call site, so coerce
    here for either.
    """
    return getattr(v, "value", v)


def _flatten_ata(dumped: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Pydantic-dumped RecuperarAtaDTO into a snake_case scalar shape.

    The atas endpoint's payload is already flat (no nested orgaoEntidade /
    unidadeOrgao structs unlike contratos), so this is mostly a rename pass.
    Kept as a separate function so the canonical column names live in one
    place and the schema is easy to extend if the PNCP API ever adds nested
    structs (mirrors the contratos pattern).
    """
    return {
        "numero_controle_pncp_ata": dumped.get("numeroControlePNCPAta"),
        "numero_ata_registro_preco": dumped.get("numeroAtaRegistroPreco"),
        "ano_ata": dumped.get("anoAta"),
        "numero_controle_pncp_compra": dumped.get("numeroControlePNCPCompra"),
        "cancelado": dumped.get("cancelado"),
        "data_cancelamento": dumped.get("dataCancelamento"),
        "data_assinatura": dumped.get("dataAssinatura"),
        "vigencia_inicio": dumped.get("vigenciaInicio"),
        "vigencia_fim": dumped.get("vigenciaFim"),
        "data_publicacao_pncp": dumped.get("dataPublicacaoPncp"),
        "data_inclusao": dumped.get("dataInclusao"),
        "data_atualizacao": dumped.get("dataAtualizacao"),
        "data_atualizacao_global": dumped.get("dataAtualizacaoGlobal"),
        "usuario": dumped.get("usuario"),
        "objeto_contratacao": dumped.get("objetoContratacao"),
        "cnpj_orgao": dumped.get("cnpjOrgao"),
        "nome_orgao": dumped.get("nomeOrgao"),
        "cnpj_orgao_subrogado": dumped.get("cnpjOrgaoSubrogado"),
        "nome_orgao_subrogado": dumped.get("nomeOrgaoSubrogado"),
        "codigo_unidade_orgao": dumped.get("codigoUnidadeOrgao"),
        "nome_unidade_orgao": dumped.get("nomeUnidadeOrgao"),
        "codigo_unidade_orgao_subrogado": dumped.get("codigoUnidadeOrgaoSubrogado"),
        "nome_unidade_orgao_subrogado": dumped.get("nomeUnidadeOrgaoSubrogado"),
    }
