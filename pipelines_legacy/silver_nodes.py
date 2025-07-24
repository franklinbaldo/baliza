import ibis
from ibis import _

def create_silver_atas(bronze_pncp_raw: ibis.Table) -> ibis.Table:
    source = bronze_pncp_raw.filter(_.endpoint_category == "atas").mutate(
        rn=ibis.row_number().over(
            group_by="numero_controle_pncp", order_by=ibis.desc("data_atualizacao")
        )
    )
    return source.filter(_.rn == 1).select(
        "response_id",
        "extracted_at",
        "endpoint_name",
        "endpoint_url",
        "data_date",
        "run_id",
        "total_records",
        "total_pages",
        "current_page",
        "numero_controle_pncp",
        "numero_ata",
        "ano_ata",
        "data_assinatura",
        "data_vigencia_inicio",
        "data_vigencia_fim",
        "data_publicacao_pncp",
        "data_atualizacao",
        "ni_fornecedor",
        "nome_razao_social_fornecedor",
        "objeto_ata",
        "informacao_complementar",
        "numero_retificacao",
        "orgao_entidade_json",
        "unidade_orgao_json",
    )

def create_silver_contratacoes(bronze_pncp_raw: ibis.Table) -> ibis.Table:
    return bronze_pncp_raw.select(
        "numeroControlePNCP",
        "anoContratacao",
        "dataPublicacaoPNCP",
        "dataAtualizacaoPNCP",
        "horaAtualizacaoPNCP",
        "sequencialOrgao",
        "cnpjOrgao",
        "siglaOrgao",
        "nomeOrgao",
        "sequencialUnidade",
        "codigoUnidade",
        "siglaUnidade",
        "nomeUnidade",
        "codigoEsfera",
        "nomeEsfera",
        "codigoPoder",
        "nomePoder",
        "codigoMunicipio",
        "nomeMunicipio",
        "uf",
        "amparoLegalId",
        "amparoLegalNome",
        "modalidadeId",
        "modalidadeNome",
        "numeroContratacao",
        "processo",
        "objetoContratacao",
        "codigoSituacaoContratacao",
        "nomeSituacaoContratacao",
        "informacaoComplementar",
        "numeroControlePNCPContrato",
        "justificativa",
        "fundamentacaoLegal",
        "valorTotalEstimado",
        "dataAssinatura",
        "dataVigenciaInicio",
        "dataVigenciaFim",
    )

def create_silver_contratos(bronze_pncp_raw: ibis.Table) -> ibis.Table:
    return bronze_pncp_raw.filter(_.numeroControlePNCPContrato.notnull()).select(
        "numeroControlePNCPContrato",
        "numeroControlePNCP",
        "dataAssinatura",
        "dataVigenciaInicio",
        "dataVigenciaFim",
        "valorTotalEstimado",
    )


def create_silver_dim_organizacoes(
    silver_contratos: ibis.Table, silver_contratacoes: ibis.Table
) -> ibis.Table:
    org_sources = (
        silver_contratos.select(org_json=_.orgao_entidade_json)
        .union(silver_contratacoes.select(org_json=_.orgao_entidade_json))
        .union(silver_contratos.select(org_json=_.orgao_subrogado_json))
        .union(silver_contratacoes.select(org_json=_.orgao_subrogado_json))
    )
    organizations = org_sources.filter(_.org_json.notnull()).select(
        org_cnpj=_.org_json.json_extract("$.cnpj").cast("string"),
        org_razao_social=_.org_json.json_extract("$.razaoSocial").cast("string"),
        org_poder_id=_.org_json.json_extract("$.poderId").cast("string"),
        org_esfera_id=_.org_json.json_extract("$.esferaId").cast("string"),
    ).distinct()
    return organizations.mutate(
        org_key=ibis.literal("org_") + organizations.org_cnpj,
        poder_nome=ibis.case()
        .when("E", "Executivo")
        .when("L", "Legislativo")
        .when("J", "Judiciário")
        .when("M", "Ministério Público")
        .else_("Outros")
        .end(),
        esfera_nome=ibis.case()
        .when("F", "Federal")
        .when("E", "Estadual")
        .when("M", "Municipal")
        .else_("Outros")
        .end(),
        created_at=ibis.now(),
        updated_at=ibis.now(),
    ).order_by("org_cnpj")


def create_silver_dim_unidades_orgao(bronze_pncp_raw: ibis.Table) -> ibis.Table:
    return bronze_pncp_raw.select(
        "sequencialOrgao",
        "cnpjOrgao",
        "siglaOrgao",
        "nomeOrgao",
        "codigoEsfera",
        "nomeEsfera",
        "codigoPoder",
        "nomePoder",
        "uf",
    ).distinct()


def create_silver_documentos(
    silver_contratacoes: ibis.Table,
    silver_atas: ibis.Table,
    silver_contratos: ibis.Table,
) -> ibis.Table:
    contratacoes_docs = silver_contratacoes.select(
        numero_controle_pncp="numeroControlePNCP",
        tipo_referencia="contratacoes",
        data_inclusao_referencia="dataAtualizacao",
        doc_data=ibis.unnest(_.response_json.json_extract("$.documentos")),
    )
    atas_docs = silver_atas.select(
        numero_controle_pncp="numero_controle_pncp",
        tipo_referencia="atas",
        data_inclusao_referencia="data_atualizacao",
        doc_data=ibis.unnest(_.response_json.json_extract("$.documentos")),
    )
    contratos_docs = silver_contratos.select(
        numero_controle_pncp="numeroControlePNCP",
        tipo_referencia="contratos",
        data_inclusao_referencia="dataAtualizacao",
        doc_data=ibis.unnest(_.response_json.json_extract("$.documentos")),
    )
    document_sources = contratacoes_docs.union(atas_docs).union(contratos_docs)
    return document_sources.select(
        "numero_controle_pncp",
        "tipo_referencia",
        "data_inclusao_referencia",
    ).mutate(
        documento_key=ibis.literal("doc_") + document_sources.numero_controle_pncp + ibis.literal("_") + document_sources.doc_data.json_extract("$.id").cast("string"),
        titulo=document_sources.doc_data.json_extract("$.titulo").cast("string"),
        url=document_sources.doc_data.json_extract("$.url").cast("string"),
        data_documento=document_sources.doc_data.json_extract("$.data").cast("timestamp"),
        data_inclusao=document_sources.doc_data.json_extract("$.dataInclusao").cast("timestamp"),
        data_atualizacao=document_sources.doc_data.json_extract("$.dataAtualizacao").cast("timestamp"),
        tipo_documento_nome=document_sources.doc_data.json_extract("$.tipoDocumentoNome").cast("string"),
    )


def create_silver_fact_contratacoes(
    silver_contratacoes: ibis.Table,
    silver_dim_organizacoes: ibis.Table,
    silver_dim_unidades_orgao: ibis.Table,
) -> ibis.Table:
    return silver_contratacoes.left_join(
        silver_dim_organizacoes,
        silver_contratacoes.cnpjOrgao == silver_dim_organizacoes.cnpj,
    ).left_join(
        silver_dim_unidades_orgao,
        (silver_contratacoes.codigoUnidade == silver_dim_unidades_orgao.codigoUnidade)
        & (silver_contratacoes.cnpjOrgao == silver_dim_unidades_orgao.cnpjOrgao),
    ).select(
        "numeroControlePNCP",
        "dataPublicacaoPNCP",
        "valorTotalEstimado",
        "objetoContratacao",
        "modalidadeNome",
        "nomeSituacaoContratacao",
        "org_key",
        "dataAtualizacaoPNCP",
    ).mutate(
        procurement_key=ibis.literal("proc_") + silver_contratacoes.numeroControlePNCP,
        unit_key=ibis.literal("unit_") + silver_dim_unidades_orgao.codigoUnidade,
        duracao_proposta_dias=(
            silver_contratacoes.dataVigenciaFim
            - silver_contratacoes.dataVigenciaInicio
        ).days(),
        faixa_valor_estimado=ibis.case()
        .when(silver_contratacoes.valorTotalEstimado <= 10000, "Até 10k")
        .when(silver_contratacoes.valorTotalEstimado <= 50000, "10k-50k")
        .when(silver_contratacoes.valorTotalEstimado <= 100000, "50k-100k")
        .else_("Acima de 100k")
        .end(),
    )


def create_silver_fact_contratos(
    silver_contratos: ibis.Table,
    silver_dim_organizacoes: ibis.Table,
    silver_dim_unidades_orgao: ibis.Table,
) -> ibis.Table:
    return silver_contratos.left_join(
        silver_dim_organizacoes,
        silver_contratos.cnpjOrgao == silver_dim_organizacoes.cnpj,
    ).left_join(
        silver_dim_unidades_orgao,
        (silver_contratos.codigoUnidade == silver_dim_unidades_orgao.codigoUnidade)
        & (silver_contratos.cnpjOrgao == silver_dim_unidades_orgao.cnpjOrgao),
    ).select(
        "numeroControlePNCP",
        "dataAssinatura",
        "dataVigenciaInicio",
        "dataVigenciaFim",
        "valorInicial",
        "niFornecedor",
        "nomeRazaoSocialFornecedor",
        "objetoContrato",
        "org_key",
        "dataAtualizacao",
    ).mutate(
        contract_key=ibis.literal("contract_") + silver_contratos.numeroControlePNCP,
        unit_key=ibis.literal("unit_") + silver_dim_unidades_orgao.codigoUnidade,
        duracao_vigencia_dias=(
            silver_contratos.dataVigenciaFim - silver_contratos.dataVigenciaInicio
        ).days(),
        faixa_valor=ibis.case()
        .when(silver_contratos.valorInicial <= 10000, "Até 10k")
        .when(silver_contratos.valorInicial <= 50000, "10k-50k")
        .when(silver_contratos.valorInicial <= 100000, "50k-100k")
        .else_("Acima de 100k")
        .end(),
        fornecedor_key=ibis.literal("forn_") + silver_contratos.niFornecedor,
    )


def create_silver_itens_contratacao(bronze_pncp_raw: ibis.Table) -> ibis.Table:
    source = bronze_pncp_raw.filter(_.endpoint_category == "contratacoes").mutate(
        rn=ibis.row_number().over(
            group_by="numero_controle_pncp", order_by=ibis.desc("data_atualizacao")
        )
    )
    itens = source.filter(_.rn == 1).select(
        numero_controle_pncp=_.response_json.json_extract("$.numeroControlePNCP").cast("string"),
        item_data=ibis.unnest(_.response_json.json_extract("$.itens")),
    )
    return itens.select(
        "numero_controle_pncp",
    ).mutate(
        item_key=ibis.literal("item_") + itens.numero_controle_pncp + ibis.literal("_") + itens.item_data.json_extract("$.numeroItem").cast("string"),
        numero_item=itens.item_data.json_extract("$.numeroItem").cast("integer"),
        descricao_item=itens.item_data.json_extract("$.descricao").cast("string"),
        quantidade=itens.item_data.json_extract("$.quantidade").cast("double"),
        valor_unitario_estimado=itens.item_data.json_extract("$.valorUnitarioEstimado").cast("double"),
        valor_total=itens.item_data.json_extract("$.valorTotal").cast("double"),
        unidade_medida=itens.item_data.json_extract("$.unidadeMedida").cast("string"),
        tipo_beneficio_nome=itens.item_data.json_extract("$.tipoBeneficioNome").cast("string"),
        situacao_compra_item_nome=itens.item_data.json_extract("$.situacaoCompraItemNome").cast("string"),
        data_inclusao=itens.item_data.json_extract("$.dataInclusao").cast("timestamp"),
        data_atualizacao=itens.item_data.json_extract("$.dataAtualizacao").cast("timestamp"),
    )


def create_silver_orgaos_entidades(bronze_pncp_raw: ibis.Table) -> ibis.Table:
    return bronze_pncp_raw.filter(_.sequencialOrgao.notnull()).select(
        "sequencialOrgao",
        "cnpjOrgao",
        "siglaOrgao",
        "nomeOrgao",
        "codigoEsfera",
        "nomeEsfera",
        "codigoPoder",
        "nomePoder",
        "codigoMunicipio",
        "nomeMunicipio",
        "uf",
    ).distinct()
