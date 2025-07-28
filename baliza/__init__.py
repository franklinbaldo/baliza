from typing import List

import dlt
from dlt.extract.source import DltResource
from rest_api import rest_api_source
from rest_api.typing import RESTAPIConfig


@dlt.source(name="baliza_source", max_table_nesting=2)
def baliza_source(
    token: str = dlt.secrets.value,
    base_url: str = dlt.config.value,
) -> List[DltResource]:

    # source configuration
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "auth": {
                "type": "bearer",
                "token": token,
            },
        },
        "resources": [
            {
                "name": "atas",
                "table_name": "ata_registro_preco_periodo_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/atas",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "idUsuario": "OPTIONAL_CONFIG",
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "atas_atualizacao",
                "table_name": "ata_registro_preco_periodo_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/atas/atualizacao",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "idUsuario": "OPTIONAL_CONFIG",
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "instrumentoscobranca_inclusao",
                "table_name": "consultar_instrumento_cobranca_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/instrumentoscobranca/inclusao",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "tipoInstrumentoCobranca": "OPTIONAL_CONFIG",
                        # "cnpjOrgao": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "pca_usuario",
                "table_name": "plano_contratacao_com_itens_do_usuario_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/pca/usuario",
                    "params": {
                        "anoPca": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "idUsuario": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "codigoClassificacaoSuperior": "OPTIONAL_CONFIG",
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "pca_atualizacao",
                "table_name": "plano_contratacao_com_itens_do_usuario_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/pca/atualizacao",
                    "params": {
                        "dataInicio": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFim": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "codigoUnidade": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "pca",
                "table_name": "plano_contratacao_com_itens_do_usuario_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/pca/",
                    "params": {
                        "anoPca": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "codigoClassificacaoSuperior": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "orgaos_compras",
                "table_name": "recuperar_compra_dto",
                "endpoint": {
                    "data_selector": "$",
                    "path": "/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}",
                    "params": {
                        "cnpj": "FILL_ME_IN",  # TODO: fill in required path parameter
                        "ano": "FILL_ME_IN",  # TODO: fill in required path parameter
                        "sequencial": "FILL_ME_IN",  # TODO: fill in required path parameter
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "contratacoes_publicacao",
                "table_name": "recuperar_compra_publicacao_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/contratacoes/publicacao",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "codigoModalidadeContratacao": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "codigoModoDisputa": "OPTIONAL_CONFIG",
                        # "uf": "OPTIONAL_CONFIG",
                        # "codigoMunicipioIbge": "OPTIONAL_CONFIG",
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "idUsuario": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "contratacoes_proposta",
                "table_name": "recuperar_compra_publicacao_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/contratacoes/proposta",
                    "params": {
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "codigoModalidadeContratacao": "OPTIONAL_CONFIG",
                        # "uf": "OPTIONAL_CONFIG",
                        # "codigoMunicipioIbge": "OPTIONAL_CONFIG",
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "idUsuario": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "contratacoes_atualizacao",
                "table_name": "recuperar_compra_publicacao_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/contratacoes/atualizacao",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "codigoModalidadeContratacao": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "codigoModoDisputa": "OPTIONAL_CONFIG",
                        # "uf": "OPTIONAL_CONFIG",
                        # "codigoMunicipioIbge": "OPTIONAL_CONFIG",
                        # "cnpj": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "idUsuario": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "contratos",
                "table_name": "recuperar_contrato_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/contratos",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "cnpjOrgao": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "usuarioId": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
            {
                "name": "contratos_atualizacao",
                "table_name": "recuperar_contrato_dto",
                "endpoint": {
                    "data_selector": "data",
                    "path": "/v1/contratos/atualizacao",
                    "params": {
                        "dataInicial": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "dataFinal": "FILL_ME_IN",  # TODO: fill in required query parameter
                        "pagina": "FILL_ME_IN",  # TODO: fill in required query parameter
                        # the parameters below can optionally be configured
                        # "cnpjOrgao": "OPTIONAL_CONFIG",
                        # "codigoUnidadeAdministrativa": "OPTIONAL_CONFIG",
                        # "usuarioId": "OPTIONAL_CONFIG",
                        # "tamanhoPagina": "OPTIONAL_CONFIG",
                    },
                    "paginator": "auto",
                },
            },
        ],
    }

    return rest_api_source(source_config)
