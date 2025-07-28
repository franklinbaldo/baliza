# src/baliza/config.py

from typing import List, Dict, Any, Tuple
from .schemas import ModalidadeContratacao

# =============================================================================
# DEFINIÇÃO DECLARATIVA DOS RESOURCES
# =============================================================================
# Esta é a nossa nova "fonte da verdade". Uma lista de tuplas, onde cada
# uma contém todos os parâmetros necessários para gerar a configuração de um resource.
# Formato: (type, name, table_name, path, pk, incremental_cursor, requires_modalidade)
#
# 'type' pode ser 'sync', 'backfill' ou 'specialized'
# 'pk' pode ser uma string ou uma lista de strings
# 'incremental_cursor' e 'requires_modalidade' são opcionais
# =============================================================================

RESOURCE_DEFINITIONS: List[Tuple] = [
    # type,      name,                         table_name,     path,                                  pk,                                                                 incremental_cursor,       requires_modalidade
    (
        "sync",
        "contratacoes_atualizacao",
        "contratacao",
        "/v1/contratacoes/atualizacao",
        "numeroControlePNCP",
        "dataAtualizacaoGlobal",
        True,
    ),
    (
        "sync",
        "contratos_atualizacao",
        "contrato",
        "/v1/contratos/atualizacao",
        "numeroControlePNCP",
        "dataAtualizacaoGlobal",
        False,
    ),
    (
        "sync",
        "atas_atualizacao",
        "ata",
        "/v1/atas/atualizacao",
        "numeroControlePNCPAta",
        "dataAtualizacaoGlobal",
        False,
    ),
    (
        "sync",
        "instrumentos_cobranca",
        "instrumento_cobranca",
        "/v1/instrumentoscobranca/inclusao",
        ["cnpj", "ano", "sequencialContrato", "sequencialInstrumentoCobranca"],
        "dataInclusao",
        False,
    ),
    (
        "backfill",
        "contratacoes_publicacao",
        "contratacao",
        "/v1/contratacoes/publicacao",
        "numeroControlePNCP",
        None,
        True,
    ),
    (
        "backfill",
        "contratos",
        "contrato",
        "/v1/contratos",
        "numeroControlePNCP",
        None,
        False,
    ),
    ("backfill", "atas", "ata", "/v1/atas", "numeroControlePNCPAta", None, False),
]

# =============================================================================
# FUNÇÕES GERADORAS DE CONFIGURAÇÃO
# =============================================================================


def _create_resource_config(
    name: str,
    table_name: str,
    endpoint_path: str,
    primary_key: str | List[str],
    incremental_cursor: str = None,
) -> Dict[str, Any]:
    """
    Gera um dicionário de configuração base para um resource do dlt a partir dos parâmetros.
    """
    resource_config = {
        "name": name,
        "table_name": table_name,
        "primary_key": primary_key,
        "write_disposition": "merge",
        "endpoint": {"path": endpoint_path, "params": {}},
    }

    if incremental_cursor:
        resource_config["incremental"] = {
            "cursor_path": incremental_cursor,
            "initial_value": "2021-01-01T00:00:00Z",
            "lag_days": 1,
        }
        resource_config["endpoint"]["params"].update(
            {
                "dataInicial": "{incremental.start}",
                "dataFinal": "{incremental.end}",
            }
        )
    else:  # Para backfill
        resource_config["endpoint"]["params"].update(
            {
                "dataInicial": "{start_chunk}",
                "dataFinal": "{end_chunk}",
            }
        )

    return resource_config


def get_pncp_resources(resource_type: str) -> List[Dict[str, Any]]:
    """
    Filtra as definições e gera a lista final de configurações de resource,
    expandindo as modalidades quando necessário.
    Esta é a função principal que o pipeline.py irá chamar.
    """
    # 1. Filtra as definições pelo tipo de recurso solicitado
    definitions_to_process = [
        defn for defn in RESOURCE_DEFINITIONS if defn[0] == resource_type
    ]

    # 2. Gera a lista de configurações base a partir das definições
    base_configs = [
        _create_resource_config(
            name=name,
            table_name=table_name,
            endpoint_path=path,
            primary_key=pk,
            incremental_cursor=inc_cursor,
        )
        for _, name, table_name, path, pk, inc_cursor, _ in definitions_to_process
    ]

    # 3. Expande os recursos que requerem modalidade
    final_resources = []
    for i, config in enumerate(base_configs):
        # A informação sobre 'requires_modalidade' está na definição original
        requires_modalidade = definitions_to_process[i][6]

        if requires_modalidade:
            for modalidade in ModalidadeContratacao:
                import copy

                mod_config = copy.deepcopy(config)
                mod_config["name"] = f"{config['name']}_mod{modalidade.value}"
                mod_config["endpoint"]["params"]["codigoModalidadeContratacao"] = (
                    modalidade.value
                )
                final_resources.append(mod_config)
        else:
            final_resources.append(config)

    return final_resources
