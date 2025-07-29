"""
Pipeline principal do Baliza - Versão unificada e simplificada.

Esta versão elimina a duplicação de código e delega 100% do trabalho
pesado para o dlt rest_api_source, como recomendado pela documentação.
"""

from typing import Any, Optional, List, Dict, Generator

import dlt
from pathlib import Path
from dlt.extract.source import DltResource
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
from datetime import datetime

from .enums import ModalidadeContratacao
from .utils.time import date_range_slicer
from .resources import prepare_resources_for_dlt, get_resource_summary
from .models import (
    RecuperarCompraDTO,
    RecuperarContratoDTO,
    PlanoContratacaoComItensDoUsuarioDTO,
    RecuperarCompraPublicacaoDTO,
    ConsultarInstrumentoCobrancaDTO,
    AtaRegistroPrecoPeriodoDTO,
)


# =============================================================================
# CONFIGURAÇÃO E HELPERS
# =============================================================================

# Mapeamento de recursos para modelos Pydantic
RESOURCE_PYDANTIC_MAPPING = {
    # Recursos de contratações
    "contratacoes": RecuperarCompraDTO,
    "contratacoes_publicacao": RecuperarCompraPublicacaoDTO,
    "contratacoes_atualizacao": RecuperarCompraDTO,
    # Recursos de contratos
    "contratos": RecuperarContratoDTO,
    # Recursos de planos de contratação
    "planos_contratacao": PlanoContratacaoComItensDoUsuarioDTO,
    # Recursos de instrumentos de cobrança
    "instrumentos_cobranca": ConsultarInstrumentoCobrancaDTO,
    # Recursos de atas de registro de preço
    "atas_registro_preco": AtaRegistroPrecoPeriodoDTO,
}


# Legacy functions removed - now using Python resources from resources.py


def _get_pydantic_model_for_resource(resource_name: str) -> Optional[Any]:
    """Retorna o modelo Pydantic correspondente ao resource."""
    # Remove sufixo de modalidade se presente
    base_name = (
        resource_name.split("_mod")[0] if "_mod" in resource_name else resource_name
    )
    return RESOURCE_PYDANTIC_MAPPING.get(base_name)


def _create_pydantic_resource(resource_config: Dict[str, Any]) -> Dict[str, Any]:
    """Cria resource simplificado compatível com DLT REST API."""
    import copy

    converted = copy.deepcopy(resource_config)
    resource_name = converted["name"]

    # Manter apenas campos aceitos pelo DLT REST API
    clean_resource = {"name": converted["name"], "endpoint": converted["endpoint"]}

    # Adicionar write_disposition se especificado
    if "write_disposition" in converted:
        clean_resource["write_disposition"] = converted["write_disposition"]

    # Adicionar primary_key se especificado
    if "primary_key" in converted:
        clean_resource["primary_key"] = converted["primary_key"]

    # Log da configuração
    print(f"✅ Configurado resource: {resource_name}")

    return clean_resource


def _get_nested_hints_for_model(model_class: Any) -> Dict[str, Any]:
    """Gera nested hints baseado no modelo Pydantic."""
    nested_hints = {}

    # Mapear campos complexos conhecidos para nested tables
    complex_fields = {
        "orgaoEntidade": {"columns": {"cnpj": {"data_type": "text"}}},
        "unidadeOrgao": {"columns": {"codigoUnidade": {"data_type": "text"}}},
        "amparoLegal": {"columns": {"codigo": {"data_type": "bigint"}}},
        "fontesOrcamentarias": {
            "columns": {
                "codigo": {"data_type": "bigint"},
                "nome": {"data_type": "text"},
                "dataInclusao": {"data_type": "timestamp"},
            }
        },
        "itens": {
            "columns": {
                "numeroItem": {"data_type": "bigint"},
                "valorTotal": {"data_type": "decimal"},
            }
        },
    }

    # Aplicar hints apenas para campos que existem no modelo
    if hasattr(model_class, "__annotations__"):
        for field_name in model_class.__annotations__.keys():
            if field_name in complex_fields:
                nested_hints[field_name] = complex_fields[field_name]

    return nested_hints


def _convert_to_rest_api_format(resource_config: Dict[str, Any]) -> Dict[str, Any]:
    """Converte configuração do YAML para o formato do rest_api_source."""
    import copy

    converted = copy.deepcopy(resource_config)

    # Configuração incremental no formato correto do DLT 1.x
    if "incremental" in converted["endpoint"]:
        incremental_config = converted["endpoint"]["incremental"]

        # Mover configuração incremental para o nível do resource
        converted["incremental"] = dlt.sources.incremental(
            cursor_path=incremental_config["cursor_path"],
            initial_value=incremental_config["initial_value"],
        )

        # Remove incremental do endpoint
        del converted["endpoint"]["incremental"]

    return converted


# =============================================================================
# FONTE PRINCIPAL PARA SINCRONIZAÇÃO CONTÍNUA
# =============================================================================


@dlt.source(name="baliza_source")
def pncp_source(
    resource_type: str = "sync", base_url: Optional[str] = dlt.secrets.value
) -> Generator[DltResource, None, None]:
    """
    Fonte dlt declarativa para a API do PNCP com validação Pydantic.

    Usa configuração Python tipada, aplica modelos Pydantic para validação,
    expande modalidades quando necessário, e delega toda a extração
    para o rest_api_source nativo do dlt.

    Args:
        resource_type: Tipo de resource ('sync', 'backfill', 'specialized')
        base_url: URL base da API PNCP

    Yields:
        DltResource: Resources configurados com schema Pydantic
    """
    # 1. Preparar recursos usando configuração Python (com modalidade expansion)
    final_resources = prepare_resources_for_dlt(resource_type)

    # 3. Criar configuração usando RESTAPIConfig com validação Pydantic
    api_config: RESTAPIConfig = {
        "client": {
            "base_url": base_url or dlt.secrets["sources.baliza_source.base_url"],
            "paginator": {
                "type": "page_number",
                "page_param": "pagina",
                "total_path": "totalPaginas",
                "base_page": 1,
            },
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "max_table_nesting": 2,  # Controlar profundidade de nested tables
        },
        "resources": final_resources,
    }

    # 4. Retornar resources com validação Pydantic aplicada
    yield from rest_api_resources(api_config)


# =============================================================================
# RESOURCE PARA BACKFILL HISTÓRICO PARALELO
# =============================================================================


@dlt.resource(name="pncp_backfill", parallelized=True, write_disposition="merge")
def pncp_backfill_resource(start_date_str: str, end_date_str: str, chunk_days: int = 7):
    """
    Resource de backfill que executa a extração histórica em fatias
    paralelas e de forma resumível usando o estado do dlt.

    Args:
        start_date_str: Data de início no formato 'YYYYMMDD'
        end_date_str: Data de fim no formato 'YYYYMMDD'
        chunk_days: Tamanho de cada fatia em dias
    """

    # Estado para resumibilidade - usando current resource state API
    state = dlt.current.resource_state()
    completed_chunks = state.setdefault("completed_chunks", [])

    # Carregar configuração de backfill usando Python resources
    final_resources = prepare_resources_for_dlt("backfill")

    # Converter datas
    start_dt = datetime.strptime(start_date_str, "%Y%m%d")
    end_dt = datetime.strptime(end_date_str, "%Y%m%d")

    # Processar cada resource
    for resource_config in final_resources:
        # Fatiar o período de tempo
        for start_chunk, end_chunk in date_range_slicer(start_dt, end_dt, chunk_days):
            chunk_id = f"{resource_config['name']}-{start_chunk}-{end_chunk}"

            # Pular fatias já concluídas
            if chunk_id in completed_chunks:
                print(f"⏩ Pulando fatia já concluída: {chunk_id}")
                continue

            # Função para processar o chunk
            def _fetch_chunk(
                config=resource_config, start=start_chunk, end=end_chunk, id=chunk_id
            ):
                print("📦 Processando fatia:", id)

                # Atualizar parâmetros de data
                config_copy = config.copy()
                if "endpoint" in config_copy and "params" in config_copy["endpoint"]:
                    config_copy["endpoint"]["params"]["dataInicial"] = start
                    config_copy["endpoint"]["params"]["dataFinal"] = end

                # Configuração do source para este chunk
                chunk_source_config = {
                    "client": {
                        "base_url": dlt.secrets["sources.baliza_source.base_url"],
                        "paginator": {
                            "type": "page_number",
                            "page_param": "pagina",
                            "total_path": "totalPaginas",
                            "base_page": 1,  # API PNCP começa na página 1
                        },
                    },
                    "resources": [config_copy],
                }

                # Extrair dados do chunk
                yield from rest_api_source(chunk_source_config)

                # Marcar como concluído após sucesso
                dlt.current.resource_state()["completed_chunks"].append(id)
                print("✅ Fatia concluída:", id)

            # Entregar o trabalho para o pool de threads do dlt
            yield _fetch_chunk


# =============================================================================
# FUNÇÕES HELPER PARA EXECUTAR PIPELINES
# =============================================================================


def check_pncp_connection() -> None:
    """Verifica conectividade com a API PNCP."""
    source = pncp_source(resource_type="sync")

    # Obter o primeiro resource disponível para teste
    resources = list(source)
    if not resources:
        raise ConnectionError("Nenhum resource configurado")

    first_resource = resources[0]
    resource_name = first_resource.name

    # Tenta conectar com o primeiro resource
    (can_connect, error_msg) = check_connection(source, resource_name)

    if not can_connect:
        print(f"❌ Erro de conectividade: {error_msg}")
        raise ConnectionError(f"Não foi possível conectar à API PNCP: {error_msg}")
    else:
        print("✅ Conectividade com API PNCP verificada!")


def run_sync_pipeline(
    pipeline_name: str = "baliza_pncp_sync",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
):
    """Executa o pipeline de sincronização contínua com validação Pydantic."""
    # Verificar conectividade primeiro
    check_pncp_connection()

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export",
    )

    source = pncp_source(resource_type="sync")

    print("🚀 Executando pipeline de sincronização com validação Pydantic...")
    info = pipeline.run(source)
    print("✅ Sincronização concluída com data quality validado!")
    print(info)
    return info


def run_backfill_pipeline(
    start_date: str,
    end_date: str,
    chunk_days: int = 7,
    pipeline_name: str = "baliza_pncp_backfill",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
):
    """Executa o pipeline de backfill histórico."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export",
    )

    backfill_resource = pncp_backfill_resource(start_date, end_date, chunk_days)

    print(f"🚀 Executando backfill de {start_date} até {end_date}...")
    info = pipeline.run(backfill_resource)
    print("✅ Backfill concluído!")
    print(info)
    return info


if __name__ == "__main__":
    # Exemplo de uso
    run_sync_pipeline()
