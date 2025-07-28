"""
Pipeline principal do Baliza - Versão unificada e simplificada.

Esta versão elimina a duplicação de código e delega 100% do trabalho
pesado para o dlt rest_api_source, como recomendado pela documentação.
"""

from typing import Any, Optional, List, Dict, Sequence

import dlt
import yaml
from pathlib import Path
from dlt.common.pendulum import pendulum
from dlt.extract.source import DltResource
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
from datetime import datetime

from .schemas import ModalidadeContratacao
from .utils.time import date_range_slicer


# =============================================================================
# CONFIGURAÇÃO E HELPERS
# =============================================================================

def _load_yaml_config() -> Dict[str, Any]:
    """Carrega a configuração do YAML."""
    config_path = Path(__file__).parent.parent.parent / "config" / "pncp_resources.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def _requires_modalidade(resource_name: str) -> bool:
    """Verifica se um resource requer parâmetro de modalidade."""
    endpoints_requiring_modalidade = {
        "contratacoes_publicacao",
        "contratacoes_atualizacao"
    }
    base_name = resource_name.split('_mod')[0] if '_mod' in resource_name else resource_name
    return base_name in endpoints_requiring_modalidade


def _generate_modalidade_resources(base_resource: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Gera 13 resources separados para cada modalidade (1-13)."""
    import copy
    resources = []
    
    for modalidade in ModalidadeContratacao:
        resource = copy.deepcopy(base_resource)
        resource["name"] = f"{base_resource['name']}_mod{modalidade.value}"
        
        if "endpoint" not in resource:
            resource["endpoint"] = {}
        if "params" not in resource["endpoint"]:
            resource["endpoint"]["params"] = {}
            
        resource["endpoint"]["params"]["codigoModalidadeContratacao"] = modalidade.value
        
        resources.append(resource)
    
    return resources


def _get_resources_by_type(config: Dict[str, Any], resource_type: str) -> List[Dict[str, Any]]:
    """Seleciona recursos baseado no tipo: sync, backfill, ou specialized."""
    type_mapping = {
        "sync": "sync_resources",
        "backfill": "backfill_resources", 
        "specialized": "specialized_resources"
    }
    
    key = type_mapping.get(resource_type, "sync_resources")
    return config.get(key, [])


def _prepare_resources_for_dlt(resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepara recursos do YAML para o formato esperado pelo rest_api_source."""
    prepared = []
    
    for resource_config in resources:
        # Se requer modalidade, expande para 13 resources
        if _requires_modalidade(resource_config["name"]):
            prepared.extend(_generate_modalidade_resources(resource_config))
        else:
            prepared.append(_convert_to_rest_api_format(resource_config))
    
    return prepared


def _convert_to_rest_api_format(resource_config: Dict[str, Any]) -> Dict[str, Any]:
    """Converte configuração do YAML para o formato do rest_api_source."""
    import copy
    
    converted = copy.deepcopy(resource_config)
    
    # Configuração incremental já no formato correto do DLT
    if "incremental" in converted and "endpoint" in converted:
        incremental_config = converted["incremental"]
        
        # Configurar incremental diretamente no endpoint
        if "params" in converted["endpoint"]:
            for param_name, param_value in converted["endpoint"]["params"].items():
                if param_value == "{incremental.start_value}":
                    converted["endpoint"]["incremental"] = {
                        "cursor_path": incremental_config["cursor_path"],
                        "initial_value": incremental_config["initial_value"]
                    }
                    converted["endpoint"]["params"][param_name] = "{incremental.start_value}"
                elif param_value == "{incremental.end}":
                    # Para dataFinal, usar data atual
                    today = datetime.now().strftime("%Y%m%d")
                    converted["endpoint"]["params"][param_name] = today
        
        # Remove incremental do nível superior após mover para endpoint
        del converted["incremental"]
    
    return converted


# =============================================================================
# FONTE PRINCIPAL PARA SINCRONIZAÇÃO CONTÍNUA
# =============================================================================

@dlt.source(name="baliza_source")
def pncp_source(
    resource_type: str = "sync",
    base_url: Optional[str] = dlt.secrets.value
) -> Any:
    """
    Fonte dlt declarativa para a API do PNCP.
    
    Lê a configuração do YAML, expande modalidades quando necessário,
    e delega toda a extração para o rest_api_source nativo do dlt.
    
    Args:
        resource_type: Tipo de resource ('sync', 'backfill', 'specialized')
        base_url: URL base da API PNCP
    """
    # 1. Carregar configuração do YAML
    config = _load_yaml_config()
    base_resources = _get_resources_by_type(config, resource_type)
    
    # 2. Preparar recursos (expandir modalidades quando necessário)
    final_resources = _prepare_resources_for_dlt(base_resources)
    
    # 3. Criar configuração usando RESTAPIConfig para type hints
    api_config: RESTAPIConfig = {
        "client": {
            "base_url": base_url or dlt.secrets["sources.baliza_source.base_url"],
            "paginator": {
                "type": "page_number",
                "page_param": "pagina",
                "total_path": "totalPaginas",
                "base_page": 1,
                "maximum_page": 2000
            },
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "tamanhoPagina": 500,
                },
            },
        },
        "resources": final_resources
    }
    
    # 4. Usar rest_api_resources como no boilerplate
    yield from rest_api_resources(api_config)


# =============================================================================
# RESOURCE PARA BACKFILL HISTÓRICO PARALELO
# =============================================================================

@dlt.resource(name="pncp_backfill", parallelized=True, write_disposition="merge")
def pncp_backfill_resource(
    start_date_str: str,
    end_date_str: str,
    chunk_days: int = 7
):
    """
    Resource de backfill que executa a extração histórica em fatias
    paralelas e de forma resumível usando o estado do dlt.

    Args:
        start_date_str: Data de início no formato 'YYYYMMDD'
        end_date_str: Data de fim no formato 'YYYYMMDD' 
        chunk_days: Tamanho de cada fatia em dias
    """
    from dlt.sources.rest_api import rest_api_source
    
    # Estado para resumibilidade
    state = dlt.state()
    completed_chunks = state.setdefault("completed_chunks", {}).setdefault("pncp_backfill", [])
    
    # Carregar configuração de backfill
    config = _load_yaml_config()
    backfill_resources = _get_resources_by_type(config, "backfill")
    final_resources = _prepare_resources_for_dlt(backfill_resources)
    
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
                config=resource_config,
                start=start_chunk,
                end=end_chunk,
                id=chunk_id
            ):
                print("📦 Processando fatia:", id)
                
                # Atualizar parâmetros de data
                config_copy = config.copy()
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
                            "base_page": 1  # API PNCP começa na página 1
                        }
                    },
                    "resources": [config_copy]
                }

                # Extrair dados do chunk
                yield from rest_api_source(chunk_source_config)
                
                # Marcar como concluído após sucesso
                dlt.state()["completed_chunks"]["pncp_backfill"].append(id)
                print("✅ Fatia concluída:", id)

            # Entregar o trabalho para o pool de threads do dlt
            yield _fetch_chunk


# =============================================================================
# FUNÇÕES HELPER PARA EXECUTAR PIPELINES
# =============================================================================

def check_pncp_connection() -> None:
    """Verifica conectividade com a API PNCP."""
    source = pncp_source(resource_type="sync")
    
    # Tenta conectar com um endpoint básico
    (can_connect, error_msg) = check_connection(
        source,
        "contratacoes"  # endpoint básico para teste
    )
    
    if not can_connect:
        print(f"❌ Erro de conectividade: {error_msg}")
        raise ConnectionError(f"Não foi possível conectar à API PNCP: {error_msg}")
    else:
        print("✅ Conectividade com API PNCP verificada!")


def run_sync_pipeline(
    pipeline_name: str = "baliza_pncp_sync",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data"
):
    """Executa o pipeline de sincronização contínua."""
    # Verificar conectividade primeiro
    check_pncp_connection()
    
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export"
    )
    
    source = pncp_source(resource_type="sync")
    
    print("🚀 Executando pipeline de sincronização...")
    info = pipeline.run(source)
    print("✅ Sincronização concluída!")
    print(info)
    return info


def run_backfill_pipeline(
    start_date: str,
    end_date: str,
    chunk_days: int = 7,
    pipeline_name: str = "baliza_pncp_backfill",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data"
):
    """Executa o pipeline de backfill histórico."""
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export"
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