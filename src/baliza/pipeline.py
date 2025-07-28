from typing import List, Dict, Any, Optional, Generator
from datetime import datetime, timedelta
from pathlib import Path

import dlt
import yaml
from dlt.extract.source import DltResource
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.extract.incremental import Incremental
from rest_api import rest_api_source
from rest_api.typing import RESTAPIConfig

from .schemas import ModalidadeContratacao


# Endpoints que REQUEREM parâmetro modalidade (baseado na análise dos scripts)
ENDPOINTS_REQUIRING_MODALIDADE = {
    "contratacoes_publicacao",
    "contratacoes_atualizacao"
}


def _requires_modalidade(resource_name: str) -> bool:
    """
    Verifica se um resource requer parâmetro de modalidade baseado no endpoint.
    Baseado na análise dos scripts em /scripts/ e documentação PNCP.
    """
    # Remove sufixos de modalidade se existirem para identificar o endpoint base
    base_name = resource_name.split('_mod')[0] if '_mod' in resource_name else resource_name
    return base_name in ENDPOINTS_REQUIRING_MODALIDADE


def _generate_modalidade_resources(base_resource: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Gera 13 resources separados para cada modalidade (1-13) para endpoints que requerem.
    Usa a enum ModalidadeContratacao do schemas.py.
    """
    resources = []
    
    for modalidade in ModalidadeContratacao:
        # Cria uma cópia do resource base
        resource = base_resource.copy()
        
        # Modifica nome para incluir modalidade
        original_name = resource["name"]
        resource["name"] = f"{original_name}_mod{modalidade.value}"
        
        # Adiciona parâmetro de modalidade
        resource["endpoint"] = resource["endpoint"].copy()
        resource["endpoint"]["params"] = resource["endpoint"]["params"].copy()
        resource["endpoint"]["params"]["codigoModalidadeContratacao"] = modalidade.value
        
        resources.append(resource)
    
    return resources


def _get_resources_by_type(config: Dict[str, Any], resource_type: str) -> List[Dict[str, Any]]:
    """
    Seleciona recursos baseado no tipo: sync, backfill, ou specialized.
    """
    type_mapping = {
        "sync": "sync_resources",
        "backfill": "backfill_resources", 
        "specialized": "specialized_resources"
    }
    
    key = type_mapping.get(resource_type, "sync_resources")
    return config.get(key, [])


def _process_resource_config(resource_config: Dict[str, Any], modalidade_codigo: Optional[int] = None) -> Dict[str, Any]:
    """
    Processa configuração do resource aplicando transformações específicas.
    BALIZA extrai TODAS as modalidades - não aplicamos filtros desnecessários.
    """
    # Faz uma cópia para não modificar o original
    config = resource_config.copy()
    
    # Configura paginador correto
    paginator = PageNumberPaginator(
        page_param="pagina",
        total_path="totalPaginas",
        current_value_path="numeroPagina"
    )
    
    # REMOVIDO: Não aplicamos filtro de modalidade
    # O BALIZA é um backup completo - extraímos dados de todas as modalidades
    
    # Configura incremental se presente
    if "incremental" in config:
        inc_config = config["incremental"]
        cursor_path = inc_config["cursor_path"]
        initial_value = inc_config["initial_value"]
        lag_days = inc_config.get("lag_days", 0)
        
        # Remove configuração incremental do dict (será aplicada diferentemente)
        del config["incremental"]
        
        # Aplica configuração incremental no endpoint
        config["endpoint"]["params"]["dataInicial"] = "{incremental.start_date}"
        config["endpoint"]["params"]["dataFinal"] = "{incremental.end_date}"
    
    # Aplica o paginador
    config["endpoint"]["paginator"] = paginator
    
    return config


def _create_incremental_resource(resource_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cria configuração de resource com suporte incremental.
    """
    if "incremental" not in resource_config:
        return resource_config
    
    inc_config = resource_config["incremental"]
    cursor_path = inc_config["cursor_path"]
    initial_value = inc_config["initial_value"]
    lag_days = inc_config.get("lag_days", 0)
    
    # Configura incremental
    incremental = Incremental(
        cursor_path=cursor_path,
        initial_value=initial_value,
        lag=timedelta(days=lag_days)
    )
    
    # Remove seção incremental e adiciona ao DLT resource
    config = resource_config.copy()
    del config["incremental"]
    
    # Aplica incremental
    config["incremental"] = incremental
    
    return config


@dlt.source(name="baliza_source", max_table_nesting=2)
def baliza_source(
    base_url: str = dlt.config.value,
    resource_type: str = "sync",  # "sync", "backfill", "specialized"
) -> List[DltResource]:
    """
    Source DLT inteligente para extração de dados da API PNCP.
    Extrai dados de TODAS as modalidades - backup completo e aberto.
    
    ESTRATÉGIA MULTI-MODALIDADE:
    - Endpoints que REQUEREM modalidade: gera 13 resources automáticos (mod1-mod13)
    - Endpoints que NÃO REQUEREM modalidade: usa 1 resource único
    
    Endpoints que REQUEREM modalidade (baseado em análise dos scripts):
    - contratacoes_publicacao
    - contratacoes_atualizacao  
    
    Endpoints que NÃO REQUEREM modalidade:
    - contratos, contratos_atualizacao (pega todas automaticamente)
    - atas, atas_atualizacao (modalidade não se aplica)
    - pca_usuario, instrumentos_cobranca (modalidade não se aplica)
    
    Args:
        base_url: URL base da API PNCP
        resource_type: Tipo de recursos a extrair (sync, backfill, specialized)
    
    Returns:
        Lista de recursos DLT configurados para backup completo de todas as 13 modalidades
    """
    # Carrega configuração dos recursos do arquivo YAML
    config_path = Path(__file__).parent.parent.parent / "config" / "pncp_resources.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Seleciona os recursos baseado no tipo solicitado
    resource_configs = _get_resources_by_type(config, resource_type)
    
    # Constroi a configuração do source com geração inteligente de modalidades
    resources = []
    for resource_config in resource_configs:
        # Verifica se este endpoint requer modalidade
        if _requires_modalidade(resource_config["name"]):
            # Gera 13 resources separados (um para cada modalidade)
            modalidade_resources = _generate_modalidade_resources(resource_config)
            for modalidade_resource in modalidade_resources:
                processed_config = _process_resource_config(modalidade_resource, modalidade_codigo=None)
                resources.append(processed_config)
        else:
            # Endpoint não requer modalidade - usa resource único
            processed_config = _process_resource_config(resource_config, modalidade_codigo=None)
            resources.append(processed_config)
    
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            # API PNCP é pública, não requer autenticação
        },
        "resources": resources,
    }

    return rest_api_source(source_config)


@dlt.source(name="baliza_backfill", max_table_nesting=2)
def baliza_backfill(
    base_url: str = dlt.config.value,
    start_date: str = "2021-01-01",
    end_date: Optional[str] = None,
    chunk_days: int = 7
) -> Generator[DltResource, None, None]:
    """
    Source especializado para backfill histórico com chunking inteligente.
    Extrai dados de TODAS as modalidades em chunks por data.
    
    Args:
        base_url: URL base da API PNCP
        start_date: Data inicial (YYYY-MM-DD)
        end_date: Data final (YYYY-MM-DD), padrão = hoje
        chunk_days: Tamanho do chunk em dias
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Processa em chunks por data (TODAS as modalidades em cada chunk)
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        
        # Configura source para este chunk específico
        source = baliza_source(
            base_url=base_url,
            resource_type="backfill"
        )
        
        # Aplica filtros de data no chunk
        for resource in source.resources:
            if hasattr(resource, 'endpoint'):
                params = resource.endpoint.get('params', {})
                params['dataInicial'] = current.strftime("%Y-%m-%d")
                params['dataFinal'] = chunk_end.strftime("%Y-%m-%d")
        
        yield from source.resources
        current = chunk_end


def run_pipeline(
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
    resource_type: str = "sync",
    **kwargs
):
    """
    Executa o pipeline com configurações otimizadas.
    """
    # Cria diretórios necessários
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path("schemas/export").mkdir(parents=True, exist_ok=True)
    
    # Configura pipeline
    pipeline = dlt.pipeline(
        pipeline_name="baliza_pncp",
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export"
    )
    
    # Seleciona source baseado no tipo
    if resource_type == "backfill":
        source = baliza_backfill(**kwargs)
    else:
        source = baliza_source(resource_type=resource_type, **kwargs)
    
    # Executa pipeline
    info = pipeline.run(source)
    
    print("\n=== Pipeline Executado com Sucesso ===")
    print(f"Destination: {destination}")
    print(f"Dataset: {dataset_name}")
    print(f"Resources processados: {len(info.load_packages[0].jobs) if info.load_packages else 0}")
    print("Schema path: schemas/export")
    
    return info


if __name__ == "__main__":
    # Execução padrão: sync incremental
    info = run_pipeline(
        destination="duckdb",
        dataset_name="pncp_data", 
        resource_type="sync"
    )
    print(info)