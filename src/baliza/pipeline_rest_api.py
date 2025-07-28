from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path

import dlt
import yaml
from dlt.extract.source import DltResource

from .schemas import ModalidadeContratacao

# Endpoints que REQUEREM parâmetro modalidade (baseado na análise dos scripts)
ENDPOINTS_REQUIRING_MODALIDADE = {
    "contratacoes_publicacao",
    "contratacoes_atualizacao"
}

def _requires_modalidade(resource_name: str) -> bool:
    """
    Verifica se um resource requer parâmetro de modalidade baseado no endpoint.
    """
    base_name = resource_name.split('_mod')[0] if '_mod' in resource_name else resource_name
    return base_name in ENDPOINTS_REQUIRING_MODALIDADE

def _generate_modalidade_resources(base_resource: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Gera 13 resources separados para cada modalidade (1-13) para endpoints que requerem.
    """
    import copy
    resources = []
    
    for modalidade in ModalidadeContratacao:
        # Cria uma cópia do resource base
        resource = copy.deepcopy(base_resource)
        
        # Modifica nome para incluir modalidade
        original_name = resource["name"]
        resource["name"] = f"{original_name}_mod{modalidade.value}"
        
        # Adiciona parâmetro de modalidade
        if "endpoint" not in resource:
            resource["endpoint"] = {}
        if "params" not in resource["endpoint"]:
            resource["endpoint"]["params"] = {}
            
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

@dlt.source(name="baliza_rest_api_source", max_table_nesting=2)
def baliza_rest_api_source(
    base_url: str = "https://pncp.gov.br/api/consulta",
    resource_type: str = "sync"
) -> List[DltResource]:
    """
    Source DLT usando REST API source oficial.
    Extrai dados de TODAS as modalidades com estratégia multi-modalidade inteligente.
    """
    try:
        from dlt.sources.rest_api import rest_api_source
        from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
    except ImportError:
        raise ImportError(
            "DLT REST API source não disponível. "
            "Execute: uv add 'dlt[rest-api]' para instalar."
        )
    
    # Carrega configuração dos recursos do arquivo YAML
    config_path = Path(__file__).parent.parent.parent / "config" / "pncp_resources.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Seleciona os recursos baseado no tipo solicitado
    resource_configs = _get_resources_by_type(config, resource_type)
    
    print(f"\n🚀 Criando REST API source BALIZA para resource_type: {resource_type}")
    print(f"📋 Resources base encontrados: {len(resource_configs)}")
    
    # Prepara resources para o REST API source
    rest_api_resources = []
    total_final_resources = 0
    
    for resource_config in resource_configs:
        resource_name = resource_config["name"]
        
        if _requires_modalidade(resource_name):
            # Gera 13 resources separados (um para cada modalidade)
            modalidade_resources = _generate_modalidade_resources(resource_config)
            print(f"⚠️  {resource_name} REQUER modalidade → gerou {len(modalidade_resources)} resources")
            
            for modalidade_resource in modalidade_resources:
                # Converte configuração YAML para formato REST API source
                # Processa parâmetros incrementais
                params = modalidade_resource["endpoint"]["params"].copy()
                
                # Substitui placeholders incrementais por configuração DLT
                if "dataInicial" in params and params["dataInicial"] == "{incremental.start}":
                    # Remove para que DLT configure automaticamente
                    del params["dataInicial"] 
                if "dataFinal" in params and params["dataFinal"] == "{incremental.end}":
                    del params["dataFinal"]
                
                rest_resource = {
                    "name": modalidade_resource["name"],
                    "endpoint": {
                        "path": modalidade_resource["endpoint"]["path"],
                        "params": params,
                        "paginator": PageNumberPaginator(
                            page_param="pagina",
                            total_path="totalPaginas",
                            current_value_path="numeroPagina"
                        )
                    },
                    "primary_key": modalidade_resource.get("primary_key"),
                    "write_disposition": modalidade_resource.get("write_disposition", "append")
                }
                
                # Adiciona configuração incremental se presente no YAML
                if "incremental" in modalidade_resource:
                    from dlt.extract.incremental import Incremental
                    inc_config = modalidade_resource["incremental"]
                    
                    rest_resource["incremental"] = Incremental(
                        cursor_path=inc_config["cursor_path"],
                        initial_value=inc_config["initial_value"],
                        lag=timedelta(days=inc_config.get("lag_days", 0))
                    )
                rest_api_resources.append(rest_resource)
                total_final_resources += 1
        else:
            # Endpoint não requer modalidade - usa resource único
            print(f"✅ {resource_name} NÃO requer modalidade → 1 resource único")
            
            # Processa parâmetros incrementais
            params = resource_config["endpoint"]["params"].copy()
            
            # Remove placeholders incrementais para configuração DLT automática
            if "dataInicial" in params and params["dataInicial"] == "{incremental.start}":
                del params["dataInicial"]
            if "dataFinal" in params and params["dataFinal"] == "{incremental.end}":
                del params["dataFinal"]
            
            rest_resource = {
                "name": resource_config["name"],
                "endpoint": {
                    "path": resource_config["endpoint"]["path"],
                    "params": params,
                    "paginator": PageNumberPaginator(
                        page_param="pagina", 
                        total_path="totalPaginas",
                        current_value_path="numeroPagina"
                    )
                },
                "primary_key": resource_config.get("primary_key"),
                "write_disposition": resource_config.get("write_disposition", "append")
            }
            
            # Adiciona configuração incremental se presente
            if "incremental" in resource_config:
                from dlt.extract.incremental import Incremental
                inc_config = resource_config["incremental"]
                
                rest_resource["incremental"] = Incremental(
                    cursor_path=inc_config["cursor_path"],
                    initial_value=inc_config["initial_value"],
                    lag=timedelta(days=inc_config.get("lag_days", 0))
                )
            rest_api_resources.append(rest_resource)
            total_final_resources += 1
    
    print(f"🎯 Total de resources finais: {total_final_resources}")
    
    # Cria REST API source configurado
    source_config = {
        "client": {
            "base_url": base_url,
            # API PNCP é pública, não requer autenticação
        },
        "resources": rest_api_resources
    }
    
    return rest_api_source(source_config)

def run_rest_api_pipeline(
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
    resource_type: str = "sync",
    **kwargs
):
    """
    Executa o pipeline usando REST API source oficial do DLT.
    """
    # Cria diretórios necessários
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    Path("schemas/export").mkdir(parents=True, exist_ok=True)
    
    # Configura pipeline
    pipeline = dlt.pipeline(
        pipeline_name="baliza_pncp_rest_api",
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export"
    )
    
    # Usa REST API source
    source = baliza_rest_api_source(resource_type=resource_type, **kwargs)
    
    # Executa pipeline
    info = pipeline.run(source)
    
    print("\n=== Pipeline REST API Executado com Sucesso ===")
    print(f"Destination: {destination}")
    print(f"Dataset: {dataset_name}")
    print(f"Resources processados: {len(info.load_packages[0].jobs) if info.load_packages else 0}")
    print("Schema path: schemas/export")
    
    return info

if __name__ == "__main__":
    # Teste da implementação REST API
    try:
        info = run_rest_api_pipeline(
            destination="duckdb",
            dataset_name="pncp_rest_api_test",
            resource_type="specialized"  # Teste com endpoints simples primeiro
        )
        print("✅ Pipeline REST API funcionando!")
        print(info)
    except ImportError as e:
        print(f"❌ {e}")
        print("🔄 Usando pipeline manual com requests como fallback...")
        from .pipeline import run_pipeline
        run_pipeline(resource_type="specialized")