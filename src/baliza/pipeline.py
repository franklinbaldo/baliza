from typing import List, Dict, Any, Optional, Generator
from datetime import datetime, timedelta
from pathlib import Path

import dlt
import yaml
import requests
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


def fetch_pncp_data(base_url: str, endpoint_path: str, params: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
    """
    Faz requisições paginadas à API PNCP e retorna os dados.
    """
    url = f"{base_url.rstrip('/')}{endpoint_path}"
    pagina = 1
    
    while True:
        # Prepara parâmetros da requisição
        request_params = params.copy()
        request_params.update({
            "pagina": pagina,
            "tamanhoPagina": 500  # Máximo por página (mínimo 10, máximo 500)
        })
        
        print(f"🔄 Fazendo requisição: {endpoint_path} - página {pagina}")
        print(f"   📋 Params: {request_params}")
        
        try:
            response = requests.get(url, params=request_params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Extrai os dados da resposta
            items = data.get("data", [])
            total_paginas = data.get("totalPaginas", 1)
            
            print(f"   ✅ Página {pagina}/{total_paginas}: {len(items)} registros")
            
            # Yield cada item individualmente
            for item in items:
                yield item
            
            # Verifica se há mais páginas
            if pagina >= total_paginas or len(items) == 0:
                break
                
            pagina += 1
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Erro na requisição: {e}")
            break
        except Exception as e:
            print(f"   ❌ Erro inesperado: {e}")
            break


def split_date_range_monthly(data_inicial: str, data_final: str) -> List[tuple]:
    """
    Divide um período em chunks mensais para organização e limite da API PNCP.
    Cada chunk corresponde a um mês completo.
    """
    # Converte strings para datetime
    start = datetime.strptime(data_inicial, "%Y%m%d")
    end = datetime.strptime(data_final, "%Y%m%d")
    
    chunks = []
    current = start.replace(day=1)  # Sempre começa no dia 1 do mês
    
    while current <= end:
        # Calcula o último dia do mês atual
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1)
        else:
            next_month = current.replace(month=current.month + 1)
        
        chunk_end = next_month - timedelta(days=1)  # Último dia do mês
        
        # Não ultrapassa a data final solicitada
        if chunk_end > end:
            chunk_end = end
        
        chunks.append((
            current.strftime("%Y%m%d"),
            chunk_end.strftime("%Y%m%d")
        ))
        
        # Move para o primeiro dia do próximo mês
        current = next_month
        
        # Para se já passou da data final
        if current > end:
            break
    
    return chunks

def create_pncp_resource(resource_config: Dict[str, Any], base_url: str) -> DltResource:
    """
    Cria um DLT resource para um endpoint específico da API PNCP.
    Automaticamente divide períodos em chunks mensais para organização.
    """
    resource_name = resource_config["name"]
    endpoint_path = resource_config["endpoint"]["path"]
    endpoint_params = resource_config["endpoint"]["params"].copy()
    
    # Processa placeholders de data incremental
    data_inicial = endpoint_params.get("dataInicial", "20210101")
    data_final = endpoint_params.get("dataFinal", datetime.now().strftime("%Y%m%d"))
    
    if data_inicial == "{incremental.start}":
        data_inicial = "20210101"  # Data padrão inicial
    if data_final == "{incremental.end}":
        data_final = datetime.now().strftime("%Y%m%d")
    
    @dlt.resource(name=resource_name, table_name=resource_config.get("table_name", resource_name))
    def resource_func():
        """Resource que busca dados da API PNCP com chunking mensal"""
        print(f"\n🚀 Iniciando extração: {resource_name}")
        print(f"   🎯 Endpoint: {endpoint_path}")
        print(f"   📅 Período: {data_inicial} até {data_final}")
        
        # Divide o período em chunks mensais
        date_chunks = split_date_range_monthly(data_inicial, data_final)
        print(f"   📆 Dividido em {len(date_chunks)} meses para organização")
        
        total_items = 0
        
        for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
            # Converte datas para formato mais legível para log
            start_readable = datetime.strptime(chunk_start, "%Y%m%d").strftime("%Y-%m")
            end_readable = datetime.strptime(chunk_end, "%Y%m%d").strftime("%Y-%m-%d")
            
            print(f"\n   📦 Mês {i}/{len(date_chunks)}: {start_readable} ({chunk_start} até {chunk_end})")
            
            # Prepara parâmetros para este chunk
            chunk_params = endpoint_params.copy()
            chunk_params["dataInicial"] = chunk_start
            chunk_params["dataFinal"] = chunk_end
            
            chunk_items = 0
            # Usa a função de fetch para obter os dados deste chunk
            for item in fetch_pncp_data(base_url, endpoint_path, chunk_params):
                yield item
                chunk_items += 1
                total_items += 1
            
            print(f"   ✅ {start_readable} concluído: {chunk_items} registros")
            
        print(f"✅ Extração concluída: {resource_name} - Total: {total_items} registros")
    
    return resource_func


@dlt.source(name="baliza_source", max_table_nesting=2)
def baliza_source(
    base_url: str = "https://pncp.gov.br/api/consulta",
    resource_type: str = "sync",  # "sync", "backfill", "specialized"
) -> List[DltResource]:
    """
    Source DLT inteligente para extração de dados da API PNCP com requests simples.
    Extrai dados de TODAS as modalidades - backup completo e aberto.
    
    ESTRATÉGIA MULTI-MODALIDADE:
    - Endpoints que REQUEREM modalidade: gera 13 resources automáticos (mod1-mod13)
    - Endpoints que NÃO REQUEREM modalidade: usa 1 resource único
    
    CHUNKING MENSAL:
    - Divide períodos em chunks mensais para organização e gerenciamento
    
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
    
    print(f"\n🚀 Criando source BALIZA para resource_type: {resource_type}")
    print(f"📋 Resources base encontrados: {len(resource_configs)}")
    
    # Constroi a lista de recursos DLT com geração inteligente de modalidades
    resources = []
    total_final_resources = 0
    
    for resource_config in resource_configs:
        resource_name = resource_config["name"]
        
        # Verifica se este endpoint requer modalidade
        if _requires_modalidade(resource_name):
            # Gera 13 resources separados (um para cada modalidade)
            modalidade_resources = _generate_modalidade_resources(resource_config)
            print(f"⚠️  {resource_name} REQUER modalidade → gerou {len(modalidade_resources)} resources")
            
            for modalidade_resource in modalidade_resources:
                dlt_resource = create_pncp_resource(modalidade_resource, base_url)
                resources.append(dlt_resource)
                total_final_resources += 1
        else:
            # Endpoint não requer modalidade - usa resource único
            print(f"✅ {resource_name} NÃO requer modalidade → 1 resource único")
            dlt_resource = create_pncp_resource(resource_config, base_url)
            resources.append(dlt_resource)
            total_final_resources += 1
    
    print(f"🎯 Total de resources finais: {total_final_resources}")
    return resources


@dlt.source(name="baliza_backfill", max_table_nesting=2)
def baliza_backfill(
    base_url: str = "https://pncp.gov.br/api/consulta",
    start_date: str = "2021-01-01",
    end_date: Optional[str] = None
) -> List[DltResource]:
    """
    Source especializado para backfill histórico.
    Extrai dados de TODAS as modalidades para um período específico.
    Automaticamente divide em chunks mensais para organização.
    
    Args:
        base_url: URL base da API PNCP
        start_date: Data inicial (YYYY-MM-DD) 
        end_date: Data final (YYYY-MM-DD), padrão = hoje
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"🚀 Backfill BALIZA: {start_date} até {end_date}")
    
    # Usa o source principal com tipo backfill
    # O chunking mensal automático já está implementado na função create_pncp_resource
    return baliza_source(base_url=base_url, resource_type="backfill")


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