"""
Pipeline principal do Baliza - Versão unificada e simplificada.

Esta versão elimina a duplicação de código e delega 100% do trabalho
pesado para o dlt rest_api_source, como recomendado pela documentação.
"""

from typing import Optional, Generator

import dlt
from dlt.extract.source import DltResource
from dlt.sources.rest_api import (
    check_connection,
    rest_api_resources,
    rest_api_source,
)
from datetime import datetime, date
import calendar
from typing import List

from .utils.time import date_range_slicer
from .resources import create_pncp_rest_config
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


# Legacy functions removed - modernized configuration now uses direct DLT patterns


# =============================================================================
# FONTE PRINCIPAL PARA SINCRONIZAÇÃO CONTÍNUA
# =============================================================================


@dlt.source(name="baliza_source")
def pncp_source(
    resource_type: str = "monthly", 
    base_url: Optional[str] = dlt.secrets.value,
    year_month: Optional[str] = None,
    exclude_modalidades: Optional[list[int]] = None
) -> Generator[DltResource, None, None]:
    """
    Fonte dlt declarativa para a API do PNCP com validação Pydantic.

    Uses modernized DLT REST API configuration with direct RESTAPIConfig usage,
    eliminating custom abstraction layers and implementing monthly extraction.

    Args:
        resource_type: Tipo de resource ('monthly', 'backfill', 'specialized')
        base_url: URL base da API PNCP
        year_month: Ano e mês para extração (formato YYYYMM)
        exclude_modalidades: Lista de modalidades para excluir (ex: [2, 7, 8])

    Yields:
        DltResource: Resources configurados com schema Pydantic
    """
    # Get base URL
    api_base_url = base_url or dlt.secrets["sources.baliza_source.base_url"]
    
    # Create modernized RESTAPIConfig directly (DLT best practices)
    api_config = create_pncp_rest_config(resource_type, api_base_url, year_month, exclude_modalidades)

    # Return resources using modern DLT configuration
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

    # Get base URL for API calls
    api_base_url = dlt.secrets["sources.baliza_source.base_url"]
    
    # Use modernized REST configuration for backfill
    api_config = create_pncp_rest_config("backfill", api_base_url)
    final_resources = api_config["resources"]

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

                # Atualizar parâmetros de data no formato modernizado
                config_copy = config.copy()
                if "endpoint" in config_copy and "params" in config_copy["endpoint"]:
                    config_copy["endpoint"]["params"]["dataInicial"] = start
                    config_copy["endpoint"]["params"]["dataFinal"] = end

                # Configuração do source para este chunk (usando estrutura modernizada)
                chunk_source_config = {
                    "client": api_config["client"],  # Use same client config as main
                    "resource_defaults": api_config["resource_defaults"],
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


def check_pncp_connection(year_month: Optional[str] = None, exclude_modalidades: Optional[list[int]] = None) -> None:
    """Verifica conectividade com a API PNCP."""
    source = pncp_source(resource_type="monthly", year_month=year_month or "202406", exclude_modalidades=exclude_modalidades)

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


def _check_month_completion(year_month: str) -> None:
    """
    Verifica se o mês pode ser processado.
    
    Para meses passados: sempre pode processar
    Para mês atual: aguarda que o mês termine
    """
    year = int(year_month[:4])
    month = int(year_month[4:])
    
    today = date.today()
    current_year_month = f"{today.year:04d}{today.month:02d}"
    
    if year_month > current_year_month:
        raise ValueError(f"❌ Não é possível extrair dados de mês futuro: {year_month}")
    
    if year_month == current_year_month:
        # Para o mês atual, verificar se já terminou
        last_day_of_month = calendar.monthrange(year, month)[1]
        if today.day < last_day_of_month:
            print(f"⏳ Mês atual ({year_month}) ainda não terminou. Aguardando...")
            print(f"   Hoje: {today}")
            print(f"   Último dia do mês: {year}-{month:02d}-{last_day_of_month}")
            raise ValueError(f"Mês {year_month} ainda não foi concluído")
    
    print(f"✅ Mês {year_month} pode ser processado")


def run_monthly_pipeline(
    year_month: str,
    pipeline_name: str = "baliza_pncp_monthly",
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
    exclude_modalidades: Optional[list[int]] = None,
):
    """Executa o pipeline de extração mensal com validação Pydantic."""
    # Verificar se o mês pode ser processado
    _check_month_completion(year_month)
    
    # Verificar conectividade primeiro
    check_pncp_connection(year_month, exclude_modalidades)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export",
    )

    source = pncp_source(resource_type="monthly", year_month=year_month, exclude_modalidades=exclude_modalidades)

    print(f"🚀 Executando pipeline mensal para {year_month} com validação Pydantic...")
    info = pipeline.run(source)
    print("✅ Extração mensal concluída com data quality validado!")
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


# =============================================================================
# ORQUESTRADOR INTELIGENTE PARA COMANDO ÚNICO 'baliza run'
# =============================================================================


def _get_months_to_process() -> List[str]:
    """
    Calcula todos os meses que devem ser processados, de 202101 até o mês anterior ao atual.
    
    Returns:
        Lista de strings no formato YYYYMM
    """
    start_year = 2021
    start_month = 1
    
    today = date.today()
    # Processamos até o mês anterior ao atual (mês atual pode não estar completo)
    if today.month == 1:
        end_year = today.year - 1
        end_month = 12
    else:
        end_year = today.year
        end_month = today.month - 1
    
    months = []
    current_year = start_year
    current_month = start_month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months.append(f"{current_year:04d}{current_month:02d}")
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    return months


def run_intelligent_pipeline(
    destination: str = "duckdb",
    dataset_name: str = "pncp_data",
    pipeline_name: str = "baliza_autonomous",
    exclude_modalidades: Optional[List[int]] = None,
    full_rebuild: bool = False,
):
    """
    Orquestrador inteligente que processa todos os meses necessários de forma idempotente.
    
    Args:
        destination: Destino dos dados (duckdb, postgresql, etc.)
        dataset_name: Nome do dataset/schema
        pipeline_name: Nome do pipeline
        exclude_modalidades: Lista de modalidades para excluir
        full_rebuild: Se True, ignora o estado e reprocessa todos os meses
    
    Returns:
        Informações sobre a execução do pipeline
    """
    # TODO HIGH: Move state backend configuration before pipeline creation
    # DLT 1.14.1 requires state backend to be set before pipeline initialization
    # Current approach may cause state synchronization issues
    dlt.config["state.backend"] = destination
    
    print("🗓️  Calculando meses para processar...")
    months_to_process = _get_months_to_process()
    
    if not months_to_process:
        print("✅ Nenhum mês para processar. Dados já estão atualizados.")
        return None
    
    print(f"📅 Período: {months_to_process[0]} até {months_to_process[-1]} ({len(months_to_process)} meses)")
    
    # TODO HIGH: Add schema contracts for data quality enforcement
    # With comprehensive Pydantic models available, enable schema contracts:
    # - data_type = "freeze" to prevent type changes
    # - columns = "evolve" to allow new columns
    
    # TODO MEDIUM: Add retry configuration and error handling
    # Current implementation lacks proper error recovery mechanisms
    
    # Inicializar pipeline
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        export_schema_path="schemas/export",
        # TODO HIGH: Add schema contracts configuration here
        # schema_contract={"data_type": "freeze", "columns": "evolve"}
    )
    
    # Carregar estado (lista de meses concluídos)
    if full_rebuild:
        print("🔄 Modo full-rebuild: resetando estado...")
        pipeline.state["completed_months"] = []
    
    completed_months = pipeline.state.get("completed_months", [])
    print(f"📊 Meses já processados: {len(completed_months)}")
    
    # Determinar meses que ainda precisam ser processados
    pending_months = [month for month in months_to_process if month not in completed_months]
    
    if not pending_months:
        print("✅ Todos os meses já foram processados. Pipeline atualizado.")
        return None
    
    print(f"⏳ Meses pendentes: {len(pending_months)}")
    
    # Verificar conectividade antes de começar
    print("🔗 Verificando conectividade com API PNCP...")
    check_pncp_connection(pending_months[0], exclude_modalidades)
    
    # Processar cada mês pendente
    last_info = None
    for i, month in enumerate(pending_months, 1):
        print(f"⏳ Processando mês {month} ({i}/{len(pending_months)})...")
        
        # TODO HIGH: Add comprehensive error handling and retry logic
        # Current implementation lacks proper handling of:
        # - API rate limiting (429 errors)
        # - Network timeouts and transient failures  
        # - State corruption recovery
        # - Partial month processing failures
        
        try:
            # Verificar se o mês pode ser processado
            _check_month_completion(month)
            
            # TODO MEDIUM: Add request rate limiting to respect API limits
            # Consider adding delays between requests or implementing backoff
            
            # Executar extração para este mês
            source = pncp_source(
                resource_type="monthly", 
                year_month=month, 
                exclude_modalidades=exclude_modalidades
            )
            
            info = pipeline.run(source)
            
            # TODO MEDIUM: Add state validation before marking as complete
            # Verify that all expected resources were processed successfully
            
            # Marcar mês como concluído no estado
            if "completed_months" not in pipeline.state:
                pipeline.state["completed_months"] = []
            pipeline.state["completed_months"].append(month)
            
            print(f"✅ Mês {month} concluído com sucesso.")
            
            if info and info.load_packages:
                rows_loaded = sum(
                    job.job_file_info.rows_in_table or 0
                    for package in info.load_packages
                    for job in package.jobs
                )
                print(f"   Linhas carregadas: {rows_loaded:,}")
            
            last_info = info
            
        except Exception as e:
            # TODO HIGH: Implement proper exception classification and recovery
            # - Transient errors should retry with backoff
            # - Permanent errors should fail fast
            # - State corruption should trigger recovery procedures
            print(f"❌ Erro ao processar mês {month}: {e}")
            print("   Progresso salvo até o mês anterior. Execute novamente para continuar.")
            raise
    
    print(f"🎉 Pipeline concluído! Processados {len(pending_months)} novos meses.")
    print(f"📊 Total de meses concluídos: {len(pipeline.state.get('completed_months', []))}")
    
    return last_info


if __name__ == "__main__":
    # Exemplo de uso do novo orquestrador
    run_intelligent_pipeline()
