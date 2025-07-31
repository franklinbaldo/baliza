"""
Pipeline principal do Baliza - Versão unificada e simplificada.

Esta versão elimina a duplicação de código e delega 100% do trabalho
pesado para o dlt rest_api_source, como recomendado pela documentação.
"""

from typing import Optional, Generator

import dlt
from pathlib import Path
from dlt.extract.source import DltResource
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)
from datetime import datetime, date
import calendar

from .enums import ModalidadeContratacao
from .utils.time import date_range_slicer
from .resources import prepare_resources_for_dlt, get_resource_summary, create_pncp_rest_config
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


if __name__ == "__main__":
    # Exemplo de uso
    run_sync_pipeline()
