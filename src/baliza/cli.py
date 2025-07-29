"""
Interface de linha de comando para o Baliza.

Fornece comandos simples para executar sincronização e backfill.
"""

import typer
from pathlib import Path

from .pipeline import run_sync_pipeline, run_backfill_pipeline

app = typer.Typer(
    name="baliza",
    help="Pipeline de extração de dados da API PNCP",
    add_completion=False,
)


@app.command()
def sync(
    destination: str = typer.Option(
        "duckdb", help="Destino dos dados (duckdb, postgresql, etc.)"
    ),
    dataset: str = typer.Option("pncp_data", help="Nome do dataset/schema"),
    pipeline_name: str = typer.Option("baliza_pncp_sync", help="Nome do pipeline"),
):
    """
    Executa sincronização contínua (incremental) dos dados da API PNCP.

    Este comando extrai apenas dados novos/atualizados desde a última execução.
    """
    typer.echo("🚀 Iniciando sincronização incremental...")
    typer.echo(f"   Destino: {destination}")
    typer.echo(f"   Dataset: {dataset}")
    typer.echo(f"   Pipeline: {pipeline_name}")
    typer.echo()

    try:
        info = run_sync_pipeline(
            pipeline_name=pipeline_name, destination=destination, dataset_name=dataset
        )

        typer.echo("✅ Sincronização concluída com sucesso!")
        if info and info.load_packages:
            rows_loaded = sum(
                job.job_file_info.rows_in_table or 0
                for package in info.load_packages
                for job in package.jobs
            )
            typer.echo(f"   Linhas carregadas: {rows_loaded:,}")

    except Exception as e:
        typer.echo(f"❌ Erro na sincronização: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def backfill(
    start_date: str = typer.Argument(..., help="Data inicial (formato: YYYYMMDD)"),
    end_date: str = typer.Argument(..., help="Data final (formato: YYYYMMDD)"),
    chunk_days: int = typer.Option(7, help="Tamanho do chunk em dias"),
    destination: str = typer.Option("duckdb", help="Destino dos dados"),
    dataset: str = typer.Option("pncp_data", help="Nome do dataset/schema"),
    pipeline_name: str = typer.Option("baliza_pncp_backfill", help="Nome do pipeline"),
):
    """
    Executa backfill histórico de dados da API PNCP.

    Este comando extrai dados de um período específico, processando
    em chunks paralelos e de forma resumível.

    Exemplos:
      baliza backfill 20240101 20240331  # Q1 2024
      baliza backfill 20230101 20231231 --chunk-days 30  # 2023 inteiro, chunks mensais
    """
    # Validação básica de formato de data
    if len(start_date) != 8 or len(end_date) != 8:
        typer.echo("❌ Erro: Datas devem estar no formato YYYYMMDD", err=True)
        raise typer.Exit(1)

    try:
        int(start_date)
        int(end_date)
    except ValueError:
        typer.echo("❌ Erro: Datas devem conter apenas números", err=True)
        raise typer.Exit(1)

    if start_date > end_date:
        typer.echo("❌ Erro: Data inicial deve ser anterior à data final", err=True)
        raise typer.Exit(1)

    typer.echo("🚀 Iniciando backfill histórico...")
    typer.echo(f"   Período: {start_date} até {end_date}")
    typer.echo(f"   Chunk: {chunk_days} dias")
    typer.echo(f"   Destino: {destination}")
    typer.echo(f"   Dataset: {dataset}")
    typer.echo(f"   Pipeline: {pipeline_name}")
    typer.echo()

    try:
        info = run_backfill_pipeline(
            start_date=start_date,
            end_date=end_date,
            chunk_days=chunk_days,
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset,
        )

        typer.echo("✅ Backfill concluído com sucesso!")
        if info and info.load_packages:
            rows_loaded = sum(
                job.job_file_info.rows_in_table or 0
                for package in info.load_packages
                for job in package.jobs
            )
            typer.echo(f"   Linhas carregadas: {rows_loaded:,}")

    except Exception as e:
        typer.echo(f"❌ Erro no backfill: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def status():
    """
    Mostra o status dos pipelines e configurações.
    """
    typer.echo("📊 Status do Baliza")
    typer.echo("=" * 50)

    # Verificar configurações
    config_path = Path("config/pncp_resources.yaml")
    secrets_path = Path(".dlt/secrets.toml")

    typer.echo(
        f"Configuração YAML: {'✅' if config_path.exists() else '❌'} {config_path}"
    )
    typer.echo(f"Secrets DLT: {'✅' if secrets_path.exists() else '❌'} {secrets_path}")

    # Verificar diretórios de dados
    data_dir = Path("data")
    logs_dir = Path("logs")
    schemas_dir = Path("schemas/export")

    typer.echo(f"Diretório dados: {'✅' if data_dir.exists() else '❌'} {data_dir}")
    typer.echo(f"Diretório logs: {'✅' if logs_dir.exists() else '❌'} {logs_dir}")
    typer.echo(f"Schema export: {'✅' if schemas_dir.exists() else '❌'} {schemas_dir}")


@app.command()
def info():
    """
    Mostra informações sobre os recursos disponíveis.
    """
    from .pipeline import (
        _load_yaml_config,
        _get_resources_by_type,
        _requires_modalidade,
    )

    typer.echo("📋 Recursos Configurados")
    typer.echo("=" * 50)

    try:
        config = _load_yaml_config()

        for resource_type in ["sync", "backfill", "specialized"]:
            resources = _get_resources_by_type(config, resource_type)
            if not resources:
                continue

            typer.echo(
                f"\n🔹 {resource_type.upper()} ({len(resources)} recursos base):"
            )

            total_final = 0
            for resource in resources:
                name = resource["name"]
                requires_mod = _requires_modalidade(name)
                final_count = 13 if requires_mod else 1
                total_final += final_count

                status = "⚠️  MODALIDADE (13x)" if requires_mod else "✅ ÚNICO"
                typer.echo(f"   • {name} - {status}")

            typer.echo(f"   📊 Total final: {total_final} resources")

    except Exception as e:
        typer.echo(f"❌ Erro ao carregar configuração: {e}", err=True)


if __name__ == "__main__":
    app()
