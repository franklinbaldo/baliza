"""
Interface de linha de comando para o Baliza.

Fornece comandos simples para executar sincronização e backfill.
"""

import typer
from pathlib import Path

from .pipeline import run_monthly_pipeline, run_backfill_pipeline

app = typer.Typer(
    name="baliza",
    help="Pipeline de extração de dados da API PNCP",
    add_completion=False,
)


@app.command()
def monthly(
    year_month: str = typer.Argument(..., help="Ano e mês (formato: YYYYMM)"),
    destination: str = typer.Option(
        "duckdb", help="Destino dos dados (duckdb, postgresql, etc.)"
    ),
    dataset: str = typer.Option("pncp_data", help="Nome do dataset/schema"),
    pipeline_name: str = typer.Option("baliza_pncp_monthly", help="Nome do pipeline"),
    exclude_modalidades: str = typer.Option(
        "", 
        help="Modalidades para excluir (ex: '2,7,8' ou '2,9,10,11,12,13' para modalidades com pouco dados)"
    ),
):
    """
    Executa extração mensal dos dados da API PNCP.

    Este comando extrai dados de um mês específico, aguardando automaticamente
    que o mês termine antes de processar dados do mês atual.
    
    Por padrão, processa todas as 13 modalidades. Use --exclude-modalidades 
    para excluir modalidades específicas que podem ter poucos dados ou causar 
    problemas (ex: modalidades 2, 7, 8, 9, 10, 11, 12, 13).
    
    Exemplos:
      baliza monthly 202407  # Julho 2024, todas modalidades
      baliza monthly 202407 --exclude-modalidades "2,7,8"  # Exclui modalidades 2, 7, 8
      baliza monthly 202412 --exclude-modalidades "9,10,11,12,13"  # Só modalidades 1-8
    """
    # Validação básica de formato
    if len(year_month) != 6:
        typer.echo("❌ Erro: Ano e mês devem estar no formato YYYYMM", err=True)
        raise typer.Exit(1)

    try:
        int(year_month)
    except ValueError:
        typer.echo("❌ Erro: Ano e mês devem conter apenas números", err=True)
        raise typer.Exit(1)

    year = int(year_month[:4])
    month = int(year_month[4:])
    
    if month < 1 or month > 12:
        typer.echo("❌ Erro: Mês deve estar entre 01 e 12", err=True)
        raise typer.Exit(1)

    # Parse exclude_modalidades parameter
    exclude_modalidades_list = []
    if exclude_modalidades.strip():
        try:
            exclude_modalidades_list = [int(x.strip()) for x in exclude_modalidades.split(",")]
            # Validate modalidades are in valid range (1-13)
            for modalidade in exclude_modalidades_list:
                if modalidade < 1 or modalidade > 13:
                    typer.echo(f"❌ Erro: Modalidade {modalidade} deve estar entre 1 e 13", err=True)
                    raise typer.Exit(1)
        except ValueError:
            typer.echo("❌ Erro: Modalidades devem ser números separados por vírgula (ex: '2,7,8')", err=True)
            raise typer.Exit(1)

    typer.echo("🚀 Iniciando extração mensal...")
    typer.echo(f"   Período: {year_month}")
    typer.echo(f"   Destino: {destination}")
    typer.echo(f"   Dataset: {dataset}")
    typer.echo(f"   Pipeline: {pipeline_name}")
    if exclude_modalidades_list:
        typer.echo(f"   Modalidades excluídas: {exclude_modalidades_list}")
    typer.echo()

    try:
        info = run_monthly_pipeline(
            year_month=year_month,
            pipeline_name=pipeline_name, 
            destination=destination, 
            dataset_name=dataset,
            exclude_modalidades=exclude_modalidades_list
        )

        typer.echo("✅ Extração mensal concluída com sucesso!")
        if info and info.load_packages:
            rows_loaded = sum(
                job.job_file_info.rows_in_table or 0
                for package in info.load_packages
                for job in package.jobs
            )
            typer.echo(f"   Linhas carregadas: {rows_loaded:,}")

    except Exception as e:
        typer.echo(f"❌ Erro na extração mensal: {e}", err=True)
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
    secrets_path = Path(".dlt/secrets.toml")

    typer.echo("Configuração Resources: ✅ Python-based (resources.py)")
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
    from .resources import get_resource_summary

    typer.echo("📋 Recursos Configurados")
    typer.echo("=" * 50)

    try:
        summary = get_resource_summary()

        for resource_type, info in summary.items():
            typer.echo(
                f"\n🔹 {resource_type.upper()} ({info['base_resources']} recursos base):"
            )

            for resource in info["resources"]:
                status = "⚠️  MODALIDADE (13x)" if resource["requires_modalidade"] else "✅ ÚNICO"
                incremental = "📈 INCREMENTAL" if resource["has_incremental"] else "📦 FULL"
                typer.echo(f"   • {resource['name']} - {status} - {incremental}")
                typer.echo(f"     Table: {resource['table']} | Page Size: {resource['page_size']}")

            typer.echo(f"   📊 Total final: {info['final_resources']} resources")

    except Exception as e:
        typer.echo(f"❌ Erro ao carregar configuração: {e}", err=True)


if __name__ == "__main__":
    app()
