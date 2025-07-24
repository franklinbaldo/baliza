import asyncio

import typer
from rich.console import Console
from rich.table import Table

from .backend import init_database_schema, connect
from .flows.raw import extract_phase_2a_concurrent
from .flows.staging import staging_transformation
from .flows.marts import marts_creation
from .config import settings

app = typer.Typer()
console = Console()


@app.command()
def run(
    mes: str = typer.Option(
        None, "--mes", help="Mês específico para processar (formato YYYY-MM)."
    ),
    dia: str = typer.Option(
        None, "--dia", help="Dia específico para processar (formato YYYY-MM-DD)."
    ),
    latest: bool = typer.Option(
        False, "--latest", help="Processa o último mês completo."
    ),
):
    """Executa o pipeline de ETL completo (raw -> staging -> marts)."""
    console.print("🚀 Executando o pipeline completo...")

    try:
        # Step 1: Raw Layer - Extract data
        console.print("📥 Etapa 1: Extração (Raw Layer)")

        if latest:
            # Extract data for the last 30 days for latest month
            days = 30
        elif mes:
            # TODO: Calculate days for specific month
            days = 30
        elif dia:
            # Extract for single day
            days = 1
        else:
            # Default: last 7 days
            days = 7

        console.print(f"📅 Extraindo dados dos últimos {days} dias...")

        # Run extraction flow
        result = asyncio.run(
            extract_phase_2a_concurrent(
                date_range_days=days,
                modalidades=settings.HIGH_PRIORITY_MODALIDADES,
                concurrent=True,
            )
        )

        console.print("✅ Etapa 1 concluída: Dados extraídos com sucesso")
        console.print(
            f"📊 Total: {result['total_records']} registros, {result['total_mb']} MB"
        )

        # Step 2: Staging Layer - Transform data
        console.print("🔄 Etapa 2: Transformação (Staging Layer)")

        staging_result = staging_transformation()

        if staging_result["status"] == "success":
            console.print("✅ Etapa 2 concluída: Staging views criadas com sucesso")
            console.print(
                f"📊 Total: {staging_result['total_staging_records']} registros staging"
            )
        else:
            console.print(
                f"❌ Erro na etapa 2: {staging_result.get('error_message', 'Unknown error')}"
            )
            raise typer.Exit(1)

        # Step 3: Marts Layer - Create analytics tables
        console.print("📈 Etapa 3: Marts (Analytics Layer)")

        marts_result = marts_creation()

        if marts_result["status"] == "success":
            console.print("✅ Etapa 3 concluída: Marts criados com sucesso")
            console.print(
                f"📊 Total: {marts_result['total_mart_records']} marts gerados"
            )
        else:
            console.print(
                f"❌ Erro na etapa 3: {marts_result.get('error_message', 'Unknown error')}"
            )
            raise typer.Exit(1)

        console.print("🎉 Pipeline completo executado com sucesso!")
        console.print("✅ Todas as camadas processadas: Raw → Staging → Marts")

    except Exception as e:
        console.print(f"❌ Erro no pipeline: {e}")
        raise typer.Exit(1)


@app.command()
def init():
    """Prepara o ambiente para a primeira execução."""
    console.print("Inicializando o ambiente...")

    try:
        # Initialize database schema
        init_database_schema()
        console.print("✅ Schema do banco de dados inicializado com sucesso")

        # Test connection
        con = connect()
        con.raw_sql("SELECT 1")
        console.print("✅ Conexão com banco de dados testada com sucesso")

        console.print("🎉 Ambiente inicializado com sucesso!")

    except Exception as e:
        console.print(f"❌ Erro na inicialização: {e}")
        raise typer.Exit(1)


@app.command()
def doctor():
    """Checa dependências, permissões e conectividade com a API."""
    console.print("Executando diagnóstico...")
    # TODO: Implement system health checks
    # TODO: Check API connectivity and version compatibility
    # TODO: Validate database schema version
    # TODO: Check available disk space and memory
    # FIXME: This command is currently a stub with no functionality


@app.command()
def extract(
    days: int = typer.Option(
        7, "--days", help="Número de dias para extrair (padrão: 7 dias)"
    ),
    modalidades: str = typer.Option(
        None,
        "--modalidades",
        help="Modalidades específicas separadas por vírgula (ex: 1,2,3)",
    ),
    sequential: bool = typer.Option(
        False, "--sequential", help="Executa em modo sequencial ao invés de concorrente"
    ),
):
    """Executa apenas a etapa de extração (raw)."""
    console.print("🚀 Iniciando extração de dados...")

    try:
        # Parse modalidades if provided
        modalidades_list = None
        if modalidades:
            modalidades_list = [int(m.strip()) for m in modalidades.split(",")]
            console.print(f"📋 Modalidades específicas: {modalidades_list}")
        else:
            modalidades_list = settings.HIGH_PRIORITY_MODALIDADES
            console.print(
                f"📋 Usando modalidades de alta prioridade: {modalidades_list}"
            )

        console.print(f"📅 Extraindo últimos {days} dias")
        console.print(f"⚡ Modo: {'Sequencial' if sequential else 'Concorrente'}")

        # Run extraction flow
        result = asyncio.run(
            extract_phase_2a_concurrent(
                date_range_days=days,
                modalidades=modalidades_list,
                concurrent=not sequential,
            )
        )

        # Display results
        _display_extraction_results(result)

    except Exception as e:
        console.print(f"❌ Erro na extração: {e}")
        raise typer.Exit(1)


@app.command()
def transform(
    mes: str = typer.Option(
        None, "--mes", help="Mês específico para transformar (formato YYYY-MM)."
    ),
    dia: str = typer.Option(
        None, "--dia", help="Dia específico para transformar (formato YYYY-MM-DD)."
    ),
):
    """Executa as etapas de transformação e carga (staging e marts)."""
    console.print("🔄 Executando transformação (Staging + Marts)...")

    try:
        # Step 1: Staging Layer
        console.print("📋 Etapa 1: Criando views de staging...")

        staging_result = staging_transformation()

        if staging_result["status"] == "success":
            console.print("✅ Staging concluído com sucesso")
            console.print(
                f"📊 {staging_result['total_staging_records']} registros processados"
            )
        else:
            console.print(f"❌ Erro no staging: {staging_result.get('error_message')}")
            raise typer.Exit(1)

        # Step 2: Marts Layer
        console.print("📈 Etapa 2: Criando tabelas de marts...")

        marts_result = marts_creation()

        if marts_result["status"] == "success":
            console.print("✅ Marts concluído com sucesso")
            console.print(f"📊 {marts_result['total_mart_records']} marts criados")
        else:
            console.print(f"❌ Erro nos marts: {marts_result.get('error_message')}")
            raise typer.Exit(1)

        console.print("🎉 Transformação completa executada com sucesso!")

    except Exception as e:
        console.print(f"❌ Erro na transformação: {e}")
        raise typer.Exit(1)


@app.command()
def ui():
    """Inicia a interface web do Prefect."""
    console.print("Iniciando a UI do Prefect...")
    # Lógica para iniciar o servidor do Prefect aqui


@app.command()
def query(sql: str):
    """Executa uma consulta SQL diretamente no banco de dados."""
    console.print(f"Executando a query: {sql}")
    # Lógica da query aqui


@app.command()
def dump_catalog():
    """Exporta o esquema das tabelas para um arquivo YAML."""
    console.print("Exportando catálogo...")
    # Lógica de dump_catalog aqui


@app.command()
def reset(
    force: bool = typer.Option(
        False, "--force", help="Força a exclusão sem confirmação."
    ),
):
    """Apaga o banco de dados local para um recomeço limpo."""
    if not force:
        if not typer.confirm(
            "Você tem certeza que deseja apagar o banco de dados? Esta ação não pode ser desfeita."
        ):
            raise typer.Abort()
    console.print("Resetando o banco de dados...")
    # Lógica de reset aqui


@app.command()
def verify(
    range: str = typer.Option(
        None,
        "--range",
        help="Intervalo de datas para verificar (formato YYYY-MM-DD:YYYY-MM-DD).",
    ),
    sample: float = typer.Option(
        None,
        "--sample",
        help="Percentual da amostra para verificar (ex: 0.1 para 10%).",
    ),
    all: bool = typer.Option(False, "--all", help="Verifica todo o histórico."),
):
    """Dispara a rotina de verificação de integridade."""
    console.print("Iniciando verificação de integridade...")
    # Lógica de verify aqui


@app.command()
def fetch_payload(
    sha256_hash: str = typer.Argument(..., help="Hash SHA-256 do payload."),
):
    """Baixa o payload bruto associado a um sha256_payload específico."""
    console.print(f"Buscando payload para hash: {sha256_hash}")
    # Lógica de fetch_payload aqui


def _display_extraction_results(result: dict) -> None:
    """Display extraction results in a formatted table"""

    # Summary table
    summary_table = Table(title="📊 Resumo da Extração")
    summary_table.add_column("Métrica", style="cyan")
    summary_table.add_column("Valor", style="green")

    summary_table.add_row("ID da Execução", result["execution_id"])
    summary_table.add_row("Período", result["date_range"])
    summary_table.add_row("Duração", f"{result['duration_seconds']:.2f}s")
    summary_table.add_row("Total de Requisições", str(result["total_requests"]))
    summary_table.add_row("Total de Registros", str(result["total_records"]))
    summary_table.add_row("Total de Dados", f"{result['total_mb']:.2f} MB")
    summary_table.add_row(
        "Throughput", f"{result['throughput_records_per_second']:.2f} records/s"
    )
    summary_table.add_row(
        "Extrações Bem-sucedidas", str(result["successful_extractions"])
    )
    summary_table.add_row("Extrações Falharam", str(result["failed_extractions"]))

    console.print(summary_table)

    # Results per endpoint
    if result["results"]:
        results_table = Table(title="📋 Resultados por Endpoint")
        results_table.add_column("Endpoint", style="cyan")
        results_table.add_column("Modalidade", style="blue")
        results_table.add_column("Status", style="green")
        results_table.add_column("Registros", style="yellow")
        results_table.add_column("Dados (MB)", style="magenta")
        results_table.add_column("Duração (s)", style="white")

        for res in result["results"]:
            status = "✅ OK" if res.success else "❌ ERRO"
            modalidade = str(res.modalidade) if res.modalidade else "-"
            mb = (
                f"{res.total_bytes / 1024 / 1024:.2f}"
                if res.total_bytes > 0
                else "0.00"
            )

            results_table.add_row(
                res.endpoint,
                modalidade,
                status,
                str(res.total_records),
                mb,
                f"{res.duration_seconds:.2f}",
            )

        console.print(results_table)

    # Success message
    if result["failed_extractions"] == 0:
        console.print("🎉 Extração concluída com sucesso!")
    else:
        console.print(
            f"⚠️  Extração concluída com {result['failed_extractions']} falhas"
        )


if __name__ == "__main__":
    app()
