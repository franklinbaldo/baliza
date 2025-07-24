import asyncio
import os
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .backend import init_database_schema, connect
from .flows.raw import extract_phase_2a_concurrent
from .flows.staging import staging_transformation
from .flows.marts import marts_creation
from .flows.complete_extraction import extract_all_pncp_endpoints
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
            # TODO: Calculate actual days for specific month (handle different month lengths)
            # FIXME: Currently hardcoded to 30 days regardless of actual month
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
    console.print("🏥 [bold blue]Executando diagnóstico do sistema Baliza...[/bold blue]")
    console.print()
    
    # Run comprehensive health checks
    try:
        asyncio.run(_run_health_checks())
    except Exception as e:
        console.print(f"❌ Erro durante diagnóstico: {e}")
        raise typer.Exit(1)


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
def extract_all(
    days: int = typer.Option(
        7, "--days", help="Número de dias para extrair (padrão: 7 dias)"
    ),
    modalidades: str = typer.Option(
        None,
        "--modalidades",
        help="Modalidades específicas separadas por vírgula (ex: 1,2,3)",
    ),
    include_pca: bool = typer.Option(
        False, "--include-pca", help="Incluir endpoints de PCA (Plano de Contratações)"
    ),
    sequential: bool = typer.Option(
        False, "--sequential", help="Executa em modo sequencial ao invés de concorrente"
    ),
):
    """Executa extração de TODOS os endpoints PNCP (100% de cobertura)."""
    console.print("🚀 Iniciando extração COMPLETA do PNCP (100% cobertura)...")

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
        console.print(f"📊 Incluir PCA: {'Sim' if include_pca else 'Não'}")
        console.print(f"⚡ Modo: {'Sequencial' if sequential else 'Concorrente'}")

        # Run complete extraction flow
        result = asyncio.run(
            extract_all_pncp_endpoints(
                date_range_days=days,
                modalidades=modalidades_list,
                include_pca=include_pca,
                concurrent=not sequential,
            )
        )

        # Display comprehensive results
        _display_complete_extraction_results(result)

    except Exception as e:
        console.print(f"❌ Erro na extração completa: {e}")
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
    # TODO: Import and start Prefect server subprocess
    # TODO: Check if Prefect server is already running (port 4200)
    # TODO: Open browser automatically to http://localhost:4200
    # TODO: Handle server startup errors and port conflicts
    # FIXME: This command is currently a stub with no functionality


@app.command()
def query(sql: str):
    """Executa uma consulta SQL diretamente no banco de dados."""
    console.print(f"Executando a query: {sql}")
    # TODO: Connect to DuckDB using backend.connect()
    # TODO: Execute SQL query and handle errors gracefully
    # TODO: Format results in a readable table using Rich
    # TODO: Support for parameterized queries to prevent SQL injection
    # TODO: Add query timeout and memory limits
    # TODO: Support for exporting query results to CSV/JSON
    # FIXME: This command is currently a stub with no functionality


@app.command()
def dump_catalog():
    """Exporta o esquema das tabelas para um arquivo YAML."""
    console.print("Exportando catálogo...")
    # TODO: Query DuckDB information_schema for all tables and views
    # TODO: Export schema definitions including column types and constraints
    # TODO: Include table row counts and approximate sizes
    # TODO: Generate YAML format with proper structure
    # TODO: Save to data/catalog.yml with timestamp
    # TODO: Include data lineage information (raw -> staging -> marts)
    # FIXME: This command is currently a stub with no functionality


@app.command()
def reset(
    force: bool = typer.Option(
        False, "--force", help="Força a exclusão sem confirmação."
    ),
    backup: bool = typer.Option(
        False, "--backup", help="Cria backup antes de resetar."
    ),
):
    """Apaga o banco de dados local para um recomeço limpo."""
    console.print("🗑️ [bold red]Preparando para resetar o banco de dados Baliza...[/bold red]")
    console.print()
    
    try:
        _perform_database_reset(force, backup)
    except Exception as e:
        console.print(f"❌ Erro durante reset: {e}")
        raise typer.Exit(1)


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
    # TODO: Implement data integrity verification flow
    # TODO: Re-fetch sample of stored payloads from PNCP API
    # TODO: Compare SHA-256 hashes to detect data drift/corruption
    # TODO: Check for missing dates or gaps in raw.api_requests
    # TODO: Validate that all referenced payload hashes exist in storage
    # TODO: Generate integrity report with divergences found
    # TODO: Log verification results to meta.divergence_log
    # FIXME: This command is currently a stub with no functionality


@app.command()
def fetch_payload(
    sha256_hash: str = typer.Argument(..., help="Hash SHA-256 do payload."),
):
    """Baixa o payload bruto associado a um sha256_payload específico."""
    console.print(f"Buscando payload para hash: {sha256_hash}")
    # TODO: Validate SHA-256 hash format (64 hex characters)
    # TODO: Query raw.hot_payloads table for matching hash
    # TODO: Decompress payload if found and display as formatted JSON
    # TODO: Handle case where hash is not found in local storage
    # TODO: Offer option to save payload to file
    # TODO: Show metadata (original API endpoint, collection date, etc.)
    # FIXME: This command is currently a stub with no functionality


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


def _display_complete_extraction_results(result: dict) -> None:
    """Display complete extraction results with coverage metrics"""

    # Coverage summary table
    coverage_table = Table(title="📊 Cobertura de Endpoints PNCP")
    coverage_table.add_column("Métrica", style="cyan")
    coverage_table.add_column("Valor", style="green")

    coverage_table.add_row("ID da Execução", result["execution_id"])
    coverage_table.add_row("Período", result["date_range"])
    coverage_table.add_row("Duração", f"{result['duration_seconds']:.2f}s")
    coverage_table.add_row("Endpoints Extraídos", str(result["unique_endpoints_extracted"]))
    coverage_table.add_row("Endpoints Disponíveis", str(result["total_endpoints_available"]))
    coverage_table.add_row("Cobertura PNCP", f"{result['coverage_percentage']}%")
    coverage_table.add_row("Total de Requisições", str(result["total_requests"]))
    coverage_table.add_row("Total de Registros", str(result["total_records"]))
    coverage_table.add_row("Total de Dados", f"{result['total_mb']:.2f} MB")
    coverage_table.add_row(
        "Throughput", f"{result['throughput_records_per_second']:.2f} records/s"
    )
    coverage_table.add_row(
        "Extrações Bem-sucedidas", str(result["successful_extractions"])
    )
    coverage_table.add_row("Extrações Falharam", str(result["failed_extractions"]))

    console.print(coverage_table)

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

    # Coverage achievement message
    if result["coverage_percentage"] == 100.0:
        console.print("🎉 COBERTURA COMPLETA ATINGIDA! Todos os endpoints PNCP foram extraídos!")
    elif result["coverage_percentage"] >= 90.0:
        console.print(f"🎯 Excelente cobertura! {result['coverage_percentage']}% dos endpoints extraídos")
    elif result["failed_extractions"] == 0:
        console.print("🎉 Extração completa sem falhas!")
    else:
        console.print(
            f"⚠️  Extração completa com {result['failed_extractions']} falhas"
        )


def _perform_database_reset(force: bool, backup: bool):
    """Perform database reset with safety checks and optional backup"""
    import shutil
    import time
    from datetime import datetime
    
    db_path = Path(settings.DATABASE_PATH)
    db_dir = db_path.parent
    
    # Check if database exists
    if not db_path.exists():
        console.print("ℹ️ Banco de dados não existe. Nada para resetar.")
        return
    
    # Get database size for reporting
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    
    # Show what will be deleted
    console.print("📋 [yellow]Análise do que será removido:[/yellow]")
    items_to_delete = []
    
    if db_path.exists():
        items_to_delete.append(f"• Banco de dados: {db_path} ({db_size_mb:.2f} MB)")
    
    # Check for temporary files
    temp_patterns = ["*.tmp", "*.log", "*.cache"]
    temp_files = []
    for pattern in temp_patterns:
        temp_files.extend(db_dir.glob(pattern))
    
    if temp_files:
        total_temp_mb = sum(f.stat().st_size for f in temp_files) / (1024 * 1024)
        items_to_delete.append(f"• {len(temp_files)} arquivos temporários ({total_temp_mb:.2f} MB)")
    
    # Check for DuckDB WAL files
    wal_files = list(db_dir.glob("*.wal"))
    if wal_files:
        wal_mb = sum(f.stat().st_size for f in wal_files) / (1024 * 1024)
        items_to_delete.append(f"• {len(wal_files)} arquivos WAL ({wal_mb:.2f} MB)")
    
    for item in items_to_delete:
        console.print(item)
    
    console.print()
    
    # Create backup if requested
    if backup:
        backup_name = f"baliza_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
        backup_path = db_dir / backup_name
        
        console.print(f"💾 Criando backup em: {backup_path}")
        try:
            shutil.copy2(db_path, backup_path)
            backup_size_mb = backup_path.stat().st_size / (1024 * 1024)
            console.print(f"✅ Backup criado com sucesso ({backup_size_mb:.2f} MB)")
        except Exception as e:
            console.print(f"❌ Erro ao criar backup: {e}")
            if not force:
                if not typer.confirm("Continuar sem backup?"):
                    raise typer.Abort()
        console.print()
    
    # Confirmation check
    if not force:
        console.print("⚠️ [bold yellow]ATENÇÃO: Esta operação irá:")
        console.print("  • Apagar TODOS os dados extraídos da API PNCP")
        console.print("  • Remover TODAS as transformações e marts")
        console.print("  • Limpar TODOS os logs e métricas")
        console.print("  • Esta ação NÃO PODE ser desfeita!")
        console.print()
        
        if not typer.confirm("Você tem CERTEZA que deseja continuar?"):
            console.print("🛑 Operação cancelada pelo usuário.")
            raise typer.Abort()
        
        # Double confirmation for safety
        console.print()
        confirmation_text = "RESETAR TUDO"
        user_input = typer.prompt(
            f"Digite '{confirmation_text}' para confirmar",
            type=str
        )
        
        if user_input != confirmation_text:
            console.print("❌ Confirmação incorreta. Operação cancelada.")
            raise typer.Abort()
    
    console.print()
    console.print("🚀 Iniciando reset do banco de dados...")
    
    # Close any active connections first
    try:
        # Import here to avoid circular imports
        import gc
        gc.collect()  # Force garbage collection to close connections
        time.sleep(0.5)  # Give time for connections to close
    except:
        pass
    
    deleted_files = []
    total_deleted_mb = 0
    
    # Delete main database
    if db_path.exists():
        try:
            size_mb = db_path.stat().st_size / (1024 * 1024)
            db_path.unlink()
            deleted_files.append(f"Database ({size_mb:.2f} MB)")
            total_deleted_mb += size_mb
            console.print("✅ Banco de dados principal removido")
        except Exception as e:
            console.print(f"❌ Erro ao remover banco: {e}")
            raise
    
    # Delete WAL files
    for wal_file in wal_files:
        try:
            size_mb = wal_file.stat().st_size / (1024 * 1024)
            wal_file.unlink()
            deleted_files.append(f"WAL file ({size_mb:.2f} MB)")
            total_deleted_mb += size_mb
        except Exception as e:
            console.print(f"⚠️ Erro ao remover {wal_file}: {e}")
    
    # Delete temporary files
    for temp_file in temp_files:
        try:
            size_mb = temp_file.stat().st_size / (1024 * 1024)
            temp_file.unlink()
            deleted_files.append(f"Temp file ({size_mb:.2f} MB)")
            total_deleted_mb += size_mb
        except Exception as e:
            console.print(f"⚠️ Erro ao remover {temp_file}: {e}")
    
    console.print(f"✅ Arquivos temporários removidos ({len(temp_files)} arquivos)")
    
    # Summary
    console.print()
    console.print("📊 [bold green]Reset completado com sucesso![/bold green]")
    console.print(f"🗑️ Total removido: {total_deleted_mb:.2f} MB")
    console.print(f"📁 Arquivos removidos: {len(deleted_files)}")
    
    if backup:
        console.print(f"💾 Backup disponível em: {backup_path}")
    
    console.print()
    console.print("💡 [blue]Execute 'baliza init' para inicializar um novo banco de dados.[/blue]")


async def _run_health_checks():
    """Run comprehensive system health checks"""
    import psutil
    import httpx
    from rich.progress import Progress
    
    checks = [
        ("💾 Database Connection", _check_database_connectivity),
        ("🌐 PNCP API Accessibility", _check_pncp_api_health),
        ("💿 Disk Space", _check_available_disk_space),
        ("🐏 Memory Usage", _check_memory_utilization),
        ("📊 Schema Version", _validate_database_schema),
        ("🔧 Dependencies", _check_python_dependencies),
        ("📁 File Permissions", _check_file_permissions),
        ("🔗 Network Connectivity", _check_network_connectivity),
    ]
    
    passed = 0
    total = len(checks)
    issues = []
    
    # Create health check table
    table = Table(title="🔍 Diagnóstico do Sistema")
    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold", no_wrap=True)
    table.add_column("Details", style="white")
    
    with Progress() as progress:
        task = progress.add_task("Executando checks...", total=total)
        
        for check_name, check_func in checks:
            try:
                status, details = await check_func()
                if status:
                    table.add_row(check_name, "✅ PASS", details)
                    passed += 1
                else:
                    table.add_row(check_name, "❌ FAIL", details)
                    issues.append(f"{check_name}: {details}")
            except Exception as e:
                table.add_row(check_name, "⚠️ ERROR", str(e))
                issues.append(f"{check_name}: Error - {e}")
            
            progress.advance(task)
    
    console.print(table)
    console.print()
    
    # Summary
    if passed == total:
        console.print("🎉 [bold green]Todos os checks passaram! Sistema está saudável.[/bold green]")
    else:
        console.print(f"⚠️ [bold yellow]{passed}/{total} checks passaram. Problemas encontrados:[/bold yellow]")
        for issue in issues:
            console.print(f"  • {issue}")
        console.print()
        console.print("💡 [blue]Execute 'baliza init' se o banco de dados não estiver configurado.[/blue]")


async def _check_database_connectivity():
    """Check database connection and basic operations"""
    try:
        con = connect()
        result = con.raw_sql("SELECT 1 as test").fetchone()
        if result and result[0] == 1:
            return True, "Conexão estabelecida com sucesso"
        return False, "Conexão falhou - resposta inválida"
    except Exception as e:
        return False, f"Erro de conexão: {str(e)}"


async def _check_pncp_api_health():
    """Check PNCP API connectivity and response"""
    try:
        import httpx
        timeout = httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Try a simple API call
            test_url = f"{settings.PNCP_API_BASE_URL}/contratacoes-publicacao"
            params = {
                "dataInicial": "2024-01-01",
                "dataFinal": "2024-01-01",
                "pagina": 1,
                "tamanhoPagina": 1
            }
            response = await client.get(test_url, params=params)
            
            if response.status_code == 200:
                return True, f"API respondendo (status: {response.status_code})"
            else:
                return False, f"API retornou status {response.status_code}"
    except httpx.TimeoutException:
        return False, "Timeout na conexão com a API"
    except Exception as e:
        return False, f"Erro na API: {str(e)}"


async def _check_available_disk_space():
    """Check available disk space for database operations"""
    try:
        import psutil
        
        # Check disk space where database is located
        db_path = Path(settings.DATABASE_PATH).parent
        disk_usage = psutil.disk_usage(str(db_path))
        
        free_gb = disk_usage.free / (1024**3)
        total_gb = disk_usage.total / (1024**3)
        used_percent = (disk_usage.used / disk_usage.total) * 100
        
        if free_gb < 1.0:  # Less than 1GB free
            return False, f"Pouco espaço: {free_gb:.1f}GB livre ({used_percent:.1f}% usado)"
        elif free_gb < 5.0:  # Less than 5GB free
            return True, f"Atenção: {free_gb:.1f}GB livre de {total_gb:.1f}GB ({used_percent:.1f}% usado)"
        else:
            return True, f"{free_gb:.1f}GB livre de {total_gb:.1f}GB ({used_percent:.1f}% usado)"
    except Exception as e:
        return False, f"Erro ao verificar disco: {str(e)}"


async def _check_memory_utilization():
    """Check system memory usage"""
    try:
        import psutil
        
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        total_gb = memory.total / (1024**3)
        used_percent = memory.percent
        
        if available_gb < 0.5:  # Less than 500MB available
            return False, f"Pouca memória: {available_gb:.1f}GB livre ({used_percent:.1f}% usado)"
        elif available_gb < 2.0:  # Less than 2GB available
            return True, f"Atenção: {available_gb:.1f}GB livre de {total_gb:.1f}GB ({used_percent:.1f}% usado)"
        else:
            return True, f"{available_gb:.1f}GB livre de {total_gb:.1f}GB ({used_percent:.1f}% usado)"
    except Exception as e:
        return False, f"Erro ao verificar memória: {str(e)}"


async def _validate_database_schema():
    """Validate database schema version and required tables"""
    try:
        con = connect()
        
        # Check if required tables exist
        required_tables = [
            "raw.api_requests",
            "meta.metrics_log"
        ]
        
        existing_tables = []
        missing_tables = []
        
        for table in required_tables:
            try:
                result = con.raw_sql(f"SELECT COUNT(*) FROM {table}").fetchone()
                existing_tables.append(table)
            except:
                missing_tables.append(table)
        
        if missing_tables:
            return False, f"Tabelas ausentes: {', '.join(missing_tables)}"
        else:
            return True, f"Todas as tabelas encontradas: {', '.join(existing_tables)}"
    except Exception as e:
        return False, f"Erro de schema: {str(e)}"


async def _check_python_dependencies():
    """Check if required Python packages are available"""
    try:
        required_packages = [
            "prefect", "ibis", "duckdb", "pydantic", 
            "httpx", "typer", "rich", "psutil"
        ]
        
        missing = []
        available = []
        
        for package in required_packages:
            try:
                __import__(package)
                available.append(package)
            except ImportError:
                missing.append(package)
        
        if missing:
            return False, f"Pacotes ausentes: {', '.join(missing)}"
        else:
            return True, f"Todos os pacotes disponíveis ({len(available)} pacotes)"
    except Exception as e:
        return False, f"Erro ao verificar dependências: {str(e)}"


async def _check_file_permissions():
    """Check file system permissions for database and temp directories"""
    try:
        # Check database directory permissions
        db_path = Path(settings.DATABASE_PATH)
        db_dir = db_path.parent
        
        issues = []
        
        # Check if database directory exists and is writable
        if not db_dir.exists():
            try:
                db_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                issues.append(f"Não pode criar diretório DB: {e}")
        
        if not os.access(str(db_dir), os.W_OK):
            issues.append("Diretório DB não é gravável")
        
        # Check temp directory access
        temp_dir = Path("/tmp")
        if not os.access(str(temp_dir), os.W_OK):
            issues.append("Diretório /tmp não é gravável")
        
        if issues:
            return False, "; ".join(issues)
        else:
            return True, "Permissões adequadas para DB e temp"
    except Exception as e:
        return False, f"Erro ao verificar permissões: {str(e)}"


async def _check_network_connectivity():
    """Check basic internet connectivity"""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Test with a reliable public service
            response = await client.get("https://httpbin.org/status/200")
            if response.status_code == 200:
                return True, "Conectividade de rede OK"
            else:
                return False, f"Teste de rede falhou: {response.status_code}"
    except Exception as e:
        return False, f"Sem conectividade: {str(e)}"


if __name__ == "__main__":
    app()
