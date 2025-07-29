"""
Command-line interface for the Baliza pipeline.
"""

import typer
from .pipeline import run_backfill_pipeline, run_transformation_pipeline

app = typer.Typer()

@app.command()
def backfill(
    start_date: str = typer.Option(..., help="Data de início (YYYYMMDD)"),
    end_date: str = typer.Option(..., help="Data de fim (YYYYMMDD)"),
    chunk_days: int = typer.Option(30, help="Número de dias por fatia de extração"),
):
    """
    Executa o backfill histórico dos dados do PNCP.
    """
    typer.echo(f"🚀 Iniciando backfill de {start_date} a {end_date} em fatias de {chunk_days} dias...")
    try:
        run_backfill_pipeline(start_date, end_date, chunk_days)
        typer.echo("✅ Backfill concluído.")
        typer.echo("🚀 Executando transformações pós-carregamento...")
        run_transformation_pipeline()
        typer.echo("✅ Transformações concluídas! As visualizações unificadas estão atualizadas.")
    except Exception as e:
        typer.echo(f"❌ Erro no backfill: {e}")
        raise typer.Exit(code=1)

@app.command()
def transform():
    """
    Executa apenas a etapa de transformação (criação de visualizações).
    """
    typer.echo("🚀 Executando transformações pós-carregamento...")
    try:
        run_transformation_pipeline()
        typer.echo("✅ Transformações concluídas! As visualizações unificadas estão atualizadas.")
    except Exception as e:
        typer.echo(f"❌ Erro na transformação: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
