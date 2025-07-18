"""
Handles the 'load' command by exporting data to Parquet and uploading to the Internet Archive.
"""

from pathlib import Path

import duckdb
import typer
from internetarchive import get_item, upload

from .config import settings

DATA_DIR = Path("data")
PARQUET_DIR = DATA_DIR / "parquet"


def export_to_parquet():
    """
    Exports the gold tables from DuckDB to Parquet files.
    """
    PARQUET_DIR.mkdir(exist_ok=True)
    conn = duckdb.connect(database=str(DATA_DIR / "baliza.duckdb"), read_only=True)
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'"
    ).fetchall()
    for table in tables:
        table_name = table[0]
        typer.echo(f"Exporting table: {table_name}")
        df = conn.table(f"gold.{table_name}").df()
        df.to_parquet(PARQUET_DIR / f"{table_name}.parquet")
    conn.close()


def upload_to_internet_archive():
    """
    Uploads the Parquet files to the Internet Archive.
    """
    identifier = settings.internet_archive_identifier
    typer.echo(f"Uploading to Internet Archive item: {identifier}")
    item = get_item(identifier)
    upload(
        identifier,
        files=[str(f) for f in PARQUET_DIR.glob("*.parquet")],
        metadata=dict(
            title=f"BALIZA - Dados de Contratações Públicas do Brasil - {item.metadata['updatedate']}",
            description="Backup Aberto de Licitações Zelando pelo Acesso (BALIZA) - Dados extraídos do Portal Nacional de Contratações Públicas (PNCP) e transformados em formato analítico.",
            creator="BALIZA",
            subject=["Brazil", "government procurement", "public contracts", "PNCP"],
        ),
        access_key=settings.ia_access_key,
        secret_key=settings.ia_secret_key,
    )


def load():
    """
    Exports data to Parquet and uploads to the Internet Archive.
    """
    export_to_parquet()
    upload_to_internet_archive()
