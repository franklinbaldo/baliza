"""Daily Parquet exporter for Internet Archive uploads.

Exports self-contained daily packages with relational structure:
- contratos.parquet (main fact table)
- orgaos.parquet (organization dimension)
- unidades.parquet (unit dimension)
- _metadata.json (schema version and stats)
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from .utils import validate_identifier

console = Console()

SCHEMA_VERSION = "1.0.0"

# PyArrow schemas matching docs/PARQUET_SCHEMA.md
CONTRATOS_SCHEMA = pa.schema([
    pa.field("numero_controle_pncp", pa.string(), nullable=False),
    pa.field("orgao_cnpj", pa.string(), nullable=False),
    pa.field("unidade_codigo", pa.string(), nullable=True),
    pa.field("ano_compra", pa.int32(), nullable=True),
    pa.field("sequencial_compra", pa.int32(), nullable=True),
    pa.field("numero_processo", pa.string(), nullable=True),
    pa.field("modalidade_id", pa.int32(), nullable=True),
    pa.field("modalidade_nome", pa.string(), nullable=True),
    pa.field("valor_inicial", pa.float64(), nullable=True),
    pa.field("objeto_contrato", pa.string(), nullable=True),
    pa.field("informacao_complementar", pa.string(), nullable=True),
    pa.field("link_sistema_origem", pa.string(), nullable=True),
    pa.field("data_publicacao", pa.date32(), nullable=True),
    pa.field("data_vigencia_inicio", pa.date32(), nullable=True),
    pa.field("data_vigencia_fim", pa.date32(), nullable=True),
    pa.field("data_inclusao", pa.timestamp("us"), nullable=True),
    pa.field("data_atualizacao", pa.timestamp("us"), nullable=True),
    pa.field("usuario_nome", pa.string(), nullable=True),
    pa.field("data_particao", pa.date32(), nullable=False),
])

ORGAOS_SCHEMA = pa.schema([
    pa.field("cnpj", pa.string(), nullable=False),
    pa.field("razao_social", pa.string(), nullable=True),
    pa.field("poder_id", pa.string(), nullable=True),
    pa.field("esfera", pa.string(), nullable=True),
    pa.field("uf", pa.string(), nullable=True),
    pa.field("contratos_no_dia", pa.int32(), nullable=True),
    pa.field("valor_total_no_dia", pa.float64(), nullable=True),
])

UNIDADES_SCHEMA = pa.schema([
    pa.field("codigo", pa.string(), nullable=False),
    pa.field("orgao_cnpj", pa.string(), nullable=False),
    pa.field("nome", pa.string(), nullable=True),
    pa.field("contratos_no_dia", pa.int32(), nullable=True),
])


class DailyExporter:
    """Exports daily self-contained parquet packages."""

    def __init__(
        self,
        db_path: Path,
        dataset: str = "baliza_raw",
    ):
        self.db_path = db_path
        self.dataset = validate_identifier(dataset)

    def export(
        self,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export a single day's data to parquet files.

        Args:
            target_date: The date to export
            output_dir: Base output directory (will create date subdirectory)

        Returns:
            Dict with export stats
        """
        # Create date-specific directory
        date_str = target_date.isoformat()
        day_dir = output_dir / date_str
        day_dir.mkdir(parents=True, exist_ok=True)

        stats: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "data_particao": date_str,
            "extracted_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "source": "PNCP API v1",
            "tables": {},
            "baliza_version": "0.1.0",
        }

        with duckdb.connect(str(self.db_path), read_only=True) as con:
            # Export contratos
            contratos_stats = self._export_contratos(con, target_date, day_dir)
            stats["tables"]["contratos"] = contratos_stats

            # Export orgaos (deduplicated for this day)
            orgaos_stats = self._export_orgaos(con, target_date, day_dir)
            stats["tables"]["orgaos"] = orgaos_stats

            # Export unidades (deduplicated for this day)
            unidades_stats = self._export_unidades(con, target_date, day_dir)
            stats["tables"]["unidades"] = unidades_stats

        # Write metadata
        metadata_path = day_dir / "_metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(stats, f, indent=2)

        total_rows = sum(t.get("row_count", 0) for t in stats["tables"].values())
        console.print(f"[green]✓ Exported {date_str}: {total_rows} total rows")

        return stats

    def _export_contratos(
        self,
        con: duckdb.DuckDBPyConnection,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export contratos table for a specific date."""
        next_day = target_date + timedelta(days=1)
        # Query with column renaming to snake_case
        # Optimization: Use fetch_record_batch() for O(1) memory usage
        # Note: fetch_record_batch() returns a RecordBatchReader in this version
        reader = con.execute(
            f"""
            SELECT
                numeroControlePNCP as numero_controle_pncp,
                orgaoEntidade_cnpj as orgao_cnpj,
                unidadeOrgao_codigoUnidade as unidade_codigo,
                CAST(anoCompra AS INTEGER) as ano_compra,
                CAST(sequencialCompra AS INTEGER) as sequencial_compra,
                numeroProcesso as numero_processo,
                CAST(modalidadeId AS INTEGER) as modalidade_id,
                modalidadeNome as modalidade_nome,
                valorInicial as valor_inicial,
                objetoContrato as objeto_contrato,
                informacaoComplementar as informacao_complementar,
                linkSistemaOrigem as link_sistema_origem,
                CAST(dataPublicacao AS DATE) as data_publicacao,
                CAST(dataVigenciaInicio AS DATE) as data_vigencia_inicio,
                CAST(dataVigenciaFim AS DATE) as data_vigencia_fim,
                dataInclusao as data_inclusao,
                dataAtualizacao as data_atualizacao,
                usuarioNome as usuario_nome,
                CAST(? AS DATE) as data_particao
            FROM {self.dataset}.contratos
            WHERE dataPublicacao >= ? AND dataPublicacao < ?
            ORDER BY numero_controle_pncp
        """,
            [target_date, target_date, next_day],
        ).fetch_record_batch(rows_per_batch=10000)

        output_path = output_dir / "contratos.parquet"
        file_size, row_count = self._write_parquet(reader, output_path, CONTRATOS_SCHEMA)

        return {
            "row_count": row_count,
            "file_size_bytes": file_size,
        }

    def _export_orgaos(
        self,
        con: duckdb.DuckDBPyConnection,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export deduplicated orgaos for a specific date."""
        next_day = target_date + timedelta(days=1)
        reader = con.execute(
            f"""
            SELECT
                orgaoEntidade_cnpj as cnpj,
                MAX(orgaoEntidade_razaoSocial) as razao_social,
                MAX(orgaoEntidade_poderId) as poder_id,
                NULL as esfera,
                NULL as uf,
                CAST(COUNT(*) AS INTEGER) as contratos_no_dia,
                SUM(valorInicial) as valor_total_no_dia
            FROM {self.dataset}.contratos
            WHERE dataPublicacao >= ? AND dataPublicacao < ?
            GROUP BY orgaoEntidade_cnpj
            ORDER BY cnpj
        """,
            [target_date, next_day],
        ).fetch_record_batch(rows_per_batch=10000)

        output_path = output_dir / "orgaos.parquet"
        file_size, row_count = self._write_parquet(reader, output_path, ORGAOS_SCHEMA)

        return {
            "row_count": row_count,
            "file_size_bytes": file_size,
        }

    def _export_unidades(
        self,
        con: duckdb.DuckDBPyConnection,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export deduplicated unidades for a specific date."""
        next_day = target_date + timedelta(days=1)
        reader = con.execute(
            f"""
            SELECT
                unidadeOrgao_codigoUnidade as codigo,
                orgaoEntidade_cnpj as orgao_cnpj,
                MAX(unidadeOrgao_nomeUnidade) as nome,
                CAST(COUNT(*) AS INTEGER) as contratos_no_dia
            FROM {self.dataset}.contratos
            WHERE dataPublicacao >= ? AND dataPublicacao < ?
              AND unidadeOrgao_codigoUnidade IS NOT NULL
            GROUP BY unidadeOrgao_codigoUnidade, orgaoEntidade_cnpj
            ORDER BY codigo
        """,
            [target_date, next_day],
        ).fetch_record_batch(rows_per_batch=10000)

        output_path = output_dir / "unidades.parquet"
        file_size, row_count = self._write_parquet(reader, output_path, UNIDADES_SCHEMA)

        return {
            "row_count": row_count,
            "file_size_bytes": file_size,
        }

    def _write_parquet(
        self,
        data: pa.Table | pa.RecordBatchReader,
        path: Path,
        schema: pa.Schema,
    ) -> tuple[int, int]:
        """Write PyArrow table or reader to parquet with standard settings.

        Args:
            data: Arrow Table or RecordBatchReader to write
            path: Output file path
            schema: Target schema

        Returns:
            Tuple of (file_size_bytes, row_count)
        """
        row_count = 0

        # Configure Parquet Writer
        with pq.ParquetWriter(
            path,
            schema=schema,
            compression="zstd",
            compression_level=3,
            write_statistics=True,
            version="2.6",
        ) as writer:

            if isinstance(data, pa.Table):
                # Legacy path for Table
                try:
                    table = data.cast(schema)
                except pa.ArrowInvalid:
                    table = data
                writer.write_table(table)
                row_count = table.num_rows
            else:
                # Streaming path for RecordBatchReader
                # Reduces memory usage from O(N) to O(1) (batch size)
                for batch in data:
                    # Cast batch to expected schema for safety
                    # We convert batch to table to perform cast, which is lightweight
                    # for small batches (~2048 rows)
                    try:
                        t = pa.Table.from_batches([batch])
                        t = t.cast(schema)
                        writer.write_table(t)
                        row_count += t.num_rows
                    except Exception:
                        # Fallback if cast fails (though queries should align)
                        writer.write_batch(batch)
                        row_count += batch.num_rows

        return path.stat().st_size, row_count
