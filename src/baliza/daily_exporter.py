"""Daily Parquet exporter for Internet Archive uploads.

Exports self-contained daily packages with relational structure:
- contratos.parquet (main fact table — full RecuperarContratoDTO)
- orgaos.parquet (agency dimension)
- unidades.parquet (administrative unit dimension)
- fornecedores.parquet (supplier dimension — the WHO got paid)
- _metadata.json (schema version and stats)
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from rich.console import Console

from .utils import PARQUET_ROW_GROUP_SIZE, validate_identifier, write_optimized_parquet

console = Console()

SCHEMA_VERSION = "2.0.0"

# ── Parquet schemas ───────────────────────────────────────────────────────────
# All column names are snake_case matching the normalized DuckDB schema.

CONTRATOS_SCHEMA = pa.schema(
    [
        # Identity
        pa.field("numero_controle_pncp", pa.string(), nullable=False),
        pa.field("numero_controle_pncp_compra", pa.string(), nullable=True),
        pa.field("ano_contrato", pa.int32(), nullable=True),
        pa.field("sequencial_contrato", pa.int32(), nullable=True),
        pa.field("ano_compra", pa.int32(), nullable=True),
        pa.field("sequencial_compra", pa.int32(), nullable=True),
        pa.field("numero_contrato_empenho", pa.string(), nullable=True),
        pa.field("numero_retificacao", pa.int32(), nullable=True),
        pa.field("processo", pa.string(), nullable=True),
        # Órgão contratante
        pa.field("cnpj_orgao", pa.string(), nullable=False),
        pa.field("razao_social_orgao", pa.string(), nullable=True),
        pa.field("poder_id", pa.string(), nullable=True),  # E/L/J
        pa.field("esfera_id", pa.string(), nullable=True),  # F/E/M/D
        pa.field("codigo_unidade", pa.string(), nullable=True),
        pa.field("nome_unidade", pa.string(), nullable=True),
        pa.field("uf_sigla", pa.string(), nullable=True),
        pa.field("uf_nome", pa.string(), nullable=True),
        pa.field("municipio_nome", pa.string(), nullable=True),
        pa.field("codigo_ibge", pa.string(), nullable=True),
        # Órgão sub-rogado (proxy agency — nullable)
        pa.field("cnpj_orgao_subrogado", pa.string(), nullable=True),
        pa.field("razao_social_orgao_subrogado", pa.string(), nullable=True),
        pa.field("codigo_unidade_subrogada", pa.string(), nullable=True),
        pa.field("nome_unidade_subrogada", pa.string(), nullable=True),
        pa.field("uf_sigla_subrogada", pa.string(), nullable=True),
        # Fornecedor (supplier — the WHO got paid)
        pa.field("ni_fornecedor", pa.string(), nullable=True),
        pa.field("tipo_pessoa", pa.string(), nullable=True),  # PJ/PF/PE
        pa.field("nome_razao_social_fornecedor", pa.string(), nullable=True),
        pa.field("codigo_pais_fornecedor", pa.string(), nullable=True),
        pa.field("ni_fornecedor_subcontratado", pa.string(), nullable=True),
        pa.field("nome_fornecedor_subcontratado", pa.string(), nullable=True),
        pa.field("tipo_pessoa_subcontratada", pa.string(), nullable=True),
        # Classification
        pa.field("modalidade_id", pa.int32(), nullable=True),
        pa.field("modalidade_nome", pa.string(), nullable=True),
        pa.field("tipo_contrato_id", pa.int32(), nullable=True),
        pa.field("tipo_contrato_nome", pa.string(), nullable=True),
        pa.field("categoria_processo_id", pa.int32(), nullable=True),
        pa.field("categoria_processo_nome", pa.string(), nullable=True),
        pa.field("receita", pa.bool_(), nullable=True),
        # Financial
        pa.field("valor_inicial", pa.float64(), nullable=True),
        pa.field("valor_parcela", pa.float64(), nullable=True),
        pa.field("valor_global", pa.float64(), nullable=True),
        pa.field("valor_acumulado", pa.float64(), nullable=True),
        pa.field("numero_parcelas", pa.int32(), nullable=True),
        # Dates
        pa.field("data_publicacao", pa.timestamp("us"), nullable=True),
        pa.field("data_publicacao_pncp", pa.timestamp("us"), nullable=True),
        pa.field("data_assinatura", pa.date32(), nullable=True),
        pa.field("data_vigencia_inicio", pa.date32(), nullable=True),
        pa.field("data_vigencia_fim", pa.date32(), nullable=True),
        pa.field("data_inclusao", pa.timestamp("us"), nullable=True),
        pa.field("data_atualizacao", pa.timestamp("us"), nullable=True),
        pa.field("data_atualizacao_global", pa.timestamp("us"), nullable=True),
        # Text / links
        pa.field("objeto_contrato", pa.string(), nullable=True),
        pa.field("informacao_complementar", pa.string(), nullable=True),
        pa.field("link_sistema_origem", pa.string(), nullable=True),
        pa.field("identificador_cipi", pa.string(), nullable=True),
        pa.field("url_cipi", pa.string(), nullable=True),
        # Audit
        pa.field("usuario_nome", pa.string(), nullable=True),
        # Pipeline metadata (not from API)
        pa.field("data_particao", pa.date32(), nullable=False),
    ]
)

ORGAOS_SCHEMA = pa.schema(
    [
        pa.field("cnpj", pa.string(), nullable=False),
        pa.field("razao_social", pa.string(), nullable=True),
        pa.field("poder_id", pa.string(), nullable=True),  # E/L/J
        pa.field("esfera_id", pa.string(), nullable=True),  # F/E/M/D
        pa.field("contratos_no_dia", pa.int32(), nullable=True),
        pa.field("valor_total_no_dia", pa.float64(), nullable=True),
    ]
)

UNIDADES_SCHEMA = pa.schema(
    [
        pa.field("codigo_unidade", pa.string(), nullable=False),
        pa.field("cnpj_orgao", pa.string(), nullable=False),
        pa.field("nome_unidade", pa.string(), nullable=True),
        pa.field("uf_sigla", pa.string(), nullable=True),
        pa.field("municipio_nome", pa.string(), nullable=True),
        pa.field("codigo_ibge", pa.string(), nullable=True),
        pa.field("contratos_no_dia", pa.int32(), nullable=True),
    ]
)

FORNECEDORES_SCHEMA = pa.schema(
    [
        pa.field("ni_fornecedor", pa.string(), nullable=False),
        pa.field("tipo_pessoa", pa.string(), nullable=True),  # PJ/PF/PE
        pa.field("nome_razao_social", pa.string(), nullable=True),
        pa.field("codigo_pais", pa.string(), nullable=True),
        pa.field("contratos_no_dia", pa.int32(), nullable=True),
        pa.field("valor_total_no_dia", pa.float64(), nullable=True),
    ]
)


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
        date_str = target_date.isoformat()
        day_dir = output_dir / date_str
        day_dir.mkdir(parents=True, exist_ok=True)

        stats: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "data_particao": date_str,
            "extracted_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "source": "PNCP API v1",
            "tables": {},
            "baliza_version": "0.2.0",
        }

        with duckdb.connect(str(self.db_path), read_only=True) as con:
            stats["tables"]["contratos"] = self._export_contratos(con, target_date, day_dir)
            stats["tables"]["orgaos"] = self._export_orgaos(con, target_date, day_dir)
            stats["tables"]["unidades"] = self._export_unidades(con, target_date, day_dir)
            stats["tables"]["fornecedores"] = self._export_fornecedores(con, target_date, day_dir)

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
        """Export contratos fact table for a specific date."""
        next_day = target_date + timedelta(days=1)
        reader = con.execute(
            f"""
            SELECT
                numero_controle_pncp,
                numero_controle_pncp_compra,
                CAST(ano_contrato AS INTEGER)           AS ano_contrato,
                CAST(sequencial_contrato AS INTEGER)    AS sequencial_contrato,
                CAST(ano_compra AS INTEGER)             AS ano_compra,
                CAST(sequencial_compra AS INTEGER)      AS sequencial_compra,
                numero_contrato_empenho,
                CAST(numero_retificacao AS INTEGER)     AS numero_retificacao,
                processo,
                cnpj_orgao,
                razao_social_orgao,
                poder_id,
                esfera_id,
                codigo_unidade,
                nome_unidade,
                uf_sigla,
                uf_nome,
                municipio_nome,
                codigo_ibge,
                cnpj_orgao_subrogado,
                razao_social_orgao_subrogado,
                codigo_unidade_subrogada,
                nome_unidade_subrogada,
                uf_sigla_subrogada,
                ni_fornecedor,
                tipo_pessoa,
                nome_razao_social_fornecedor,
                codigo_pais_fornecedor,
                ni_fornecedor_subcontratado,
                nome_fornecedor_subcontratado,
                tipo_pessoa_subcontratada,
                CAST(modalidade_id AS INTEGER)          AS modalidade_id,
                modalidade_nome,
                CAST(tipo_contrato_id AS INTEGER)       AS tipo_contrato_id,
                tipo_contrato_nome,
                CAST(categoria_processo_id AS INTEGER)  AS categoria_processo_id,
                categoria_processo_nome,
                receita,
                valor_inicial,
                valor_parcela,
                valor_global,
                valor_acumulado,
                CAST(numero_parcelas AS INTEGER)        AS numero_parcelas,
                data_publicacao,
                data_publicacao_pncp,
                data_assinatura,
                data_vigencia_inicio,
                data_vigencia_fim,
                data_inclusao,
                data_atualizacao,
                data_atualizacao_global,
                objeto_contrato,
                informacao_complementar,
                link_sistema_origem,
                identificador_cipi,
                url_cipi,
                usuario_nome,
                CAST(? AS DATE)                         AS data_particao
            FROM {self.dataset}.contratos
            WHERE data_publicacao >= ? AND data_publicacao < ?
            ORDER BY cnpj_orgao, data_publicacao DESC, numero_controle_pncp
        """,
            [target_date, target_date, next_day],
        ).to_arrow_reader(batch_size=PARQUET_ROW_GROUP_SIZE)

        output_path = output_dir / "contratos.parquet"
        file_size, row_count = write_optimized_parquet(
            reader, output_path, CONTRATOS_SCHEMA, "contratos"
        )
        return {"row_count": row_count, "file_size_bytes": file_size}

    def _export_orgaos(
        self,
        con: duckdb.DuckDBPyConnection,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export deduplicated agencies seen on this day."""
        next_day = target_date + timedelta(days=1)
        reader = con.execute(
            f"""
            SELECT
                cnpj_orgao                          AS cnpj,
                MAX(razao_social_orgao)             AS razao_social,
                MAX(poder_id)                       AS poder_id,
                MAX(esfera_id)                      AS esfera_id,
                CAST(COUNT(*) AS INTEGER)           AS contratos_no_dia,
                SUM(valor_inicial)                  AS valor_total_no_dia
            FROM {self.dataset}.contratos
            WHERE data_publicacao >= ? AND data_publicacao < ?
            GROUP BY cnpj_orgao
            ORDER BY cnpj
        """,
            [target_date, next_day],
        ).to_arrow_reader(batch_size=PARQUET_ROW_GROUP_SIZE)

        output_path = output_dir / "orgaos.parquet"
        file_size, row_count = write_optimized_parquet(
            reader, output_path, ORGAOS_SCHEMA, "orgaos"
        )
        return {"row_count": row_count, "file_size_bytes": file_size}

    def _export_unidades(
        self,
        con: duckdb.DuckDBPyConnection,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export deduplicated administrative units seen on this day."""
        next_day = target_date + timedelta(days=1)
        reader = con.execute(
            f"""
            SELECT
                codigo_unidade,
                cnpj_orgao,
                MAX(nome_unidade)                   AS nome_unidade,
                MAX(uf_sigla)                       AS uf_sigla,
                MAX(municipio_nome)                 AS municipio_nome,
                MAX(codigo_ibge)                    AS codigo_ibge,
                CAST(COUNT(*) AS INTEGER)           AS contratos_no_dia
            FROM {self.dataset}.contratos
            WHERE data_publicacao >= ? AND data_publicacao < ?
              AND codigo_unidade IS NOT NULL
            GROUP BY codigo_unidade, cnpj_orgao
            ORDER BY codigo_unidade
        """,
            [target_date, next_day],
        ).to_arrow_reader(batch_size=PARQUET_ROW_GROUP_SIZE)

        output_path = output_dir / "unidades.parquet"
        file_size, row_count = write_optimized_parquet(
            reader, output_path, UNIDADES_SCHEMA, "unidades"
        )
        return {"row_count": row_count, "file_size_bytes": file_size}

    def _export_fornecedores(
        self,
        con: duckdb.DuckDBPyConnection,
        target_date: date,
        output_dir: Path,
    ) -> dict[str, Any]:
        """Export deduplicated suppliers seen on this day."""
        next_day = target_date + timedelta(days=1)
        reader = con.execute(
            f"""
            SELECT
                ni_fornecedor,
                MAX(tipo_pessoa)                        AS tipo_pessoa,
                MAX(nome_razao_social_fornecedor)       AS nome_razao_social,
                MAX(codigo_pais_fornecedor)             AS codigo_pais,
                CAST(COUNT(*) AS INTEGER)               AS contratos_no_dia,
                SUM(valor_inicial)                      AS valor_total_no_dia
            FROM {self.dataset}.contratos
            WHERE data_publicacao >= ? AND data_publicacao < ?
              AND ni_fornecedor IS NOT NULL
            GROUP BY ni_fornecedor
            ORDER BY ni_fornecedor
        """,
            [target_date, next_day],
        ).to_arrow_reader(batch_size=PARQUET_ROW_GROUP_SIZE)

        output_path = output_dir / "fornecedores.parquet"
        file_size, row_count = write_optimized_parquet(
            reader, output_path, FORNECEDORES_SCHEMA, "fornecedores"
        )
        return {"row_count": row_count, "file_size_bytes": file_size}

