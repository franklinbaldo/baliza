"""
Data connector to consume clean data from Baliza PSA (Persistent Staging Area).
"""
import os
from pathlib import Path
from typing import Optional
import duckdb
import pandas as pd


class BalizaDataConnector:
    """Connects to Baliza database and provides clean contract data."""
    
    def __init__(self, baliza_db_path: Optional[str] = None):
        """Initialize connection to Baliza database."""
        if baliza_db_path is None:
            # Default: Look for Baliza DB relative to this project
            baliza_db_path = Path(__file__).parents[4] / "state" / "baliza.duckdb"
        
        self.baliza_db_path = str(baliza_db_path)
        if not os.path.exists(self.baliza_db_path):
            raise FileNotFoundError(f"Baliza database not found at: {self.baliza_db_path}")
    
    def get_contracts(
        self, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        uf_filter: Optional[list[str]] = None,
        min_value: Optional[float] = None
    ) -> pd.DataFrame:
        """
        Get clean contract data from Baliza staging area.
        
        Args:
            start_date: Filter contracts from this date (YYYY-MM-DD)
            end_date: Filter contracts until this date (YYYY-MM-DD)
            uf_filter: List of UF codes to filter (e.g., ['RO', 'SP'])
            min_value: Minimum contract value in BRL
            
        Returns:
            DataFrame with clean contract data
        """
        conn = duckdb.connect(':memory:')
        conn.execute(f"ATTACH '{self.baliza_db_path}' AS baliza_source")
        
        query = """
            SELECT 
                contrato_id,
                data_assinatura,
                data_vigencia_inicio,
                data_vigencia_fim,
                valor_global_brl,
                valor_inicial_brl,
                duracao_contrato_dias,
                
                -- Organizational info
                orgao_cnpj,
                orgao_razao_social,
                orgao_poder_id,
                orgao_esfera_id,
                uf_sigla,
                uf_nome,
                municipio_nome,
                municipio_codigo_ibge,
                
                -- Supplier info
                fornecedor_ni,
                fornecedor_nome,
                fornecedor_tipo_pessoa,
                fornecedor_pais_codigo,
                
                -- Contract details
                objeto_contrato,
                tipo_contrato_nome,
                categoria_processo_nome,
                
                -- Quality flags
                flag_valor_invalido,
                flag_datas_incompletas,
                
                -- Metadata
                baliza_data_date,
                baliza_extracted_at
                
            FROM baliza_source.staging.stg_contratos
            WHERE 1=1
        """
        
        conditions = []
        
        if start_date:
            conditions.append(f"data_assinatura >= '{start_date}'")
        
        if end_date:
            conditions.append(f"data_assinatura <= '{end_date}'")
            
        if uf_filter:
            uf_list = "', '".join(uf_filter)
            conditions.append(f"uf_sigla IN ('{uf_list}')")
            
        if min_value:
            conditions.append(f"valor_global_brl >= {min_value}")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
            
        query += " ORDER BY data_assinatura DESC"
        
        return conn.execute(query).df()
    
    def get_summary_stats(self) -> dict:
        """Get basic statistics about the Baliza dataset."""
        conn = duckdb.connect(':memory:')
        conn.execute(f"ATTACH '{self.baliza_db_path}' AS baliza_source")
        
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_contracts,
                COUNT(DISTINCT orgao_cnpj) as total_agencies,
                COUNT(DISTINCT fornecedor_ni) as total_suppliers,
                COUNT(DISTINCT uf_sigla) as total_states,
                MIN(data_assinatura) as earliest_contract,
                MAX(data_assinatura) as latest_contract,
                SUM(valor_global_brl) / 1000000 as total_value_millions_brl,
                AVG(valor_global_brl) / 1000000 as avg_value_millions_brl
            FROM baliza_source.staging.stg_contratos
            WHERE NOT flag_valor_invalido
        """).fetchone()
        
        return {
            "total_contracts": stats[0],
            "total_agencies": stats[1], 
            "total_suppliers": stats[2],
            "total_states": stats[3],
            "earliest_contract": stats[4],
            "latest_contract": stats[5],
            "total_value_millions_brl": round(stats[6], 2),
            "avg_value_millions_brl": round(stats[7], 4)
        }
    
    def get_top_suppliers(self, limit: int = 20) -> pd.DataFrame:
        """Get suppliers with most contracts."""
        conn = duckdb.connect(':memory:')
        conn.execute(f"ATTACH '{self.baliza_db_path}' AS baliza_source")
        
        return conn.execute(f"""
            SELECT 
                fornecedor_ni,
                fornecedor_nome,
                COUNT(*) as total_contratos,
                SUM(valor_global_brl) / 1000000 as valor_total_milhoes_brl,
                AVG(valor_global_brl) / 1000000 as valor_medio_milhoes_brl,
                COUNT(DISTINCT orgao_cnpj) as orgaos_diferentes
            FROM baliza_source.staging.stg_contratos
            WHERE NOT flag_valor_invalido
            GROUP BY 1, 2
            ORDER BY total_contratos DESC
            LIMIT {limit}
        """).df()
    
    def get_top_agencies(self, limit: int = 20) -> pd.DataFrame:
        """Get agencies with most contracts."""
        conn = duckdb.connect(':memory:')
        conn.execute(f"ATTACH '{self.baliza_db_path}' AS baliza_source")
        
        return conn.execute(f"""
            SELECT 
                orgao_cnpj,
                orgao_razao_social,
                uf_sigla,
                COUNT(*) as total_contratos,
                SUM(valor_global_brl) / 1000000 as valor_total_milhoes_brl,
                COUNT(DISTINCT fornecedor_ni) as fornecedores_diferentes
            FROM baliza_source.staging.stg_contratos
            WHERE NOT flag_valor_invalido
            GROUP BY 1, 2, 3
            ORDER BY total_contratos DESC
            LIMIT {limit}
        """).df()