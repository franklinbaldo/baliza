"""
Domain table integration for Ibis pipeline.
Loads CSV domain tables and provides enrichment functions.
"""
import ibis
import pandas as pd
from pathlib import Path
from typing import Dict, Optional

def load_domain_tables(con: ibis.BaseBackend) -> Dict[str, ibis.Table]:
    """
    Load all CSV domain tables as Ibis tables for reference joins.
    
    Args:
        con: Ibis backend connection
        
    Returns:
        Dictionary mapping table names to Ibis tables
    """
    domain_path = Path("dbt_baliza/seeds/domain_tables")
    domain_tables = {}
    
    if not domain_path.exists():
        print(f"Warning: Domain tables directory not found: {domain_path}")
        return domain_tables
    
    csv_files = [f for f in domain_path.glob("*.csv") if f.name != "README.md"]
    
    for csv_file in csv_files:
        try:
            table_name = f"dim_{csv_file.stem}"
            df = pd.read_csv(csv_file)
            
            # Standardize column names (some CSVs use 'description' instead of 'name')
            if "name" not in df.columns and "description" in df.columns:
                df = df.rename(columns={"description": "name"})
            
            # Ensure we have the expected structure
            if "code" in df.columns and "name" in df.columns:
                domain_tables[table_name] = con.create_table(
                    table_name, ibis.memtable(df), overwrite=True
                )
                print(f"✅ Loaded {table_name}: {len(df)} records")
            else:
                print(f"⚠️  Skipping {csv_file.name}: missing required columns (code, name)")
                
        except Exception as e:
            print(f"❌ Error loading {csv_file.name}: {e}")
    
    return domain_tables

def enrich_with_domain_data(stage_table: ibis.Table, con: ibis.BaseBackend) -> ibis.Table:
    """
    Enrich stage tables with human-readable domain descriptions.
    
    Args:
        stage_table: The stage table to enrich
        con: Ibis backend connection
        
    Returns:
        Enriched table with additional descriptive columns
    """
    enriched = stage_table
    
    try:
        # Get list of available domain tables
        available_tables = con.list_tables()
        
        # Enrich with modalidade names
        if "dim_modalidade_contratacao" in available_tables and "modalidadeId" in stage_table.columns:
            modalidade_dim = con.table("dim_modalidade_contratacao")
            enriched = enriched.join(
                modalidade_dim,
                enriched.modalidadeId.cast("int") == modalidade_dim.code.cast("int"),
                how="left"
            ).select(
                enriched,
                modalidade_dim.name.name("modalidade_nome")
            )
        
        # Enrich with situacao names
        if "dim_situacao_contratacao" in available_tables and "codigoSituacaoContratacao" in enriched.columns:
            situacao_dim = con.table("dim_situacao_contratacao")
            enriched = enriched.join(
                situacao_dim,
                enriched.codigoSituacaoContratacao.cast("int") == situacao_dim.code.cast("int"),
                how="left"
            ).select(
                enriched,
                situacao_dim.name.name("situacao_nome")
            )
        
        # Enrich with UF names
        if "dim_uf_brasil" in available_tables and "uf" in enriched.columns:
            uf_dim = con.table("dim_uf_brasil")
            enriched = enriched.join(
                uf_dim,
                enriched.uf == uf_dim.code,
                how="left"
            ).select(
                enriched,
                uf_dim.name.name("uf_nome")
            )
            
        # Enrich with natureza juridica (if available)
        if "dim_natureza_juridica" in available_tables and "naturezaJuridica" in enriched.columns:
            natureza_dim = con.table("dim_natureza_juridica")
            enriched = enriched.join(
                natureza_dim,
                enriched.naturezaJuridica.cast("int") == natureza_dim.code.cast("int"),
                how="left"
            ).select(
                enriched,
                natureza_dim.name.name("natureza_juridica_nome")
            )
            
    except Exception as e:
        print(f"Warning: Error during domain enrichment: {e}")
        # Return original table if enrichment fails
        return stage_table
    
    return enriched

def validate_domain_table_consistency(con: ibis.BaseBackend) -> Dict[str, any]:
    """
    Validate that domain tables are consistent with enum definitions.
    
    Args:
        con: Ibis backend connection
        
    Returns:
        Validation report
    """
    validation_report = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "table_counts": {}
    }
    
    try:
        from src.baliza.enums import (
            ModalidadeContratacao, 
            SituacaoContratacao,
            ENUM_REGISTRY
        )
        
        available_tables = con.list_tables()
        
        # Validate modalidade consistency
        if "dim_modalidade_contratacao" in available_tables:
            modalidade_table = con.table("dim_modalidade_contratacao")
            modalidade_df = modalidade_table.execute()
            csv_codes = set(modalidade_df["code"])
            enum_codes = {m.value for m in ModalidadeContratacao}
            
            validation_report["table_counts"]["modalidade"] = len(modalidade_df)
            
            # Check if enum values exist in CSV
            missing_in_csv = enum_codes - csv_codes
            if missing_in_csv:
                validation_report["warnings"].append(
                    f"Modalidade enum values not in CSV: {missing_in_csv}"
                )
        
        # Validate situacao consistency  
        if "dim_situacao_contratacao" in available_tables:
            situacao_table = con.table("dim_situacao_contratacao")
            situacao_df = situacao_table.execute()
            csv_codes = set(situacao_df["code"])
            enum_codes = {s.value for s in SituacaoContratacao}
            
            validation_report["table_counts"]["situacao"] = len(situacao_df)
            
            missing_in_csv = enum_codes - csv_codes
            if missing_in_csv:
                validation_report["warnings"].append(
                    f"Situacao enum values not in CSV: {missing_in_csv}"
                )
        
        # Count all domain tables
        domain_tables = [t for t in available_tables if t.startswith("dim_")]
        validation_report["total_domain_tables"] = len(domain_tables)
        
    except Exception as e:
        validation_report["valid"] = False
        validation_report["errors"].append(f"Validation error: {e}")
    
    return validation_report

def get_domain_table_stats(con: ibis.BaseBackend) -> Dict[str, any]:
    """
    Get statistics about loaded domain tables.
    
    Args:
        con: Ibis backend connection
        
    Returns:
        Statistics dictionary
    """
    stats = {
        "total_tables": 0,
        "total_records": 0,
        "tables": {}
    }
    
    try:
        available_tables = con.list_tables()
        domain_tables = [t for t in available_tables if t.startswith("dim_")]
        
        stats["total_tables"] = len(domain_tables)
        
        for table_name in domain_tables:
            table = con.table(table_name)
            count = table.count().execute()
            stats["tables"][table_name] = {
                "records": count,
                "columns": list(table.columns)
            }
            stats["total_records"] += count
            
    except Exception as e:
        stats["error"] = str(e)
    
    return stats

def create_domain_lookup_functions(con: ibis.BaseBackend) -> Dict[str, any]:
    """
    Create lookup functions for common domain value translations.
    
    Args:
        con: Ibis backend connection
        
    Returns:
        Dictionary of lookup functions
    """
    lookup_functions = {}
    
    try:
        available_tables = con.list_tables()
        
        # Modalidade lookup
        if "dim_modalidade_contratacao" in available_tables:
            modalidade_table = con.table("dim_modalidade_contratacao")
            modalidade_df = modalidade_table.execute()
            modalidade_lookup = dict(zip(modalidade_df["code"], modalidade_df["name"]))
            
            def get_modalidade_name(modalidade_id: int) -> Optional[str]:
                return modalidade_lookup.get(modalidade_id)
            
            lookup_functions["modalidade"] = get_modalidade_name
        
        # Situacao lookup
        if "dim_situacao_contratacao" in available_tables:
            situacao_table = con.table("dim_situacao_contratacao")
            situacao_df = situacao_table.execute()
            situacao_lookup = dict(zip(situacao_df["code"], situacao_df["name"]))
            
            def get_situacao_name(situacao_id: int) -> Optional[str]:
                return situacao_lookup.get(situacao_id)
            
            lookup_functions["situacao"] = get_situacao_name
        
        # UF lookup
        if "dim_uf_brasil" in available_tables:
            uf_table = con.table("dim_uf_brasil")
            uf_df = uf_table.execute()
            uf_lookup = dict(zip(uf_df["code"], uf_df["name"]))
            
            def get_uf_name(uf_code: str) -> Optional[str]:
                return uf_lookup.get(uf_code)
            
            lookup_functions["uf"] = get_uf_name
            
    except Exception as e:
        lookup_functions["error"] = str(e)
    
    return lookup_functions