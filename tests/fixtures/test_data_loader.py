"""
Test data loader for Ibis pipeline using real PNCP API response data.
"""
import json
import ibis
from pathlib import Path
from typing import List, Dict, Any

def load_test_data_as_bronze(con: ibis.BaseBackend, data_type: str = "contratacoes_publicacao") -> List[Dict[str, Any]]:
    """
    Load real PNCP test data into bronze tables for pipeline testing.
    
    Args:
        con: Ibis backend connection
        data_type: Type of test data to load (contratacoes_publicacao, contratos_publicacao, etc.)
        
    Returns:
        List of records loaded into the bronze table
    """
    test_file = Path(__file__).parent.parent / "test_data" / f"{data_type}.json"
    
    if not test_file.exists():
        raise FileNotFoundError(f"Test data file not found: {test_file}")
    
    with open(test_file) as f:
        test_data = json.load(f)
    
    # Transform API response to bronze table format
    records = []
    api_data = test_data.get("data", {}).get("data", [])
    
    for item in api_data:
        # Map API structure to bronze table schema
        record = {
            "numeroControlePNCP": item.get("numeroControlePNCP", ""),
            "anoContratacao": item.get("anoCompra", item.get("anoContratacao", 2024)),
            "dataPublicacaoPNCP": item.get("dataPublicacaoPncp", item.get("dataPublicacaoPNCP", "")),
            "dataAtualizacaoPNCP": item.get("dataAtualizacao", item.get("dataAtualizacaoPNCP", "")),
            "horaAtualizacaoPNCP": "12:00:00",  # Default since not in API
            "sequencialOrgao": item.get("orgaoEntidade", {}).get("sequencial", 1),
            "cnpjOrgao": item.get("orgaoEntidade", {}).get("cnpj", ""),
            "siglaOrgao": "TEST",  # Not in API, use default
            "nomeOrgao": item.get("orgaoEntidade", {}).get("razaoSocial", ""),
            "sequencialUnidade": 1,  # Default
            "codigoUnidade": item.get("unidadeOrgao", {}).get("codigoUnidade", "001"),
            "siglaUnidade": "TU",  # Default
            "nomeUnidade": item.get("unidadeOrgao", {}).get("nomeUnidade", ""),
            "codigoEsfera": item.get("orgaoEntidade", {}).get("esferaId", "M"),
            "nomeEsfera": "Municipal",  # Map from esferaId
            "codigoPoder": item.get("orgaoEntidade", {}).get("poderId", "E"),
            "nomePoder": "Executivo",  # Map from poderId
            "codigoMunicipio": item.get("unidadeOrgao", {}).get("codigoIbge", ""),
            "nomeMunicipio": item.get("unidadeOrgao", {}).get("municipioNome", ""),
            "uf": item.get("unidadeOrgao", {}).get("ufSigla", ""),
            "amparoLegalId": str(item.get("amparoLegal", {}).get("codigo", 1)),
            "amparoLegalNome": item.get("amparoLegal", {}).get("nome", ""),
            "modalidadeId": str(item.get("modalidadeId", 1)),
            "modalidadeNome": item.get("modalidadeNome", ""),
            "numeroContratacao": item.get("numeroCompra", "001/2024"),
            "processo": item.get("processo", ""),
            "objetoContratacao": item.get("objetoCompra", item.get("objetoContratacao", "")),
            "codigoSituacaoContratacao": "1",  # Default
            "nomeSituacaoContratacao": "Active",  # Default
            "valorTotalEstimado": float(item.get("valorTotalEstimado", 0)),
            "informacaoComplementar": item.get("informacaoComplementar", ""),
            "dataAssinatura": item.get("dataAberturaProposta", "2024-01-01"),
            "dataVigenciaInicio": item.get("dataAberturaProposta", "2024-01-01"),
            "dataVigenciaFim": item.get("dataEncerramentoProposta", "2024-12-31"),
            "numeroControlePNCPContrato": item.get("numeroControlePNCP", ""),
            "justificativa": item.get("justificativaPresencial", ""),
            "fundamentacaoLegal": item.get("amparoLegal", {}).get("descricao", ""),
        }
        records.append(record)
    
    # Create bronze table with real data
    if records:
        con.create_table("bronze_pncp_raw", ibis.memtable(records), overwrite=True)
    
    return records

def load_multiple_test_datasets(con: ibis.BaseBackend) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load multiple test datasets for comprehensive testing.
    
    Returns:
        Dictionary mapping dataset names to loaded records
    """
    datasets = {}
    test_files = [
        "contratacoes_publicacao",
        "contratacoes_atualizacao",
        "contratos_publicacao", 
        "contratos_atualizacao"
    ]
    
    all_records = []
    for data_type in test_files:
        try:
            records = load_test_data_as_bronze(con, data_type)
            datasets[data_type] = records
            all_records.extend(records)
        except FileNotFoundError:
            # Skip missing test files
            continue
    
    # Create combined bronze table
    if all_records:
        con.create_table("bronze_pncp_raw", ibis.memtable(all_records), overwrite=True)
    
    return datasets

def create_sample_bronze_table(con: ibis.BaseBackend, num_records: int = 100) -> List[Dict[str, Any]]:
    """
    Create a larger sample dataset by duplicating and modifying real test data.
    Useful for performance testing.
    """
    base_records = load_test_data_as_bronze(con, "contratacoes_publicacao")
    
    if not base_records:
        return []
    
    large_records = []
    for i in range(num_records // len(base_records) + 1):
        for record in base_records:
            new_record = record.copy()
            new_record["numeroControlePNCP"] = f"{record['numeroControlePNCP']}-{i}"
            new_record["anoContratacao"] = 2024 + (i % 3)  # Vary years
            new_record["valorTotalEstimado"] = record["valorTotalEstimado"] * (1 + i * 0.1)
            large_records.append(new_record)
            
            if len(large_records) >= num_records:
                break
        
        if len(large_records) >= num_records:
            break
    
    # Create large bronze table
    con.create_table("bronze_pncp_raw", ibis.memtable(large_records[:num_records]), overwrite=True)
    return large_records[:num_records]

def validate_test_data_structure(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate that test data has expected structure and quality.
    
    Returns:
        Validation report dictionary
    """
    if not records:
        return {"valid": False, "error": "No records found"}
    
    required_fields = [
        "numeroControlePNCP", "anoContratacao", "modalidadeId", 
        "cnpjOrgao", "uf", "valorTotalEstimado"
    ]
    
    validation_report = {
        "valid": True,
        "total_records": len(records),
        "missing_fields": [],
        "data_quality": {}
    }
    
    # Check required fields
    for field in required_fields:
        missing_count = sum(1 for r in records if not r.get(field))
        if missing_count > 0:
            validation_report["missing_fields"].append({
                "field": field,
                "missing_count": missing_count,
                "missing_percentage": missing_count / len(records) * 100
            })
    
    # Data quality checks
    validation_report["data_quality"] = {
        "unique_contracts": len(set(r["numeroControlePNCP"] for r in records)),
        "year_range": {
            "min": min(r["anoContratacao"] for r in records),
            "max": max(r["anoContratacao"] for r in records)
        },
        "value_range": {
            "min": min(r["valorTotalEstimado"] for r in records),
            "max": max(r["valorTotalEstimado"] for r in records),
            "total": sum(r["valorTotalEstimado"] for r in records)
        },
        "states_covered": len(set(r["uf"] for r in records if r["uf"])),
        "modalities_covered": len(set(r["modalidadeId"] for r in records if r["modalidadeId"]))
    }
    
    return validation_report