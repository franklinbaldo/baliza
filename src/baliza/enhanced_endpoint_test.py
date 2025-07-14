#!/usr/bin/env python3
"""
Enhanced PNCP Endpoint Tester
Test Contratações endpoints with proper parameters based on OpenAPI client analysis
"""

import json
from pathlib import Path
from typing import Dict, Any, List
import sys
import os

# Add the package to the path to allow imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.baliza.pncp_client import PNCPClient


def test_contratacoes_with_parameters(client: PNCPClient, test_date: str) -> Dict[str, Any]:
    """Test Contratações endpoints with different modalidade parameters."""
    
    # Common modalidade de contratação codes based on PNCP documentation
    modalidades_to_test = [
        1,   # Pregão Presencial
        2,   # Pregão Eletrônico 
        3,   # Concorrência
        4,   # Tomada de Preços
        5,   # Convite
        6,   # Concurso
        7,   # Leilão
        8,   # Dispensa de Licitação
        9,   # Inexigibilidade
        10,  # Credenciamento
        11,  # Pré-qualificação
        12,  # Sistema de Registro de Preços
        13,  # Diálogo Competitivo
        14   # Licitação por Técnica e Preço
    ]
    
    results = {}
    
    # Test Contratações por Data de Publicação
    print("\n🔍 Testing Contratações por Data de Publicação with different modalidades:")
    
    for modalidade in modalidades_to_test:
        print(f"  📋 Testing modalidade {modalidade}...")
        
        try:
            # Use the underlying httpx client from our wrapper
            httpx_client = client.client.get_httpx_client()
            response = httpx_client.get(
                "/v1/contratacoes/publicacao",
                params={
                    "dataInicial": test_date,
                    "dataFinal": test_date,
                    "codigoModalidadeContratacao": modalidade,
                    "pagina": 1,
                    "tamanhoPagina": 10
                }
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    
                    if total_records > 0:
                        print(f"    ✅ Success: Found {total_records:,} records for modalidade {modalidade}")
                        
                        sample_data = data.get("data", [])
                        results[f"contratacoes_publicacao_modalidade_{modalidade}"] = {
                            "endpoint": "/v1/contratacoes/publicacao",
                            "modalidade": modalidade,
                            "status": "success",
                            "total_records": total_records,
                            "sample_count": len(sample_data),
                            "sample_fields": list(sample_data[0].keys()) if sample_data else [],
                            "sample_data": sample_data[:2] if sample_data else []  # Just 2 samples to save space
                        }
                    else:
                        print(f"    📭 No data for modalidade {modalidade}")
                        
                except json.JSONDecodeError:
                    print(f"    ⚠️  Invalid JSON response for modalidade {modalidade}")
                    
            elif response.status_code == 204:
                print(f"    📭 No data for modalidade {modalidade}")
                
            elif response.status_code == 400:
                print(f"    ❌ Bad request for modalidade {modalidade}")
                
            else:
                print(f"    ❌ HTTP {response.status_code} for modalidade {modalidade}")
                
        except Exception as e:
            print(f"    ❌ Error for modalidade {modalidade}: {e}")
    
    # Test Contratações por Data de Atualização
    print("\n🔍 Testing Contratações por Data de Atualização with working modalidades:")
    
    # Only test modalidades that worked for publicação
    working_modalidades = [
        int(key.split("_")[-1]) for key in results.keys() 
        if key.startswith("contratacoes_publicacao_modalidade_")
    ]
    
    for modalidade in working_modalidades[:3]:  # Test only first 3 working ones
        print(f"  📋 Testing modalidade {modalidade}...")
        
        try:
            httpx_client = client.client.get_httpx_client()
            response = httpx_client.get(
                "/v1/contratacoes/atualizacao",
                params={
                    "dataInicial": test_date,
                    "dataFinal": test_date,
                    "codigoModalidadeContratacao": modalidade,
                    "pagina": 1,
                    "tamanhoPagina": 10
                }
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    
                    if total_records > 0:
                        print(f"    ✅ Success: Found {total_records:,} records for modalidade {modalidade}")
                        
                        sample_data = data.get("data", [])
                        results[f"contratacoes_atualizacao_modalidade_{modalidade}"] = {
                            "endpoint": "/v1/contratacoes/atualizacao",
                            "modalidade": modalidade,
                            "status": "success",
                            "total_records": total_records,
                            "sample_count": len(sample_data),
                            "sample_fields": list(sample_data[0].keys()) if sample_data else [],
                            "sample_data": sample_data[:2] if sample_data else []
                        }
                    else:
                        print(f"    📭 No data for modalidade {modalidade}")
                        
                except json.JSONDecodeError:
                    print(f"    ⚠️  Invalid JSON response for modalidade {modalidade}")
                    
            else:
                print(f"    ❌ HTTP {response.status_code} for modalidade {modalidade}")
                
        except Exception as e:
            print(f"    ❌ Error for modalidade {modalidade}: {e}")
    
    # Test Período de Recebimento de Propostas endpoint
    print("\n🔍 Testing Contratações - Período de Recebimento de Propostas:")
    
    for modalidade in working_modalidades[:2]:  # Test first 2 working modalidades
        print(f"  📋 Testing modalidade {modalidade}...")
        
        try:
            httpx_client = client.client.get_httpx_client()
            response = httpx_client.get(
                "/v1/contratacoes/propostas",
                params={
                    "dataInicial": test_date,
                    "dataFinal": test_date,
                    "codigoModalidadeContratacao": modalidade,
                    "pagina": 1,
                    "tamanhoPagina": 10
                }
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    
                    if total_records > 0:
                        print(f"    ✅ Success: Found {total_records:,} records for modalidade {modalidade}")
                        
                        sample_data = data.get("data", [])
                        results[f"contratacoes_propostas_modalidade_{modalidade}"] = {
                            "endpoint": "/v1/contratacoes/propostas",
                            "modalidade": modalidade,
                            "status": "success",
                            "total_records": total_records,
                            "sample_count": len(sample_data),
                            "sample_fields": list(sample_data[0].keys()) if sample_data else [],
                            "sample_data": sample_data[:2] if sample_data else []
                        }
                    else:
                        print(f"    📭 No data for modalidade {modalidade}")
                        
                except json.JSONDecodeError:
                    print(f"    ⚠️  Invalid JSON response for modalidade {modalidade}")
                    
            else:
                print(f"    ❌ HTTP {response.status_code} for modalidade {modalidade}")
                
        except Exception as e:
            print(f"    ❌ Error for modalidade {modalidade}: {e}")
    
    return results


def test_other_missing_endpoints(client: PNCPClient, test_date: str) -> Dict[str, Any]:
    """Test other endpoints with different parameter combinations."""
    
    results = {}
    
    # Test different paths for missing endpoints
    endpoints_to_try = [
        # Plano de Contratação variations
        {
            "name": "Plano de Contratação (ano)",
            "path": "/v1/plano-contratacoes",
            "params": {"ano": test_date[:4], "pagina": 1, "tamanhoPagina": 10}
        },
        {
            "name": "Plano de Contratação (cnpj + ano)",
            "path": "/v1/plano-contratacoes", 
            "params": {"ano": test_date[:4], "cnpj": "00000000000191", "pagina": 1, "tamanhoPagina": 10}  # Federal gov CNPJ
        },
        
        # Instrumentos de Cobrança variations
        {
            "name": "Instrumentos de Cobrança (data)",
            "path": "/v1/instrumentos-cobranca",
            "params": {"dataInicial": test_date, "dataFinal": test_date, "pagina": 1, "tamanhoPagina": 10}
        },
        {
            "name": "Instrumentos de Cobrança (atualização)",
            "path": "/v1/instrumentos-cobranca/atualizacao",
            "params": {"dataInicial": test_date, "dataFinal": test_date, "pagina": 1, "tamanhoPagina": 10}
        },
        
        # Alternative paths
        {
            "name": "Compras (alternative)",
            "path": "/v1/compras",
            "params": {"dataInicial": test_date, "dataFinal": test_date, "pagina": 1, "tamanhoPagina": 10}
        },
        {
            "name": "Licitações",
            "path": "/v1/licitacoes",
            "params": {"dataInicial": test_date, "dataFinal": test_date, "pagina": 1, "tamanhoPagina": 10}
        }
    ]
    
    print("\n🔍 Testing Other Endpoint Variations:")
    
    for endpoint_info in endpoints_to_try:
        print(f"  📋 Testing: {endpoint_info['name']}")
        
        try:
            httpx_client = client.client.get_httpx_client()
            response = httpx_client.get(
                endpoint_info["path"],
                params=endpoint_info["params"]
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    
                    print(f"    ✅ Success: Found {total_records:,} records")
                    
                    if total_records > 0:
                        sample_data = data.get("data", [])
                        results[endpoint_info["path"]] = {
                            "name": endpoint_info["name"],
                            "status": "success",
                            "total_records": total_records,
                            "sample_count": len(sample_data),
                            "sample_fields": list(sample_data[0].keys()) if sample_data else [],
                            "sample_data": sample_data[:2] if sample_data else []
                        }
                    
                except json.JSONDecodeError:
                    print(f"    ⚠️  Success but invalid JSON response")
                    
            elif response.status_code == 204:
                print(f"    📭 No data available")
                
            else:
                print(f"    ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"    ❌ Error: {e}")
    
    return results


def main():
    """Run enhanced PNCP endpoint testing with proper parameters."""
    print("🔍 Enhanced PNCP API Endpoint Analysis")
    print("Testing Contratações endpoints with proper modalidade parameters")
    print("=" * 70)
    
    test_date = "20240710"  # July 10, 2024
    print(f"📅 Test Date: {test_date}")
    
    # Initialize client
    client = PNCPClient()
    
    # Test results storage
    all_results = {
        "test_date": test_date,
        "endpoints": {}
    }
    
    # Test Contratações endpoints with proper parameters
    print("\n" + "=" * 70)
    print("TESTING CONTRATAÇÕES ENDPOINTS WITH MODALIDADE PARAMETERS")
    print("=" * 70)
    
    contratacoes_results = test_contratacoes_with_parameters(client, test_date)
    all_results["endpoints"].update(contratacoes_results)
    
    # Test other missing endpoints
    print("\n" + "=" * 70)
    print("TESTING OTHER ENDPOINT VARIATIONS")
    print("=" * 70)
    
    other_results = test_other_missing_endpoints(client, test_date)
    all_results["endpoints"].update(other_results)
    
    # Save results
    output_dir = Path("data/endpoint_analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"enhanced_pncp_endpoint_analysis_{test_date}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)
    
    # Print summary
    print("\n" + "=" * 70)
    print("ENHANCED ANALYSIS SUMMARY")
    print("=" * 70)
    
    total_endpoints = len(all_results["endpoints"])
    successful_endpoints = sum(1 for ep in all_results["endpoints"].values() 
                             if ep.get("status") == "success")
    total_records = sum(ep.get("total_records", 0) for ep in all_results["endpoints"].values())
    
    print(f"📊 Total endpoint combinations tested: {total_endpoints}")
    print(f"✅ Successful combinations: {successful_endpoints}")
    print(f"📄 Total records found: {total_records:,}")
    print(f"💾 Results saved to: {output_file}")
    
    # Print detailed results for successful endpoints
    if successful_endpoints > 0:
        print(f"\n🎉 Successfully Working Endpoints:")
        for endpoint_path, result in all_results["endpoints"].items():
            if result.get("status") == "success":
                name = result.get("name", endpoint_path)
                total_records = result.get("total_records", 0)
                modalidade = result.get("modalidade", "")
                modalidade_text = f" (modalidade {modalidade})" if modalidade else ""
                
                print(f"  ✅ {name}{modalidade_text}: {total_records:,} records")
    
    return all_results


if __name__ == "__main__":
    main()