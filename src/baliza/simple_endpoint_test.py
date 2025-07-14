#!/usr/bin/env python3
"""
Simple PNCP Endpoint Tester
Test all available PNCP API endpoints using the wrapper client
"""

import json
from pathlib import Path
from datetime import date, timedelta
from typing import Dict, Any

from .pncp_client import PNCPClient


def test_contratos_endpoint(client: PNCPClient, test_date: str) -> Dict[str, Any]:
    """Test the contratos endpoint (our working implementation)."""
    print(f"🔍 Testing Contratos endpoint for date: {test_date}")
    
    try:
        result = client.fetch_contratos_data(
            data_inicial=test_date,
            data_final=test_date,
            pagina=1,
            tamanho_pagina=10
        )
        
        total_records = result.get("totalRegistros", 0)
        sample_data = result.get("data", [])
        
        print(f"✅ Success: Found {total_records:,} contratos")
        
        if sample_data:
            print(f"📄 Sample fields: {list(sample_data[0].keys())}")
            print(f"📊 Sample record count: {len(sample_data)}")
        
        return {
            "endpoint": "/v1/contratos",
            "status": "success",
            "total_records": total_records,
            "sample_count": len(sample_data),
            "sample_fields": list(sample_data[0].keys()) if sample_data else [],
            "sample_data": sample_data[:3] if sample_data else []
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "endpoint": "/v1/contratos",
            "status": "error",
            "error": str(e),
            "total_records": 0,
            "sample_count": 0
        }


def test_manual_endpoints(client: PNCPClient, test_date: str) -> Dict[str, Any]:
    """Test other endpoints using manual httpx calls."""
    
    endpoints_to_test = [
        {
            "name": "Contratos por Data de Atualização",
            "path": "/v1/contratos/atualizacao",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Contratações por Data de Publicação",
            "path": "/v1/contratacoes/publicacao",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Contratações por Data de Atualização",
            "path": "/v1/contratacoes/atualizacao",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Contratações - Período de Recebimento de Propostas",
            "path": "/v1/contratacoes/propostas",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Atas de Registro de Preços por Período",
            "path": "/v1/atas",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Atas de Registro de Preços por Data de Atualização",
            "path": "/v1/atas/atualizacao",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Plano de Contratação",
            "path": "/v1/plano-contratacoes",
            "params": {
                "ano": test_date[:4],  # Extract year from YYYYMMDD
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        },
        {
            "name": "Instrumentos de Cobrança",
            "path": "/v1/instrumentos-cobranca",
            "params": {
                "dataInicial": test_date,
                "dataFinal": test_date,
                "pagina": "1",
                "tamanhoPagina": "10"
            }
        }
    ]
    
    results = {}
    
    for endpoint_info in endpoints_to_test:
        print(f"\n🔍 Testing: {endpoint_info['name']}")
        
        try:
            # Use the underlying httpx client from our wrapper
            response = client.client._client.get(
                url=endpoint_info["path"],
                params=endpoint_info["params"]
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    sample_data = data.get("data", [])
                    
                    print(f"✅ Success: Found {total_records:,} records")
                    
                    if sample_data:
                        print(f"📄 Sample fields: {list(sample_data[0].keys())}")
                    
                    results[endpoint_info["path"]] = {
                        "name": endpoint_info["name"],
                        "status": "success",
                        "total_records": total_records,
                        "sample_count": len(sample_data),
                        "sample_fields": list(sample_data[0].keys()) if sample_data else [],
                        "sample_data": sample_data[:3] if sample_data else []
                    }
                    
                except json.JSONDecodeError:
                    print(f"⚠️  Success but invalid JSON response")
                    results[endpoint_info["path"]] = {
                        "name": endpoint_info["name"],
                        "status": "invalid_json",
                        "total_records": 0,
                        "response_text": response.text[:200]
                    }
                    
            elif response.status_code == 204:
                print(f"📭 No data available")
                results[endpoint_info["path"]] = {
                    "name": endpoint_info["name"],
                    "status": "no_data",
                    "total_records": 0
                }
                
            else:
                print(f"❌ HTTP {response.status_code}: {response.reason_phrase}")
                results[endpoint_info["path"]] = {
                    "name": endpoint_info["name"],
                    "status": "error",
                    "error": f"HTTP {response.status_code}",
                    "total_records": 0
                }
                
        except Exception as e:
            print(f"❌ Error: {e}")
            results[endpoint_info["path"]] = {
                "name": endpoint_info["name"],
                "status": "error",
                "error": str(e),
                "total_records": 0
            }
    
    return results


def main():
    """Run comprehensive PNCP endpoint testing."""
    print("🔍 PNCP API Comprehensive Endpoint Analysis")
    print("=" * 50)
    
    # Use a recent date for testing
    test_date = "20240710"  # July 10, 2024
    print(f"📅 Test Date: {test_date}")
    
    # Initialize client
    client = PNCPClient()
    
    # Test results storage
    all_results = {
        "test_date": test_date,
        "endpoints": {}
    }
    
    # Test the working contratos endpoint first
    print("\n" + "=" * 50)
    print("TESTING WORKING ENDPOINT")
    print("=" * 50)
    
    contratos_result = test_contratos_endpoint(client, test_date)
    all_results["endpoints"]["contratos_publicacao"] = contratos_result
    
    # Test other endpoints manually
    print("\n" + "=" * 50)
    print("TESTING OTHER ENDPOINTS")
    print("=" * 50)
    
    other_results = test_manual_endpoints(client, test_date)
    all_results["endpoints"].update(other_results)
    
    # Save results
    output_dir = Path("data/endpoint_analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"pncp_endpoint_analysis_{test_date}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)
    
    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    
    total_endpoints = len(all_results["endpoints"])
    successful_endpoints = sum(1 for ep in all_results["endpoints"].values() 
                             if ep.get("status") == "success")
    total_records = sum(ep.get("total_records", 0) for ep in all_results["endpoints"].values())
    
    print(f"📊 Total endpoints tested: {total_endpoints}")
    print(f"✅ Successful endpoints: {successful_endpoints}")
    print(f"📄 Total records found: {total_records:,}")
    print(f"💾 Results saved to: {output_file}")
    
    # Print detailed results
    print(f"\n📋 Detailed Results:")
    for endpoint_path, result in all_results["endpoints"].items():
        status_emoji = {
            "success": "✅",
            "no_data": "📭",
            "error": "❌",
            "invalid_json": "⚠️"
        }.get(result.get("status"), "❓")
        
        name = result.get("name", endpoint_path)
        total_records = result.get("total_records", 0)
        
        print(f"  {status_emoji} {name}: {total_records:,} records")
    
    return all_results


if __name__ == "__main__":
    main()