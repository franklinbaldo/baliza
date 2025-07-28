#!/usr/bin/env python3
"""
Comprehensive test script for ALL PNCP API endpoints with correct parameters
"""

import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urljoin

BASE_URL = "https://pncp.gov.br/api/consulta/v1/"

def test_endpoint(endpoint, params_list, endpoint_name=""):
    """Test an endpoint with different parameter combinations"""
    print(f"\n{'='*60}")
    print(f"Testing endpoint: {endpoint_name or endpoint}")
    print(f"{'='*60}")
    
    for i, params in enumerate(params_list, 1):
        url = urljoin(BASE_URL, endpoint)
        print(f"\n{i}. Testing with params: {params}")
        print(f"   URL: {url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
        
        try:
            response = requests.get(url, params=params, timeout=30)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        print(f"   Response type: dict with {len(data)} keys")
                        if 'data' in data:
                            print(f"   Data items: {len(data.get('data', []))}")
                        print(f"   Keys: {list(data.keys())}")
                        
                        # Sample data structure for successful endpoint
                        if 'data' in data and data['data']:
                            first_item = data['data'][0]
                            print(f"   First item keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'Not a dict'}")
                            
                    return True  # Endpoint works
                        
                except json.JSONDecodeError:
                    print(f"   Response is not JSON")
                    
            elif response.status_code == 204:
                print(f"   No content (valid response)")
                return True
                    
            else:
                print(f"   Error: {response.text[:200]}...")
                
        except requests.exceptions.RequestException as e:
            print(f"   Request failed: {e}")
            
    return False

def main():
    current_date = datetime.now()
    thirty_days_ago = (current_date - timedelta(days=30)).strftime("%Y%m%d")
    today = current_date.strftime("%Y%m%d")
    
    # Working parameters based on OpenAPI spec and testing
    working_params = {}
    
    print("=== COMPREHENSIVE PNCP API ENDPOINT TESTING ===")
    
    # 1. contratacoes/publicacao - requires modalidade, dataInicial, dataFinal
    print("\n1. contratacoes/publicacao")
    contratacoes_publicacao_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131", "codigoModalidadeContratacao": 1},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131", "codigoModalidadeContratacao": 5},
    ]
    if test_endpoint("contratacoes/publicacao", contratacoes_publicacao_params, "contratacoes_publicacao"):
        working_params["contratacoes_publicacao"] = contratacoes_publicacao_params[0]
    
    # 2. contratos - requires dataInicial, dataFinal
    print("\n2. contratos")
    contratos_params = [
        {"tamanhoPagina": 500, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
    ]
    if test_endpoint("contratos", contratos_params, "contratos"):
        working_params["contratos"] = contratos_params[0]
    
    # 3. atas - requires dataInicial, dataFinal
    print("\n3. atas")
    atas_params = [
        {"tamanhoPagina": 500, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
    ]
    if test_endpoint("atas", atas_params, "atas"):
        working_params["atas"] = atas_params[0]
    
    # 4. contratacoes/atualizacao - requires modalidade, dataInicial, dataFinal
    print("\n4. contratacoes/atualizacao")
    contratacoes_atualizacao_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131", "codigoModalidadeContratacao": 1},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131", "codigoModalidadeContratacao": 5},
    ]
    if test_endpoint("contratacoes/atualizacao", contratacoes_atualizacao_params, "contratacoes_atualizacao"):
        working_params["contratacoes_atualizacao"] = contratacoes_atualizacao_params[0]
    
    # 5. contratos/atualizacao - requires dataInicial, dataFinal
    print("\n5. contratos/atualizacao")
    contratos_atualizacao_params = [
        {"tamanhoPagina": 500, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
    ]
    if test_endpoint("contratos/atualizacao", contratos_atualizacao_params, "contratos_atualizacao"):
        working_params["contratos_atualizacao"] = contratos_atualizacao_params[0]
    
    # 6. atas/atualizacao - requires dataInicial, dataFinal
    print("\n6. atas/atualizacao")
    atas_atualizacao_params = [
        {"tamanhoPagina": 500, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20210101", "dataFinal": "20210131"},
    ]
    if test_endpoint("atas/atualizacao", atas_atualizacao_params, "atas_atualizacao"):
        working_params["atas_atualizacao"] = atas_atualizacao_params[0]
    
    # 7. contratacoes/proposta - requires dataFinal >= current date (WORKING)
    print("\n7. contratacoes/proposta")
    proposta_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
    ]
    if test_endpoint("contratacoes/proposta", proposta_params, "contratacoes_proposta"):
        working_params["contratacoes_proposta"] = proposta_params[0]
    
    # 8. instrumentoscobranca/inclusao - requires both dataInicial and dataFinal, max 30 days apart (WORKING)
    print("\n8. instrumentoscobranca/inclusao")
    instrumentos_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": thirty_days_ago, "dataFinal": today},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20240601", "dataFinal": "20240630"},
    ]
    if test_endpoint("instrumentoscobranca/inclusao", instrumentos_params, "instrumentoscobranca_inclusao"):
        working_params["instrumentoscobranca_inclusao"] = instrumentos_params[0]
    
    # 9. pca/ - requires anoPca and codigoClassificacaoSuperior (WORKING)
    print("\n9. pca/")
    pca_params = [
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2024, "codigoClassificacaoSuperior": "00"},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2023, "codigoClassificacaoSuperior": "01"},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2021, "codigoClassificacaoSuperior": "10"},
    ]
    if test_endpoint("pca/", pca_params, "pca"):
        working_params["pca"] = pca_params[0]
    
    # 10. pca/usuario - requires anoPca and idUsuario
    print("\n10. pca/usuario")
    pca_usuario_params = [
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2024, "idUsuario": 1},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2023, "idUsuario": 1},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2021, "idUsuario": 1},
    ]
    if test_endpoint("pca/usuario", pca_usuario_params, "pca_usuario"):
        working_params["pca_usuario"] = pca_usuario_params[0]
    
    # 11. pca/atualizacao - requires dataInicio and dataFim (WORKING)
    print("\n11. pca/atualizacao")
    pca_atualizacao_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataInicio": thirty_days_ago, "dataFim": today},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicio": "20240601", "dataFim": "20240630"},
    ]
    if test_endpoint("pca/atualizacao", pca_atualizacao_params, "pca_atualizacao"):
        working_params["pca_atualizacao"] = pca_atualizacao_params[0]
    
    print("\n" + "="*60)
    print("SUMMARY - Working parameters:")
    print("="*60)
    for endpoint, params in working_params.items():
        print(f"{endpoint}: {params}")
    
    print("\n" + "="*60)
    print("ISSUES FOUND:")
    print("="*60)
    
    failing_endpoints = []
    expected_endpoints = [
        "contratacoes_publicacao", "contratos", "atas", "contratacoes_atualizacao",
        "contratos_atualizacao", "atas_atualizacao", "contratacoes_proposta",
        "instrumentoscobranca_inclusao", "pca", "pca_usuario", "pca_atualizacao"
    ]
    
    for endpoint in expected_endpoints:
        if endpoint not in working_params:
            failing_endpoints.append(endpoint)
    
    if failing_endpoints:
        print("Failing endpoints:")
        for endpoint in failing_endpoints:
            print(f"  - {endpoint}")
    else:
        print("All endpoints working!")
    
    return working_params

if __name__ == "__main__":
    main()