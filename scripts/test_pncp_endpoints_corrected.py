#!/usr/bin/env python3
"""
Test script for PNCP API endpoints with correct parameters based on OpenAPI spec
"""

import requests
import json
from datetime import datetime, timedelta
from urllib.parse import urljoin

BASE_URL = "https://pncp.gov.br/api/consulta/v1/"

def test_endpoint(endpoint, params_list):
    """Test an endpoint with different parameter combinations"""
    print(f"\n{'='*60}")
    print(f"Testing endpoint: {endpoint}")
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
                    
            else:
                print(f"   Error: {response.text[:200]}...")
                
        except requests.exceptions.RequestException as e:
            print(f"   Request failed: {e}")
            
    return False

def main():
    current_date = datetime.now()
    
    # Working parameters based on OpenAPI spec and testing
    working_params = {}
    
    print("=== Testing endpoints with correct parameters based on OpenAPI spec ===")
    
    # 1. contratacoes/proposta - works with dataFinal >= current date
    print("\n1. contratacoes/proposta - WORKING (dataFinal >= current date)")
    proposta_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataFinal": "20300101"},
    ]
    if test_endpoint("contratacoes/proposta", proposta_params):
        working_params["contratacoes_proposta"] = proposta_params[0]
    
    # 2. instrumentoscobranca/inclusao - requires both dataInicial and dataFinal, max 30 days apart
    print("\n2. instrumentoscobranca/inclusao - Testing with 30-day range")
    today = current_date.strftime("%Y%m%d")
    thirty_days_ago = (current_date - timedelta(days=30)).strftime("%Y%m%d")
    
    instrumentos_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": thirty_days_ago, "dataFinal": today},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicial": "20240601", "dataFinal": "20240630"},
    ]
    if test_endpoint("instrumentoscobranca/inclusao", instrumentos_params):
        working_params["instrumentoscobranca_inclusao"] = instrumentos_params[0]
    
    # 3. pca/ - requires anoPca and codigoClassificacaoSuperior
    print("\n3. pca/ - Testing with required parameters")
    pca_params = [
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2024, "codigoClassificacaoSuperior": "00"},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2023, "codigoClassificacaoSuperior": "01"},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2021, "codigoClassificacaoSuperior": "10"},
    ]
    if test_endpoint("pca/", pca_params):
        working_params["pca"] = pca_params[0]
    
    # 4. pca/usuario - requires anoPca and idUsuario
    print("\n4. pca/usuario - Testing with required parameters")
    pca_usuario_params = [
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2024, "idUsuario": 1},
        {"tamanhoPagina": 50, "pagina": 1, "anoPca": 2023, "idUsuario": 1},
    ]
    if test_endpoint("pca/usuario", pca_usuario_params):
        working_params["pca_usuario"] = pca_usuario_params[0]
    
    # 5. pca/atualizacao - requires dataInicio and dataFim
    print("\n5. pca/atualizacao - Testing with required parameters")
    pca_atualizacao_params = [
        {"tamanhoPagina": 50, "pagina": 1, "dataInicio": thirty_days_ago, "dataFim": today},
        {"tamanhoPagina": 50, "pagina": 1, "dataInicio": "20240601", "dataFim": "20240630"},
    ]
    if test_endpoint("pca/atualizacao", pca_atualizacao_params):
        working_params["pca_atualizacao"] = pca_atualizacao_params[0]
    
    print("\n" + "="*60)
    print("SUMMARY - Working parameters:")
    print("="*60)
    for endpoint, params in working_params.items():
        print(f"{endpoint}: {params}")
    
    return working_params

if __name__ == "__main__":
    main()