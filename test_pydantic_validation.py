#!/usr/bin/env python3
"""
Test script to validate Pydantic models against real PNCP API responses.
This will help us identify missing fields and fix our schema definitions.
"""

import requests
import json
from datetime import date, timedelta
from baliza.settings import settings
from baliza.models import (
    PaginaRetornoRecuperarCompraPublicacaoDTO,
    PaginaRetornoRecuperarContratoDTO, 
    PaginaRetornoAtaRegistroPrecoPeriodoDTO,
    PaginaRetornoConsultarInstrumentoCobrancaDTO,
    PaginaRetornoPlanoContratacaoComItensDoUsuarioDTO
)
from pydantic import ValidationError


def test_endpoint_pydantic_model(endpoint_path: str, params: dict, model_class, endpoint_name: str):
    """Test if a Pydantic model can validate real API response from an endpoint."""
    
    print(f"\n🔍 Testing {endpoint_name} ({endpoint_path})")
    print(f"   Params: {params}")
    
    # Make API request
    try:
        url = f"{settings.pncp_api_base_url}{endpoint_path}"
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 204:
            print(f"   ⚪ No data found (204 No Content)")
            return
        elif response.status_code != 200:
            print(f"   ❌ API Error: {response.status_code}")
            if response.text:
                print(f"      Response: {response.text}")
            return
            
        data = response.json()
        
        # Try to validate with Pydantic model
        try:
            validated = model_class.model_validate(data)
            print(f"   ✅ Pydantic model validation: SUCCESS")
            print(f"   📊 Records found: {len(data.get('data', []))}")
            
        except ValidationError as e:
            print(f"   ❌ Pydantic model validation: FAILED")
            print(f"   📋 Validation errors:")
            for error in e.errors():
                print(f"      - {error['loc']}: {error['msg']}")
            
            # Show sample of actual data fields vs model fields
            if data.get('data') and len(data['data']) > 0:
                sample_record = data['data'][0]
                print(f"   📄 Sample API response fields:")
                for field in sorted(sample_record.keys())[:10]:  # Show first 10 fields
                    print(f"      - {field}: {type(sample_record[field]).__name__}")
                if len(sample_record.keys()) > 10:
                    print(f"      ... and {len(sample_record.keys()) - 10} more fields")
                    
    except Exception as e:
        print(f"   💥 Request failed: {e}")


def main():
    """Test all endpoints with their corresponding Pydantic models."""
    
    print("🧪 Testing Pydantic Model Validation Against Real PNCP API Responses")
    print("=" * 70)
    
    # Test date range (use recent dates that are likely to have data)
    recent_date = date.today() - timedelta(days=30)  # 30 days ago
    start_date = recent_date.strftime("%Y%m%d")
    end_date = recent_date.strftime("%Y%m%d")
    
    # Test cases: (endpoint_path, params, model_class, name)
    # Use parameters that work based on our successful extractions
    test_cases = [
        (
            "/v1/contratos", 
            {"dataInicial": start_date, "dataFinal": end_date, "tamanhoPagina": 10, "pagina": 1},
            PaginaRetornoRecuperarContratoDTO,
            "contratos"
        ),
        (
            "/v1/contratacoes/publicacao",
            {"dataInicial": start_date, "dataFinal": end_date, "codigoModalidadeContratacao": 8, "tamanhoPagina": 10, "pagina": 1},
            PaginaRetornoRecuperarCompraPublicacaoDTO,
            "contratacoes_publicacao"
        ),
        (
            "/v1/atas",
            {"dataInicial": start_date, "dataFinal": end_date, "tamanhoPagina": 10, "pagina": 1},
            PaginaRetornoAtaRegistroPrecoPeriodoDTO,
            "atas"
        ),
    ]
    
    for endpoint_path, params, model_class, name in test_cases:
        test_endpoint_pydantic_model(endpoint_path, params, model_class, name)
    
    print("\n" + "=" * 70)
    print("🎯 Recommendations:")
    print("1. ✅ For endpoints that validate successfully: Keep using Pydantic models")
    print("2. ❌ For endpoints that fail validation: Use permissive column definitions")
    print("3. 🔧 Consider updating Pydantic models to include missing fields")


if __name__ == "__main__":
    main()