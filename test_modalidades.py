#!/usr/bin/env python3
"""
Test script for modalidade iteration functionality
"""

import asyncio
import httpx
from datetime import date, timedelta
from src.baliza.pncp_extractor import PNCP_ENDPOINTS, PNCP_BASE_URL


async def test_contratacoes_with_modalidades():
    """Test contratacoes endpoints with modalidade iteration"""
    print("🚀 Testing contratacoes endpoints with modalidade iteration\n")
    
    # Test date - use a date from the past that should have data
    test_date = date(2024, 6, 18)  # Use a date from last year
    
    # Find contratacoes endpoints
    contratacoes_endpoints = [
        ep for ep in PNCP_ENDPOINTS 
        if ep["name"] in ["contratacoes_publicacao", "contratacoes_atualizacao"]
    ]
    
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        
        for endpoint in contratacoes_endpoints:
            print(f"\n🧪 Testing {endpoint['name']}")
            modalidades = endpoint.get("iterate_modalidades", [])
            
            for modalidade in modalidades:
                print(f"   Testing modalidade {modalidade}...")
                
                # Build parameters
                params = {
                    "tamanhoPagina": 10,
                    "pagina": 1,
                    "dataInicial": test_date.strftime("%Y%m%d"),
                    "dataFinal": test_date.strftime("%Y%m%d"),
                    "codigoModalidadeContratacao": modalidade
                }
                
                try:
                    response = await client.get(endpoint["path"], params=params)
                    
                    if response.status_code == 200:
                        data = response.json()
                        total_records = data.get("totalRegistros", 0)
                        print(f"      ✅ Modalidade {modalidade}: {total_records:,} records")
                    else:
                        print(f"      ❌ Modalidade {modalidade}: HTTP {response.status_code}")
                        if response.status_code == 422:
                            error_data = response.json()
                            print(f"         Error: {error_data.get('message', 'Unknown error')}")
                        
                except Exception as e:
                    print(f"      💥 Modalidade {modalidade}: Exception - {e}")
                
                # Small delay to be respectful
                await asyncio.sleep(0.5)


async def test_contratacoes_proposta_with_future_date():
    """Test contratacoes_proposta endpoint with future date"""
    print("\n🧪 Testing contratacoes_proposta with future date")
    
    # Use tomorrow's date
    future_date = date.today() + timedelta(days=1)
    
    endpoint = next(
        (ep for ep in PNCP_ENDPOINTS if ep["name"] == "contratacoes_proposta"),
        None
    )
    
    if not endpoint:
        print("❌ contratacoes_proposta endpoint not found")
        return
    
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        
        modalidades = endpoint.get("iterate_modalidades", [])
        
        for modalidade in modalidades:
            print(f"   Testing modalidade {modalidade} with future date...")
            
            params = {
                "tamanhoPagina": 10,
                "pagina": 1,
                "dataFinal": future_date.strftime("%Y%m%d"),
                "codigoModalidadeContratacao": modalidade
            }
            
            try:
                response = await client.get(endpoint["path"], params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    print(f"      ✅ Modalidade {modalidade}: {total_records:,} records")
                else:
                    print(f"      ❌ Modalidade {modalidade}: HTTP {response.status_code}")
                    if response.status_code == 422:
                        error_data = response.json()
                        print(f"         Error: {error_data.get('message', 'Unknown error')}")
                    
            except Exception as e:
                print(f"      💥 Modalidade {modalidade}: Exception - {e}")
            
            # Small delay to be respectful
            await asyncio.sleep(0.5)


async def test_instrumentos_cobranca_with_data():
    """Test instrumentos cobranca with a date that has data"""
    print("\n🧪 Testing instrumentoscobranca_inclusao with different dates")
    
    endpoint = next(
        (ep for ep in PNCP_ENDPOINTS if ep["name"] == "instrumentoscobranca_inclusao"),
        None
    )
    
    if not endpoint:
        print("❌ instrumentoscobranca_inclusao endpoint not found")
        return
    
    # Test different date ranges
    test_dates = [
        (date(2024, 3, 1), date(2024, 3, 31)),  # March 2024
        (date(2024, 6, 1), date(2024, 6, 30)),  # June 2024
        (date(2024, 1, 1), date(2024, 1, 31)),  # January 2024
    ]
    
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        
        for start_date, end_date in test_dates:
            print(f"   Testing date range: {start_date} to {end_date}")
            
            params = {
                "tamanhoPagina": 10,
                "pagina": 1,
                "dataInicial": start_date.strftime("%Y%m%d"),
                "dataFinal": end_date.strftime("%Y%m%d"),
            }
            
            try:
                response = await client.get(endpoint["path"], params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    print(f"      ✅ {start_date} to {end_date}: {total_records:,} records")
                else:
                    print(f"      ❌ {start_date} to {end_date}: HTTP {response.status_code}")
                    if response.status_code == 404:
                        error_data = response.json()
                        print(f"         Error: {error_data.get('message', 'Not Found')}")
                    
            except Exception as e:
                print(f"      💥 {start_date} to {end_date}: Exception - {e}")
            
            # Small delay to be respectful
            await asyncio.sleep(1.0)


async def main():
    """Run all modalidade tests"""
    print("🔍 PNCP Modalidade Testing Script")
    print("=" * 50)
    
    await test_contratacoes_with_modalidades()
    await test_contratacoes_proposta_with_future_date()
    await test_instrumentos_cobranca_with_data()
    
    print("\n✅ All modalidade tests completed!")


if __name__ == "__main__":
    asyncio.run(main())