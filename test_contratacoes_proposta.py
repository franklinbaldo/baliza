#!/usr/bin/env python3
"""
Test contratacoes_proposta endpoint to understand date requirements and limits
"""

import asyncio
import httpx
from datetime import date, timedelta
from src.baliza.pncp_extractor import PNCP_BASE_URL


async def test_contratacoes_proposta_limits():
    """Test contratacoes_proposta endpoint date limits and requirements"""
    print("🧪 Testing contratacoes_proposta endpoint requirements")
    
    today = date.today()
    
    # Test different scenarios
    test_scenarios = [
        # Test if dataFinal is required
        ("No dataFinal parameter", {}),
        
        # Test with today's date
        ("Today's date", {"dataFinal": today.strftime("%Y%m%d")}),
        
        # Test with future dates
        ("Tomorrow", {"dataFinal": (today + timedelta(days=1)).strftime("%Y%m%d")}),
        ("1 week future", {"dataFinal": (today + timedelta(days=7)).strftime("%Y%m%d")}),
        ("30 days future", {"dataFinal": (today + timedelta(days=30)).strftime("%Y%m%d")}),
        ("90 days future", {"dataFinal": (today + timedelta(days=90)).strftime("%Y%m%d")}),
        ("1 year future", {"dataFinal": (today + timedelta(days=365)).strftime("%Y%m%d")}),
        ("2 years future", {"dataFinal": (today + timedelta(days=730)).strftime("%Y%m%d")}),
        ("5 years future", {"dataFinal": (today + timedelta(days=1825)).strftime("%Y%m%d")}),
    ]
    
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        
        for scenario_name, base_params in test_scenarios:
            print(f"\n   Testing: {scenario_name}")
            
            # Test with modalidade 6 (usually has most data)
            params = {
                "tamanhoPagina": 10,
                "pagina": 1,
                "codigoModalidadeContratacao": 6,
                **base_params
            }
            
            try:
                response = await client.get("/v1/contratacoes/proposta", params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    print(f"      ✅ SUCCESS: {total_records:,} records found")
                    if total_records > 0:
                        sample_data = data.get("data", [])
                        if sample_data:
                            first_record = sample_data[0]
                            print(f"         Sample contract: {first_record.get('numeroControlePNCP', 'N/A')}")
                            print(f"         Deadline: {first_record.get('dataFimPropostas', 'N/A')}")
                elif response.status_code == 422:
                    error_data = response.json()
                    print(f"      ❌ HTTP 422: {error_data.get('message', 'Validation error')}")
                elif response.status_code == 400:
                    error_data = response.json()
                    print(f"      ❌ HTTP 400: {error_data.get('message', 'Bad request')}")
                else:
                    print(f"      ❌ HTTP {response.status_code}: {response.text[:200]}")
                    
            except Exception as e:
                print(f"      💥 Exception: {e}")
            
            # Small delay to be respectful
            await asyncio.sleep(1.0)
    
    print("\n🏁 Contratacoes proposta testing complete!")


async def test_without_modalidade():
    """Test if modalidade parameter is required"""
    print("\n🧪 Testing contratacoes_proposta WITHOUT modalidade parameter")
    
    today = date.today()
    future_date = today + timedelta(days=30)
    
    params = {
        "tamanhoPagina": 10,
        "pagina": 1,
        "dataFinal": future_date.strftime("%Y%m%d"),
        # No modalidade parameter
    }
    
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        
        try:
            response = await client.get("/v1/contratacoes/proposta", params=params)
            
            if response.status_code == 200:
                data = response.json()
                total_records = data.get("totalRegistros", 0)
                print(f"   ✅ SUCCESS without modalidade: {total_records:,} records found")
            elif response.status_code == 400:
                error_data = response.json()
                print(f"   ❌ HTTP 400: {error_data.get('message', 'Bad request')}")
                print("   → Modalidade parameter IS required")
            else:
                print(f"   ❌ HTTP {response.status_code}: {response.text[:200]}")
                
        except Exception as e:
            print(f"   💥 Exception: {e}")


if __name__ == "__main__":
    asyncio.run(test_contratacoes_proposta_limits())
    asyncio.run(test_without_modalidade())