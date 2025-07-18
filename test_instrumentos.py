#!/usr/bin/env python3
"""
Test instrumentoscobranca_inclusao endpoint with different date ranges
"""

import asyncio
import httpx
from datetime import date, timedelta
from src.baliza.pncp_extractor import PNCP_BASE_URL


async def test_instrumentos_cobranca():
    """Test instrumentoscobranca_inclusao with various date ranges"""
    print("🧪 Testing instrumentoscobranca_inclusao endpoint")
    
    # Test different date ranges to find data
    test_ranges = [
        (date(2024, 1, 1), date(2024, 1, 31)),   # January 2024
        (date(2024, 3, 1), date(2024, 3, 31)),   # March 2024
        (date(2024, 6, 1), date(2024, 6, 30)),   # June 2024
        (date(2023, 12, 1), date(2023, 12, 31)), # December 2023
        (date(2023, 6, 1), date(2023, 6, 30)),   # June 2023
        (date(2022, 12, 1), date(2022, 12, 31)), # December 2022
        (date(2021, 12, 1), date(2021, 12, 31)), # December 2021
    ]
    
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        
        for start_date, end_date in test_ranges:
            print(f"\n   Testing range: {start_date} to {end_date}")
            
            params = {
                "tamanhoPagina": 10,
                "pagina": 1,
                "dataInicial": start_date.strftime("%Y%m%d"),
                "dataFinal": end_date.strftime("%Y%m%d"),
            }
            
            try:
                response = await client.get("/v1/instrumentoscobranca/inclusao", params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    total_records = data.get("totalRegistros", 0)
                    print(f"      ✅ SUCCESS: {total_records:,} records found!")
                    if total_records > 0:
                        print(f"         Sample data: {data.get('data', [])[:1]}")
                    break  # Found data, no need to test more
                elif response.status_code == 404:
                    print(f"      ❌ HTTP 404: No data found for this period")
                else:
                    print(f"      ❌ HTTP {response.status_code}: {response.text[:200]}")
                    
            except Exception as e:
                print(f"      💥 Exception: {e}")
            
            # Small delay to be respectful
            await asyncio.sleep(1.0)
    
    print("\n🏁 Instrumentos cobranca testing complete!")


if __name__ == "__main__":
    asyncio.run(test_instrumentos_cobranca())