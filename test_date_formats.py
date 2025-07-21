#!/usr/bin/env python3
"""Test different date formats to find the correct one for PNCP API."""

import asyncio
from datetime import date, timedelta

import httpx


async def test_date_formats():
    """Test different date formats."""
    test_date = date.today() - timedelta(days=1)

    formats_to_test = [
        test_date.strftime('%Y-%m-%d'),      # 2025-07-20
        test_date.strftime('%d/%m/%Y'),      # 20/07/2025
        test_date.strftime('%d-%m-%Y'),      # 20-07-2025
        test_date.strftime('%Y%m%d'),        # 20250720
        test_date.strftime('%d%m%Y'),        # 20072025
    ]

    async with httpx.AsyncClient(
        base_url="https://pncp.gov.br/api/consulta",
        timeout=30.0,
    ) as client:

        for date_format in formats_to_test:
            print(f"Testing date format: {date_format}")

            params = {
                "dataInicial": date_format,
                "dataFinal": date_format,
                "pagina": 1,
                "tamanhoPagina": 10,
            }

            try:
                response = await client.get("/v1/contratos", params=params)
                print(f"  Status: {response.status_code}")
                if response.status_code != 200:
                    content = response.text[:200]
                    print(f"  Error: {content}")
                else:
                    print("  ✅ SUCCESS!")
                    return date_format
            except Exception as e:
                print(f"  Exception: {e}")

            print()

    return None

if __name__ == "__main__":
    result = asyncio.run(test_date_formats())
    if result:
        print(f"Correct date format: {result}")
    else:
        print("No working date format found")
