#!/usr/bin/env python3
"""Script para baixar dados de teste da API PNCP.

Este script baixa a página 1 de cada endpoint principal para criar 
dados de teste realistas para as validações Pydantic.
"""

import asyncio
import json
import logging
from pathlib import Path
from datetime import date, timedelta

import httpx

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base configuration
BASE_URL = "https://pncp.gov.br/api/consulta/v1"
TIMEOUT = 30
TEST_DATA_DIR = Path(__file__).parent / "test_data"

# Test date range (using a month with known data)
today = date.today()
# Use July 2024 as test data (known to have data)
# API expects YYYYMMDD format (not ISO YYYY-MM-DD)
start_date = "20240701"
end_date = "20240731"

# Endpoints to fetch test data from
ENDPOINTS = {
    "contratos_publicacao": {
        "url": f"{BASE_URL}/contratos",
        "params": {
            "dataInicial": start_date,
            "dataFinal": end_date,
            "pagina": 1,
            "tamanhoPagina": 10  # Small sample for tests
        }
    },
    "contratos_atualizacao": {
        "url": f"{BASE_URL}/contratos/atualizacao", 
        "params": {
            "dataInicial": start_date,
            "dataFinal": end_date,
            "pagina": 1,
            "tamanhoPagina": 10
        }
    },
    "contratacoes_publicacao": {
        "url": f"{BASE_URL}/contratacoes/publicacao",
        "params": {
            "dataInicial": start_date,
            "dataFinal": end_date,
            "codigoModalidadeContratacao": 1,  # Pregão eletrônico
            "pagina": 1,
            "tamanhoPagina": 10
        }
    },
    "contratacoes_atualizacao": {
        "url": f"{BASE_URL}/contratacoes/atualizacao",
        "params": {
            "dataInicial": start_date,
            "dataFinal": end_date,
            "codigoModalidadeContratacao": 1,
            "pagina": 1,
            "tamanhoPagina": 10
        }
    },
    "atas_periodo": {
        "url": f"{BASE_URL}/atas",
        "params": {
            "dataInicial": start_date,
            "dataFinal": end_date,
            "pagina": 1,
            "tamanhoPagina": 10
        }
    },
    "atas_atualizacao": {
        "url": f"{BASE_URL}/atas/atualizacao",
        "params": {
            "dataInicial": start_date,
            "dataFinal": end_date,
            "pagina": 1,
            "tamanhoPagina": 10
        }
    }
}


async def fetch_endpoint_data(client: httpx.AsyncClient, endpoint_name: str, config: dict) -> dict:
    """Fetch data from a single endpoint."""
    try:
        logger.info(f"Fetching {endpoint_name}...")
        
        response = await client.get(
            config["url"],
            params=config["params"],
            timeout=TIMEOUT
        )
        
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"✅ {endpoint_name}: {data.get('totalRegistros', 0)} total records, "
                   f"{len(data.get('data', []))} in page 1")
        
        return {
            "endpoint_name": endpoint_name,
            "url": str(response.url),
            "status_code": response.status_code,
            "data": data,
            "fetch_date": today.isoformat(),
            "params": config["params"]
        }
        
    except httpx.HTTPStatusError as e:
        logger.error(f"❌ {endpoint_name}: HTTP {e.response.status_code}")
        return {
            "endpoint_name": endpoint_name,
            "error": f"HTTP {e.response.status_code}",
            "url": config["url"],
            "params": config["params"]
        }
    except Exception as e:
        logger.error(f"❌ {endpoint_name}: {str(e)}")
        return {
            "endpoint_name": endpoint_name,
            "error": str(e),
            "url": config["url"],
            "params": config["params"]
        }


async def fetch_all_test_data():
    """Fetch test data from all endpoints concurrently."""
    # Create test data directory
    TEST_DATA_DIR.mkdir(exist_ok=True)
    
    logger.info(f"📥 Fetching test data for date range: {start_date} to {end_date}")
    logger.info(f"💾 Saving to: {TEST_DATA_DIR}")
    
    async with httpx.AsyncClient() as client:
        # Fetch all endpoints concurrently
        tasks = [
            fetch_endpoint_data(client, endpoint_name, config)
            for endpoint_name, config in ENDPOINTS.items()
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Save results to individual JSON files
        for result in results:
            endpoint_name = result["endpoint_name"]
            output_file = TEST_DATA_DIR / f"{endpoint_name}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            
            if "error" in result:
                logger.warning(f"⚠️  {endpoint_name}: Saved with error to {output_file}")
            else:
                records_count = len(result["data"].get("data", []))
                logger.info(f"💾 {endpoint_name}: Saved {records_count} records to {output_file}")
        
        # Create summary file
        summary = {
            "fetch_date": today.isoformat(),
            "date_range": f"{start_date} to {end_date}",
            "date_format": "YYYYMMDD (API format)",
            "endpoints": len(ENDPOINTS),
            "results": {
                result["endpoint_name"]: {
                    "status": "success" if "error" not in result else "error",
                    "records": len(result.get("data", {}).get("data", [])) if "error" not in result else 0,
                    "total_records": result.get("data", {}).get("totalRegistros", 0) if "error" not in result else 0
                }
                for result in results
            }
        }
        
        summary_file = TEST_DATA_DIR / "summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📋 Summary saved to {summary_file}")
        
        # Print final summary
        success_count = sum(1 for result in results if "error" not in result)
        total_records = sum(
            len(result.get("data", {}).get("data", [])) 
            for result in results if "error" not in result
        )
        
        logger.info(f"🎉 Completed: {success_count}/{len(ENDPOINTS)} endpoints successful")
        logger.info(f"📊 Total test records: {total_records}")


if __name__ == "__main__":
    asyncio.run(fetch_all_test_data())