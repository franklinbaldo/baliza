#!/usr/bin/env python3
"""
PNCP Endpoint Testing Script
Tests each endpoint individually to ensure OpenAPI compliance
"""

import asyncio
import httpx
import json
from datetime import date, timedelta
from src.baliza.pncp_extractor import PNCP_ENDPOINTS, PNCP_BASE_URL

async def test_endpoint(endpoint_config: dict, test_date: date = None) -> dict:
    """Test a single endpoint with proper parameters."""
    if test_date is None:
        test_date = date.today() - timedelta(days=30)  # Test with data from 30 days ago
    
    print(f"\n🧪 Testing endpoint: {endpoint_config['name']}")
    print(f"   Path: {endpoint_config['path']}")
    print(f"   Description: {endpoint_config['description']}")
    
    # Build parameters
    params = {
        "tamanhoPagina": min(10, endpoint_config.get("page_size", 20)),  # Use small page size for testing
        "pagina": 1,
    }
    
    # Add date parameters
    if endpoint_config["supports_date_range"]:
        start_date = test_date.strftime("%Y%m%d")
        end_date = test_date.strftime("%Y%m%d")  # Same day for testing
        params[endpoint_config["date_params"][0]] = start_date
        params[endpoint_config["date_params"][1]] = end_date
        print(f"   Date range: {start_date} to {end_date}")
    elif endpoint_config.get("requires_single_date", False):
        single_date = test_date.strftime("%Y%m%d")
        params[endpoint_config["date_params"][0]] = single_date
        print(f"   Single date: {single_date}")
    
    # Add extra parameters if specified
    if "extra_params" in endpoint_config:
        params.update(endpoint_config["extra_params"])
        print(f"   Extra params: {endpoint_config['extra_params']}")
    
    print(f"   Full params: {params}")
    
    # Make the request
    async with httpx.AsyncClient(
        base_url=PNCP_BASE_URL,
        timeout=30.0,
        headers={
            "User-Agent": "BALIZA/3.0 (Testing)",
            "Accept": "application/json",
        }
    ) as client:
        try:
            response = await client.get(endpoint_config["path"], params=params)
            
            result = {
                "endpoint": endpoint_config["name"],
                "status_code": response.status_code,
                "success": response.status_code == 200,
                "url": str(response.url),
                "params": params,
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result["total_records"] = data.get("totalRegistros", 0)
                    result["total_pages"] = data.get("totalPaginas", 0)
                    result["has_data"] = len(data.get("data", [])) > 0
                    print(f"   ✅ SUCCESS: {result['total_records']} records, {result['total_pages']} pages")
                except Exception as e:
                    result["json_error"] = str(e)
                    result["response_text"] = response.text[:500]
                    print(f"   ⚠️  SUCCESS but JSON parse error: {e}")
            else:
                result["error"] = response.text
                print(f"   ❌ FAILED: HTTP {response.status_code}")
                print(f"   Error: {response.text[:200]}...")
                
        except Exception as e:
            result = {
                "endpoint": endpoint_config["name"],
                "status_code": 0,
                "success": False,
                "error": str(e),
                "params": params,
            }
            print(f"   💥 EXCEPTION: {e}")
    
    return result

async def test_all_endpoints():
    """Test all configured endpoints."""
    print("🚀 Starting PNCP Endpoint Testing")
    print(f"📊 Testing {len(PNCP_ENDPOINTS)} endpoints")
    
    results = []
    successful = 0
    failed = 0
    
    for endpoint in PNCP_ENDPOINTS:
        result = await test_endpoint(endpoint)
        results.append(result)
        
        if result["success"]:
            successful += 1
        else:
            failed += 1
        
        # Small delay between requests to be respectful
        await asyncio.sleep(1)
    
    # Summary
    print(f"\n📋 TESTING SUMMARY")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total: {len(results)}")
    
    # Detailed results
    print(f"\n📄 DETAILED RESULTS:")
    for result in results:
        status = "✅" if result["success"] else "❌"
        endpoint_name = result["endpoint"]
        status_code = result.get("status_code", 0)
        
        if result["success"]:
            total_records = result.get("total_records", "?")
            print(f"{status} {endpoint_name}: HTTP {status_code} - {total_records} records")
        else:
            error = result.get("error", result.get("json_error", "Unknown error"))[:100]
            print(f"{status} {endpoint_name}: HTTP {status_code} - {error}")
    
    # Save results to file
    with open("endpoint_test_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: endpoint_test_results.json")
    
    return results

if __name__ == "__main__":
    asyncio.run(test_all_endpoints())